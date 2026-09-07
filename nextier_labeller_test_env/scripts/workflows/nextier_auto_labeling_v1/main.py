from __future__ import annotations

import hashlib
import importlib.metadata as importlib_metadata
import inspect
import json
from pathlib import Path
import sys
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
from prefect import flow, get_run_logger

TEMPLATE_ROOT = Path(__file__).resolve().parents[3]
if str(TEMPLATE_ROOT) not in sys.path:
    sys.path.insert(0, str(TEMPLATE_ROOT))

from nixdlt.workflow_sdk.platform_tasks import (  # noqa: E402
    delete_featurestore_records,
    get_state,
    query_store,
    set_state,
    write_featurestore,
)
from scripts.workflows.nextier_labeling_common_v1.common import (  # noqa: E402
    COPPER_LABELS_FEATURESTORE_KEY,
    COPPER_STAGE_INDEX_FEATURESTORE_KEY,
    apply_well_name_filters,
    combine_well_names,
    format_dt,
    last_non_null,
    now_utc,
    parse_dt,
    resolve_window,
    to_polars_for_write,
    validate_mode,
)

WORKFLOW_NAME = "nextier_auto_labeling_v1"
AUTO_LABELS_FEATURESTORE_KEY = "nextier_auto_labels_v1"
AUTO_STAGE_SUMMARY_FEATURESTORE_KEY = "nextier_auto_stage_summary_v1"
AUTO_MANIFEST_FEATURESTORE_KEY = "nextier_auto_processing_manifest_v1"
DEFAULT_AUTO_ALGORITHM_VERSION = "nextier_auto_layer_v1"
DEFAULT_AUTO_BETA_ALGORITHM_VERSION = "nextier_auto_layer_beta_v1"
DEFAULT_CONCENTRATION_FEATURE = "all"
AUTO_EXPLICIT_CONCENTRATION_FEATURES = ("denso", "inline", "target", "auger")
AUTO_TIP_STAGE_EDGE_BUFFER_MINUTES = 15.0
AUTO_LABEL_WRITE_BATCH_ROWS = 25_000
# These statuses mean the wrapper made a valid decision for a
# well/stage/concentration feature. Historical backfills can advance past them;
# only true failures should keep a window pending.
AUTO_TERMINAL_PROGRESS_STATUSES = (
    "written",
    "skipped_no_tip_window",
    "skipped_no_copper_rows",
)
AUTO_TERMINAL_PROGRESS_STATUS_SQL = ", ".join(
    f"'{status}'" for status in AUTO_TERMINAL_PROGRESS_STATUSES
)
CONCENTRATION_FEATURE_TO_COLUMN = {
    "denso": "prop_conc_blend_denso",
    "inline": "prop_conc_inline",
    "target": "prop_conc_target",
    "auger": "prop_conc_blend_auger",
    "auto": None,
    "all": None,
}

COPPER_COLUMNS = [
    "telemetry_point_id",
    "fleet_name",
    "pad_name",
    "well_name",
    "well_id",
    "api_num",
    "record_ts",
    "created_ts",
    "bronze_continuous",
    "copper_provisional",
    "copper_confirmed",
    "copper_continuous",
    "stage_num",
    "rate_slurry",
    "press_mainline",
    "prop_conc_blend_denso",
    "prop_conc_blend_auger",
    "prop_conc_inline",
    "prop_conc_target",
]

POINT_LABELS = {"open_well", "stage_start", "ttr", "stage_end", "close_well"}
WINDOW_LABELS = {"pad", "slurry", "flush"}


def _callable_runtime_info(fn: Any | None) -> dict[str, str | None]:
    if fn is None:
        return {"module": None, "file": None, "source_sha256": None}
    try:
        source = inspect.getsource(fn)
        source_hash = hashlib.sha256(source.encode("utf-8")).hexdigest()[:12]
    except Exception:
        source_hash = None
    try:
        file_path = inspect.getfile(fn)
    except Exception:
        file_path = None
    return {
        "module": getattr(fn, "__module__", None),
        "file": file_path,
        "source_sha256": source_hash,
    }


def _nextier_dash_runtime_info() -> dict[str, Any]:
    info: dict[str, Any] = {
        "version": "not-installed",
        "url": None,
        "requested_revision": None,
        "commit_id": None,
    }
    try:
        distribution = importlib_metadata.distribution("nextier-dash")
    except importlib_metadata.PackageNotFoundError:
        return info

    info["version"] = distribution.version
    try:
        direct_url_raw = distribution.read_text("direct_url.json")
        direct_url = json.loads(direct_url_raw) if direct_url_raw else {}
        vcs_info = direct_url.get("vcs_info") or {}
        info.update(
            {
                "url": direct_url.get("url"),
                "requested_revision": vcs_info.get("requested_revision"),
                "commit_id": vcs_info.get("commit_id"),
            }
        )
    except Exception as exc:
        info["metadata_error"] = f"{type(exc).__name__}: {exc}"
    return info


def _log_auto_ds_runtime(
    logger: Any,
    *,
    algorithm_version: str,
    compute_auto_labels_for_stage: Any,
    compute_auto_labels_for_copper_tip: Any | None,
    closed_valid_stage_ids_from_frame: Any,
    ensure_auto_mid_label_column: Any,
) -> None:
    logger.info(
        "Auto DS runtime algorithm_version=%s nextier_dash=%s "
        "stage_fn=%s tip_fn=%s stage_id_selector_fn=%s mid_input_fn=%s",
        algorithm_version,
        _nextier_dash_runtime_info(),
        _callable_runtime_info(compute_auto_labels_for_stage),
        _callable_runtime_info(compute_auto_labels_for_copper_tip),
        _callable_runtime_info(closed_valid_stage_ids_from_frame),
        _callable_runtime_info(ensure_auto_mid_label_column),
    )


def _normalize_concentration_feature(value: str | None) -> str:
    normalized = str(value or DEFAULT_CONCENTRATION_FEATURE).strip().lower()
    if normalized not in CONCENTRATION_FEATURE_TO_COLUMN:
        raise ValueError(
            f"Unsupported concentration_feature={value!r}; expected one of "
            f"{sorted(CONCENTRATION_FEATURE_TO_COLUMN)}"
        )
    return normalized


def _concentration_features_to_process(value: str | None) -> tuple[str, ...]:
    normalized = _normalize_concentration_feature(value)
    if normalized == "all":
        return AUTO_EXPLICIT_CONCENTRATION_FEATURES
    return (normalized,)


def _auto_historical_cursor_key(
    *,
    stage_index_featurestore_key: str,
    auto_manifest_featurestore_key: str,
    algorithm_version: str,
    concentration_features: tuple[str, ...],
    well_name: str | None,
    well_names: list[str] | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    stage_num: float | None,
    requested_start_ts: pd.Timestamp | None,
    requested_end_ts: pd.Timestamp | None,
) -> str:
    payload = {
        "stage_index_featurestore_key": stage_index_featurestore_key,
        "auto_manifest_featurestore_key": auto_manifest_featurestore_key,
        "algorithm_version": algorithm_version,
        "concentration_features": list(concentration_features),
        "well_name": str(well_name).strip() if well_name and str(well_name).strip() else None,
        "well_names": _normalize_name_list(well_names),
        "fleet_name": str(fleet_name).strip() if fleet_name and str(fleet_name).strip() else None,
        "pad_name": str(pad_name).strip() if pad_name and str(pad_name).strip() else None,
        "include_fleet_names": _normalize_name_list(include_fleet_names),
        "exclude_fleet_names": _normalize_name_list(exclude_fleet_names),
        "stage_num": float(stage_num) if stage_num is not None else None,
        "requested_start_ts": format_dt(requested_start_ts),
        "requested_end_ts": format_dt(requested_end_ts),
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"__checkpoint__auto_historical_window_{digest}"



def _normalize_name_list(values: list[str] | None) -> list[str]:
    if not values:
        return []
    return [str(value).strip() for value in values if str(value).strip()]


def _add_named_list_filter(
    conditions: list[str],
    params: dict[str, Any],
    field_sql: str,
    values: list[str],
    param_prefix: str,
    *,
    negate: bool = False,
) -> None:
    if not values:
        return
    placeholders = []
    for index, value in enumerate(values):
        key = f"{param_prefix}_{index}"
        placeholders.append(f":{key}")
        params[key] = value
    expression = f"{field_sql} {'NOT IN' if negate else 'IN'} ({', '.join(placeholders)})"
    if negate:
        expression = f"({field_sql} IS NULL OR {expression})"
    conditions.append(expression)


def apply_fleet_filters(
    conditions: list[str],
    params: dict[str, Any],
    field_sql: str,
    *,
    fleet_name: str | None = None,
    include_fleet_names: list[str] | None = None,
    exclude_fleet_names: list[str] | None = None,
) -> None:
    include_fleets = _normalize_name_list(include_fleet_names)
    exclude_fleets = _normalize_name_list(exclude_fleet_names)
    if include_fleets and exclude_fleets:
        raise ValueError("include_fleet_names and exclude_fleet_names are mutually exclusive")
    if fleet_name and str(fleet_name).strip():
        conditions.append(f"{field_sql} = :fleet_name")
        params["fleet_name"] = str(fleet_name).strip()
    _add_named_list_filter(conditions, params, field_sql, include_fleets, "include_fleet")
    _add_named_list_filter(conditions, params, field_sql, exclude_fleets, "exclude_fleet", negate=True)


def _source_signature(stage_df: pd.DataFrame) -> str:
    if stage_df.empty:
        return "empty"
    min_ts = format_dt(stage_df["record_ts"].min())
    max_ts = format_dt(stage_df["record_ts"].max())
    row_count = len(stage_df)
    created_max = format_dt(stage_df["created_ts"].max()) if "created_ts" in stage_df else None
    return f"rows={row_count}|min={min_ts}|max={max_ts}|created_max={created_max}"


def _label_kind(label: str) -> str:
    value = str(label or "NA").strip()
    if value in POINT_LABELS:
        return "point"
    if value in WINDOW_LABELS:
        return "window"
    return "unknown"


def _idx_to_ts(times: pd.Series, idx: Any) -> str | None:
    if idx is None or pd.isna(idx):
        return None
    pos = int(idx)
    if pos < 0 or pos >= len(times):
        return None
    return format_dt(times.iloc[pos])


def _safe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric) or not np.isfinite(float(numeric)):
        return None
    return float(numeric)


def _prepare_auto_input(stage_df: pd.DataFrame) -> pd.DataFrame:
    out = stage_df.copy().sort_values(["record_ts", "created_ts", "telemetry_point_id"], kind="mergesort")
    out["datetime_fmt"] = pd.to_datetime(out["record_ts"], errors="coerce")
    out["name"] = out["well_name"].astype(str)
    if "id" not in out.columns and "well_id" in out.columns:
        out["id"] = out["well_id"]
    return out.dropna(subset=["datetime_fmt"]).reset_index(drop=True)


def _slice_stage_context_for_tip(
    *,
    stage_df: pd.DataFrame,
    auto_input: pd.DataFrame,
    tip_result: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    labels = tip_result.get("labels")
    if labels is None or len(labels) == len(auto_input):
        return stage_df, auto_input

    start_pos = int(tip_result.get("start_pos") or 0)
    end_pos = int(tip_result.get("end_pos") if tip_result.get("end_pos") is not None else start_pos + len(labels) - 1)
    if start_pos < 0 or end_pos < start_pos or end_pos >= len(auto_input):
        return stage_df, auto_input

    ordered_stage_df = (
        stage_df.copy()
        .sort_values(["record_ts", "created_ts", "telemetry_point_id"], kind="mergesort")
        .dropna(subset=["record_ts"])
        .reset_index(drop=True)
    )
    return (
        ordered_stage_df.iloc[start_pos : end_pos + 1].reset_index(drop=True),
        auto_input.iloc[start_pos : end_pos + 1].reset_index(drop=True),
    )


def _auto_tip_candidate_count(auto_input: pd.DataFrame) -> int:
    """Return how many copper tip ids are visible in the selected stage frame.

    The DS tip selector ranks positive stage ids and returns only the last
    max_tips ids. Historical runs may select an older stage from a frame that
    also contains adjacent labels, so the wrapper must not cap this to one.
    """
    stage_ids: set[int] = set()
    for column in ("copper_provisional", "copper_confirmed", "copper_continuous"):
        if column not in auto_input.columns:
            continue
        values = pd.to_numeric(auto_input[column], errors="coerce")
        stage_ids.update(int(value) for value in values.dropna().unique() if float(value) > 0)
    return max(1, len(stage_ids))


def _build_auto_frames(
    *,
    well_name: str,
    stage_num: int,
    stage_df: pd.DataFrame,
    auto_input: pd.DataFrame,
    auto_result: dict[str, Any],
    stage_closed: bool,
    mode: str,
    algorithm_version: str,
    processed_at: str,
    concentration_feature: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    labels = auto_result.get("labels")
    if labels is None or len(labels) != len(auto_input):
        labels = pd.Series(["NA"] * len(auto_input), dtype=object)
    label_series = pd.Series(labels).fillna("NA").astype(str).reset_index(drop=True)

    base = auto_input.reset_index(drop=True).copy()
    base["substage_label"] = label_series
    labeled = base[base["substage_label"].notna() & (base["substage_label"] != "NA")].copy()

    label_rows: list[dict[str, Any]] = []
    for _, row in labeled.iterrows():
        label = str(row["substage_label"])
        telemetry_id = str(row["telemetry_point_id"])
        label_rows.append(
            {
                "auto_label_id": f"{telemetry_id}:auto:{concentration_feature}:{stage_num}",
                "telemetry_point_id": telemetry_id,
                "fleet_name": row.get("fleet_name"),
                "pad_name": row.get("pad_name"),
                "well_name": row.get("well_name"),
                "well_id": row.get("well_id"),
                "api_num": row.get("api_num"),
                "record_ts": row.get("record_ts"),
                "created_ts": row.get("created_ts"),
                "stage_num": float(stage_num),
                "copper_continuous": float(stage_num),
                "substage_label": label,
                "label_kind": _label_kind(label),
                "stage_closed": bool(stage_closed),
                "mid_confirmed": bool(auto_result.get("mid_confirmed", False)),
                "stage_start_source": auto_result.get("stage_start_source"),
                "concentration_feature": concentration_feature,
                "conc_col": auto_result.get("conc_col"),
                "algorithm_version": algorithm_version,
                "source_mode": mode,
                "processed_at": processed_at,
            }
        )

    times = auto_result.get("times")
    if times is None or len(times) != len(auto_input):
        times = pd.to_datetime(auto_input["datetime_fmt"], errors="coerce")
    window_indices = auto_result.get("window_indices") or {}
    landmark_ts = auto_result.get("landmark_timestamps") or {}

    summary_row = {
        "auto_stage_uid": f"{well_name}:{stage_num}:{concentration_feature}",
        "fleet_name": last_non_null(stage_df["fleet_name"]) if "fleet_name" in stage_df else None,
        "pad_name": last_non_null(stage_df["pad_name"]) if "pad_name" in stage_df else None,
        "well_name": well_name,
        "well_id": last_non_null(stage_df["well_id"]) if "well_id" in stage_df else None,
        "api_num": last_non_null(stage_df["api_num"]) if "api_num" in stage_df else None,
        "stage_num": float(stage_num),
        "copper_continuous": float(stage_num),
        "stage_start_ts": format_dt(stage_df["record_ts"].min()),
        "stage_end_ts": format_dt(stage_df["record_ts"].max()),
        "source_min_record_ts": format_dt(stage_df["record_ts"].min()),
        "source_max_record_ts": format_dt(stage_df["record_ts"].max()),
        "open_well_ts": format_dt(landmark_ts.get("open_well")),
        "auto_stage_start_ts": format_dt(landmark_ts.get("stage_start")),
        "ttr_ts": format_dt(landmark_ts.get("ttr")),
        "auto_stage_end_ts": format_dt(landmark_ts.get("stage_end")),
        "close_well_ts": format_dt(landmark_ts.get("close_well")),
        "rampdown_trigger_ts": format_dt(landmark_ts.get("rampdown_trigger")),
        "last_pump_ts": format_dt(landmark_ts.get("last_pump")),
        "pad_start_ts": _idx_to_ts(times, window_indices.get("pad_start")),
        "pad_end_ts": _idx_to_ts(times, window_indices.get("pad_end")),
        "slurry_start_ts": _idx_to_ts(times, window_indices.get("slurry_start")),
        "slurry_end_ts": _idx_to_ts(times, window_indices.get("slurry_end")),
        "flush_start_ts": _idx_to_ts(times, window_indices.get("flush_start")),
        "flush_end_ts": _idx_to_ts(times, window_indices.get("flush_end")),
        "stage_closed": bool(stage_closed),
        "mid_confirmed": bool(auto_result.get("mid_confirmed", False)),
        "stage_start_source": auto_result.get("stage_start_source"),
        "concentration_feature": concentration_feature,
        "conc_col": auto_result.get("conc_col"),
        "resolved_design_rate": _safe_float(auto_result.get("resolved_design_rate")),
        "source_row_count": float(len(stage_df)),
        "label_row_count": float(len(label_rows)),
        "source_signature": _source_signature(stage_df),
        "algorithm_version": algorithm_version,
        "source_mode": mode,
        "processed_at": processed_at,
    }
    return pd.DataFrame(label_rows), pd.DataFrame([summary_row])


async def _write_auto_manifest(
    *,
    workspace_id: int,
    manifest_featurestore_key: str,
    manifest: dict[str, Any],
) -> None:
    await write_featurestore(
        featurestore_key=manifest_featurestore_key,
        workspace_id=workspace_id,
        df=to_polars_for_write(pd.DataFrame([manifest])),
        upsert=True,
    )


async def _write_auto_batch(
    *,
    workspace_id: int,
    featurestore_key: str,
    df: pd.DataFrame,
    bulk: bool = False,
) -> int:
    if df.empty:
        return 0
    await write_featurestore(
        featurestore_key=featurestore_key,
        workspace_id=workspace_id,
        df=to_polars_for_write(df),
        upsert=True,
        bulk=bulk,
    )
    return len(df)


class _AutoWriteBuffer:
    """Batch auto/substage writes without changing per-stage DS processing."""

    def __init__(
        self,
        *,
        workspace_id: int,
        labels_featurestore_key: str,
        summary_featurestore_key: str,
        manifest_featurestore_key: str,
        label_row_limit: int = AUTO_LABEL_WRITE_BATCH_ROWS,
    ) -> None:
        self.workspace_id = workspace_id
        self.labels_featurestore_key = labels_featurestore_key
        self.summary_featurestore_key = summary_featurestore_key
        self.manifest_featurestore_key = manifest_featurestore_key
        self.label_row_limit = label_row_limit
        self.label_batches: list[pd.DataFrame] = []
        self.label_rows = 0
        self.label_batches_written = 0
        self.label_rows_written = 0
        self.summary_batches: list[pd.DataFrame] = []
        self.summary_rows_written = 0
        self.manifest_rows: list[dict[str, Any]] = []
        self.manifest_rows_written = 0

    async def add_labels(self, rows: pd.DataFrame) -> None:
        if rows.empty:
            return
        self.label_batches.append(rows)
        self.label_rows += len(rows)
        if self.label_rows >= self.label_row_limit:
            await self.flush_labels(reason="threshold")

    def add_summary(self, rows: pd.DataFrame) -> None:
        if not rows.empty:
            self.summary_batches.append(rows)

    def add_manifest(self, row: dict[str, Any]) -> None:
        self.manifest_rows.append(row)

    async def flush_labels(self, *, reason: str) -> int:
        if not self.label_batches:
            return 0
        logger = get_run_logger()
        rows = pd.concat(self.label_batches, ignore_index=True)
        next_batch = self.label_batches_written + 1
        logger.info(
            "Auto label batch write start reason=%s featurestore=%s batch=%s rows=%s buffered_batches=%s total_rows_written_before=%s bulk=%s",
            reason,
            self.labels_featurestore_key,
            next_batch,
            len(rows),
            len(self.label_batches),
            self.label_rows_written,
            True,
        )
        row_count = await _write_auto_batch(
            workspace_id=self.workspace_id,
            featurestore_key=self.labels_featurestore_key,
            df=rows,
            bulk=True,
        )
        self.label_batches.clear()
        self.label_rows = 0
        self.label_batches_written = next_batch
        self.label_rows_written += row_count
        logger.info(
            "Auto label batch write complete reason=%s featurestore=%s batch=%s rows=%s total_rows_written=%s",
            reason,
            self.labels_featurestore_key,
            self.label_batches_written,
            row_count,
            self.label_rows_written,
        )
        return row_count

    async def flush_summaries(self, *, reason: str) -> int:
        if not self.summary_batches:
            return 0
        logger = get_run_logger()
        rows = pd.concat(self.summary_batches, ignore_index=True)
        logger.info(
            "Auto summary batch write start reason=%s featurestore=%s rows=%s",
            reason,
            self.summary_featurestore_key,
            len(rows),
        )
        row_count = await _write_auto_batch(
            workspace_id=self.workspace_id,
            featurestore_key=self.summary_featurestore_key,
            df=rows,
        )
        self.summary_batches.clear()
        self.summary_rows_written += row_count
        logger.info(
            "Auto summary batch write complete reason=%s featurestore=%s rows=%s total_rows_written=%s",
            reason,
            self.summary_featurestore_key,
            row_count,
            self.summary_rows_written,
        )
        return row_count

    async def flush_manifests(self, *, reason: str) -> int:
        if not self.manifest_rows:
            return 0
        logger = get_run_logger()
        rows = pd.DataFrame(self.manifest_rows)
        logger.info(
            "Auto manifest batch write start reason=%s featurestore=%s rows=%s",
            reason,
            self.manifest_featurestore_key,
            len(rows),
        )
        row_count = await _write_auto_batch(
            workspace_id=self.workspace_id,
            featurestore_key=self.manifest_featurestore_key,
            df=rows,
        )
        self.manifest_rows.clear()
        self.manifest_rows_written += row_count
        logger.info(
            "Auto manifest batch write complete reason=%s featurestore=%s rows=%s total_rows_written=%s",
            reason,
            self.manifest_featurestore_key,
            row_count,
            self.manifest_rows_written,
        )
        return row_count

    async def flush_all(self, *, reason: str) -> None:
        # Manifests are written last so historical progress advances only after
        # labels and summaries for the same stage-feature outputs are persisted.
        await self.flush_labels(reason=reason)
        await self.flush_summaries(reason=reason)
        await self.flush_manifests(reason=reason)
        logger = get_run_logger()
        logger.info(
            "Auto buffered writes complete reason=%s label_batches=%s label_rows=%s summary_rows=%s manifest_rows=%s",
            reason,
            self.label_batches_written,
            self.label_rows_written,
            self.summary_rows_written,
            self.manifest_rows_written,
        )


async def _delete_auto_outputs(
    *,
    workspace_id: int,
    well_name: str,
    stage_num: int,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    auto_labels_featurestore_key: str,
    auto_summary_featurestore_key: str,
    concentration_feature: str,
) -> dict[str, int | None]:
    # Reprocessing a stage replaces labels for the same well/time/concentration
    # window before upserting fresh rows. Summary rows are keyed by stage because
    # they describe the whole stage for one concentration feature.
    label_filters = [
        {"field": "well_name", "op": "eq", "value": well_name},
        {"field": "concentration_feature", "op": "eq", "value": concentration_feature},
        {"field": "record_ts", "op": "gte", "value": format_dt(start_ts)},
        {"field": "record_ts", "op": "lt", "value": format_dt(end_ts)},
    ]
    summary_filters = [
        {"field": "well_name", "op": "eq", "value": well_name},
        {"field": "stage_num", "op": "eq", "value": float(stage_num)},
        {"field": "concentration_feature", "op": "eq", "value": concentration_feature},
    ]
    return {
        auto_labels_featurestore_key: await delete_featurestore_records(
            featurestore_key=auto_labels_featurestore_key,
            workspace_id=workspace_id,
            filters=label_filters,
            require_primary_key_filter=False,
        ),
        auto_summary_featurestore_key: await delete_featurestore_records(
            featurestore_key=auto_summary_featurestore_key,
            workspace_id=workspace_id,
            filters=summary_filters,
            require_primary_key_filter=False,
        ),
    }


async def _select_candidate_stages(
    *,
    workspace_id: int,
    stage_index_featurestore_key: str,
    auto_manifest_featurestore_key: str,
    mode: str,
    well_name: str | None,
    well_names: list[str] | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    stage_num: float | None,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    max_wells: int,
    max_stages: int,
    skip_completed: bool,
    algorithm_version: str,
    concentration_features: tuple[str, ...],
) -> pd.DataFrame:
    # Auto/substage labeling is stage-index driven. This keeps the workflow
    # aligned with the current served stage windows, including provisional and
    # confirmed windows when continuous is not available yet.
    conditions = [
        "s.well_name IS NOT NULL",
        "s.stage_num IS NOT NULL",
        "CAST(s.stage_num AS DOUBLE PRECISION) > 0",
        "s.stage_start_ts IS NOT NULL",
        "s.stage_end_ts IS NOT NULL",
    ]
    params: dict[str, Any] = {
        "workflow_name": WORKFLOW_NAME,
        "mode": mode,
        "algorithm_version": algorithm_version,
    }
    requested_features_sql = " UNION ALL ".join(
        f"SELECT '{feature}' AS concentration_feature" for feature in concentration_features
    )
    targeted_wells = apply_well_name_filters(
        conditions,
        params,
        "s.well_name",
        well_name=well_name,
        well_names=well_names,
    )
    apply_fleet_filters(
        conditions,
        params,
        "s.fleet_name",
        fleet_name=fleet_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
    )
    if pad_name and str(pad_name).strip():
        conditions.append("s.pad_name = :pad_name")
        params["pad_name"] = str(pad_name).strip()
    if stage_num is not None:
        conditions.append("CAST(s.stage_num AS DOUBLE PRECISION) = :stage_num")
        params["stage_num"] = float(stage_num)
    if start_ts is not None:
        # A stage is selected if it overlaps the target window. This matters for
        # live/background because a stage can start before the lookback window
        # but still be active or need correction inside it.
        conditions.append("CAST(s.stage_end_ts AS TIMESTAMP) >= :start_time")
        params["start_time"] = format_dt(start_ts)
    if end_ts is not None:
        conditions.append("CAST(s.stage_start_ts AS TIMESTAMP) < :end_time")
        params["end_time"] = format_dt(end_ts)

    exclusion = ""
    if skip_completed:
        # Historical backfills can skip stages only when every requested
        # concentration feature has a terminal manifest entry. DS no-output
        # statuses are terminal for progress; actual failures remain pending.
        exclusion = f"""
            AND EXISTS (
                SELECT 1
                FROM requested_features rf
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM featurestore:{auto_manifest_featurestore_key} m
                    WHERE m.workflow_name = :workflow_name
                      AND m.mode = :mode
                      AND m.well_name = s.well_name
                      AND CAST(m.stage_num AS DOUBLE PRECISION) = CAST(s.stage_num AS DOUBLE PRECISION)
                      AND m.algorithm_version = :algorithm_version
                      AND m.concentration_feature = rf.concentration_feature
                      AND m.status IN ({AUTO_TERMINAL_PROGRESS_STATUS_SQL})
                      AND COALESCE(m.dry_run, false) = false
                )
            )
        """

    limit = max(1, int(max(max_wells, 1) * max(max_stages, 1)))
    feature_count = max(1, len(concentration_features))
    select_cols = f"""
        s.stage_uid,
        s.fleet_name,
        s.pad_name,
        s.well_name,
        s.well_id,
        s.api_num,
        CAST(s.stage_num AS DOUBLE PRECISION) AS stage_num,
        CAST(s.stage_start_ts AS TIMESTAMP) AS stage_start_ts,
        CAST(s.stage_end_ts AS TIMESTAMP) AS stage_end_ts,
        COALESCE(s.is_closed, false) AS is_closed,
        COALESCE(s.source_stage_column, 'copper_continuous') AS source_stage_column,
        COALESCE(s.stage_status, 'continuous') AS stage_status,
        s.algorithm_version AS copper_algorithm_version
    """
    where_sql = " AND ".join(conditions)

    if len(targeted_wells) == 1:
        # Explicit well runs are common for debugging and targeted backfills. Avoid the
        # global well ranking CTE in that case; it can force expensive planning/scans
        # even though the caller already selected the well.
        sql = f"""
            WITH requested_features AS (
                {requested_features_sql}
            )
            SELECT
                {select_cols},
                1 AS well_stage_rank,
                1 AS well_rank
            FROM featurestore:{stage_index_featurestore_key} s
            WHERE {where_sql}
              {exclusion}
            ORDER BY CAST(s.stage_num AS DOUBLE PRECISION), CAST(s.stage_start_ts AS TIMESTAMP)
            LIMIT {int(max_stages)}
        """
    else:
        # Scheduled runs should pick a bounded set of wells first, then load stages
        # only for those wells. Historical skip-completed still uses the manifest as
        # a completion ledger; live/background use it only as a fair rotation cursor.
        manifest_cte = ""
        candidate_wells_join = ""
        candidate_wells_filter = ""
        candidate_wells_order = "sc.first_stage_start_ts ASC, sc.well_name ASC"
        selected_well_rank_order = "cw.first_stage_start_ts ASC, b.well_name ASC"
        base_stage_join = ""
        base_extra_cols = ""
        stage_rank_order = "b.stage_num, b.stage_start_ts"
        if skip_completed:
            manifest_cte = f"""
            ,
            manifest_done AS (
                SELECT
                    sb.well_name,
                    COUNT(DISTINCT (sb.stage_num, rf.concentration_feature)) AS done_count
                FROM stage_base sb
                CROSS JOIN requested_features rf
                JOIN featurestore:{auto_manifest_featurestore_key} m
                  ON m.workflow_name = :workflow_name
                 AND m.mode = :mode
                 AND m.well_name = sb.well_name
                 AND CAST(m.stage_num AS DOUBLE PRECISION) = sb.stage_num
                 AND m.algorithm_version = :algorithm_version
                 AND m.concentration_feature = rf.concentration_feature
                 AND m.status IN ({AUTO_TERMINAL_PROGRESS_STATUS_SQL})
                 AND COALESCE(m.dry_run, false) = false
                GROUP BY sb.well_name
            )
            """
            candidate_wells_join = "LEFT JOIN manifest_done md ON md.well_name = sc.well_name"
            candidate_wells_filter = f"WHERE COALESCE(md.done_count, 0) < sc.stage_count * {feature_count}"
        elif mode in {"live", "background"}:
            # Live/background do not use skip_completed by default. Instead,
            # rotate wells/stages by the last manifest write so a capped run
            # does not keep selecting the same active well forever.
            manifest_cte = f"""
            ,
            manifest_well_last AS (
                SELECT
                    m.well_name,
                    MAX(CAST(COALESCE(m.completed_at, m.started_at) AS TIMESTAMP)) AS last_processed_at
                FROM featurestore:{auto_manifest_featurestore_key} m
                WHERE m.workflow_name = :workflow_name
                  AND m.mode = :mode
                  AND m.algorithm_version = :algorithm_version
                  AND COALESCE(m.dry_run, false) = false
                GROUP BY m.well_name
            ),
            manifest_stage_last AS (
                SELECT
                    m.well_name,
                    CAST(m.stage_num AS DOUBLE PRECISION) AS stage_num,
                    MAX(CAST(COALESCE(m.completed_at, m.started_at) AS TIMESTAMP)) AS last_stage_processed_at
                FROM featurestore:{auto_manifest_featurestore_key} m
                WHERE m.workflow_name = :workflow_name
                  AND m.mode = :mode
                  AND m.algorithm_version = :algorithm_version
                  AND COALESCE(m.dry_run, false) = false
                GROUP BY m.well_name, CAST(m.stage_num AS DOUBLE PRECISION)
            )
            """
            candidate_wells_join = "LEFT JOIN manifest_well_last mwl ON mwl.well_name = sc.well_name"
            candidate_wells_order = "mwl.last_processed_at ASC NULLS FIRST, sc.latest_stage_end_ts DESC, sc.well_name ASC"
            selected_well_rank_order = "cw.last_processed_at ASC NULLS FIRST, cw.latest_stage_end_ts DESC, b.well_name ASC"
            base_stage_join = "LEFT JOIN manifest_stage_last msl ON msl.well_name = s.well_name AND msl.stage_num = CAST(s.stage_num AS DOUBLE PRECISION)"
            base_extra_cols = ", msl.last_stage_processed_at"
            stage_rank_order = "b.last_stage_processed_at ASC NULLS FIRST, b.stage_end_ts DESC, b.stage_num ASC, b.stage_start_ts ASC"

        sql = f"""
            WITH requested_features AS (
                {requested_features_sql}
            ),
            stage_base AS (
                SELECT
                    s.well_name,
                    CAST(s.stage_num AS DOUBLE PRECISION) AS stage_num,
                    MIN(CAST(s.stage_start_ts AS TIMESTAMP)) AS stage_start_ts,
                    MAX(CAST(s.stage_end_ts AS TIMESTAMP)) AS stage_end_ts
                FROM featurestore:{stage_index_featurestore_key} s
                WHERE {where_sql}
                GROUP BY s.well_name, CAST(s.stage_num AS DOUBLE PRECISION)
            ),
            stage_counts AS (
                SELECT
                    well_name,
                    MIN(stage_start_ts) AS first_stage_start_ts,
                    MAX(stage_end_ts) AS latest_stage_end_ts,
                    COUNT(*) AS stage_count
                FROM stage_base
                GROUP BY well_name
            )
            {manifest_cte},
            candidate_wells AS (
                SELECT
                    sc.well_name,
                    sc.first_stage_start_ts,
                    sc.latest_stage_end_ts
                    {", mwl.last_processed_at" if mode in {"live", "background"} and not skip_completed else ""}
                FROM stage_counts sc
                {candidate_wells_join}
                {candidate_wells_filter}
                ORDER BY {candidate_wells_order}
                LIMIT {int(max_wells)}
            ),
            base AS (
                SELECT
                    {select_cols}
                    {base_extra_cols}
                FROM featurestore:{stage_index_featurestore_key} s
                JOIN candidate_wells cw ON cw.well_name = s.well_name
                {base_stage_join}
                WHERE {where_sql}
                  {exclusion}
            ),
            selected AS (
                SELECT
                    b.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY b.well_name
                        ORDER BY {stage_rank_order}
                    ) AS well_stage_rank,
                    DENSE_RANK() OVER (
                        ORDER BY {selected_well_rank_order}
                    ) AS well_rank
                FROM base b
                JOIN candidate_wells cw ON cw.well_name = b.well_name
            )
            SELECT *
            FROM selected
            WHERE well_stage_rank <= {int(max_stages)}
            ORDER BY well_rank ASC, well_stage_rank ASC
            LIMIT {limit}
        """

    logger = get_run_logger()
    logger.debug(
        "Auto candidate-stage select start mode=%s scope=(wells=%s fleet=%s include_fleets=%s exclude_fleets=%s pad=%s stage=%s) range=(%s,%s) max_wells=%s max_stages=%s skip_completed=%s concentration_features=%s",
        mode,
        targeted_wells or well_name,
        fleet_name,
        include_fleet_names,
        exclude_fleet_names,
        pad_name,
        stage_num,
        format_dt(start_ts),
        format_dt(end_ts),
        max_wells,
        max_stages,
        skip_completed,
        ",".join(concentration_features),
    )
    df = await query_store(
        sql=sql,
        workspace_id=workspace_id,
        params=params,
    )
    if df.is_empty():
        logger.info("Auto candidate-stage select complete selected=0")
        return pd.DataFrame()
    out = df.to_pandas().copy()
    out["stage_start_ts"] = pd.to_datetime(out["stage_start_ts"], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
    out["stage_end_ts"] = pd.to_datetime(out["stage_end_ts"], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
    out["stage_num"] = pd.to_numeric(out["stage_num"], errors="coerce")
    out = out.dropna(subset=["well_name", "stage_num", "stage_start_ts", "stage_end_ts"]).reset_index(drop=True)
    logger.info("Auto candidate-stage select complete selected=%s wells=%s", len(out), sorted(out["well_name"].astype(str).unique().tolist()) if not out.empty else [])
    return out



async def _count_pending_stages_in_window(
    *,
    workspace_id: int,
    stage_index_featurestore_key: str,
    auto_manifest_featurestore_key: str,
    mode: str,
    well_name: str | None,
    well_names: list[str] | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    stage_num: float | None,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    algorithm_version: str,
    concentration_features: tuple[str, ...],
) -> int:
    conditions = [
        "s.well_name IS NOT NULL",
        "s.stage_num IS NOT NULL",
        "CAST(s.stage_num AS DOUBLE PRECISION) > 0",
        "s.stage_start_ts IS NOT NULL",
        "s.stage_end_ts IS NOT NULL",
    ]
    params: dict[str, Any] = {
        "workflow_name": WORKFLOW_NAME,
        "mode": mode,
        "algorithm_version": algorithm_version,
    }
    requested_features_sql = " UNION ALL ".join(
        f"SELECT '{feature}' AS concentration_feature" for feature in concentration_features
    )
    apply_well_name_filters(conditions, params, "s.well_name", well_name=well_name, well_names=well_names)
    apply_fleet_filters(
        conditions,
        params,
        "s.fleet_name",
        fleet_name=fleet_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
    )
    if pad_name and str(pad_name).strip():
        conditions.append("s.pad_name = :pad_name")
        params["pad_name"] = str(pad_name).strip()
    if stage_num is not None:
        conditions.append("CAST(s.stage_num AS DOUBLE PRECISION) = :stage_num")
        params["stage_num"] = float(stage_num)
    if start_ts is not None:
        conditions.append("CAST(s.stage_end_ts AS TIMESTAMP) >= :start_time")
        params["start_time"] = format_dt(start_ts)
    if end_ts is not None:
        conditions.append("CAST(s.stage_start_ts AS TIMESTAMP) < :end_time")
        params["end_time"] = format_dt(end_ts)

    pending_df = await query_store(
        sql=f"""
            WITH requested_features AS (
                {requested_features_sql}
            ),
            stages AS (
                SELECT DISTINCT
                    s.well_name,
                    CAST(s.stage_num AS DOUBLE PRECISION) AS stage_num
                FROM featurestore:{stage_index_featurestore_key} s
                WHERE {" AND ".join(conditions)}
            )
            SELECT COUNT(*) AS pending_stages
            FROM stages s
            WHERE EXISTS (
                SELECT 1
                FROM requested_features rf
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM featurestore:{auto_manifest_featurestore_key} m
                    WHERE m.workflow_name = :workflow_name
                      AND m.mode = :mode
                      AND m.well_name = s.well_name
                      AND CAST(m.stage_num AS DOUBLE PRECISION) = s.stage_num
                      AND m.algorithm_version = :algorithm_version
                      AND m.concentration_feature = rf.concentration_feature
                      AND m.status IN ({AUTO_TERMINAL_PROGRESS_STATUS_SQL})
                      AND COALESCE(m.dry_run, false) = false
                )
            )
        """,
        workspace_id=workspace_id,
        params=params,
    )
    if pending_df.is_empty():
        return 0
    return int(pending_df.to_pandas().iloc[0].get("pending_stages") or 0)


async def _count_stage_candidates_in_window(
    *,
    workspace_id: int,
    stage_index_featurestore_key: str,
    well_name: str | None,
    well_names: list[str] | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    stage_num: float | None,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
) -> int:
    conditions = [
        "s.well_name IS NOT NULL",
        "s.stage_num IS NOT NULL",
        "CAST(s.stage_num AS DOUBLE PRECISION) > 0",
        "s.stage_start_ts IS NOT NULL",
        "s.stage_end_ts IS NOT NULL",
    ]
    params: dict[str, Any] = {}
    apply_well_name_filters(conditions, params, "s.well_name", well_name=well_name, well_names=well_names)
    apply_fleet_filters(
        conditions,
        params,
        "s.fleet_name",
        fleet_name=fleet_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
    )
    if pad_name and str(pad_name).strip():
        conditions.append("s.pad_name = :pad_name")
        params["pad_name"] = str(pad_name).strip()
    if stage_num is not None:
        conditions.append("CAST(s.stage_num AS DOUBLE PRECISION) = :stage_num")
        params["stage_num"] = float(stage_num)
    if start_ts is not None:
        conditions.append("CAST(s.stage_end_ts AS TIMESTAMP) >= :start_time")
        params["start_time"] = format_dt(start_ts)
    if end_ts is not None:
        conditions.append("CAST(s.stage_start_ts AS TIMESTAMP) < :end_time")
        params["end_time"] = format_dt(end_ts)

    count_df = await query_store(
        sql=f"""
            SELECT COUNT(*) AS stage_count
            FROM (
                SELECT DISTINCT
                    s.well_name,
                    CAST(s.stage_num AS DOUBLE PRECISION) AS stage_num
                FROM featurestore:{stage_index_featurestore_key} s
                WHERE {" AND ".join(conditions)}
            ) selected
        """,
        workspace_id=workspace_id,
        params=params,
    )
    if count_df.is_empty():
        return 0
    return int(count_df.to_pandas().iloc[0].get("stage_count") or 0)


async def _select_historical_time_window(
    *,
    workspace_id: int,
    workflow_id: int | None,
    cursor_key: str,
    stage_index_featurestore_key: str,
    auto_manifest_featurestore_key: str,
    well_name: str | None,
    well_names: list[str] | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    stage_num: float | None,
    total_start_ts: pd.Timestamp,
    total_end_ts: pd.Timestamp,
    chunk_hours: float | None,
    max_chunks: int | None,
    skip_completed: bool,
    algorithm_version: str,
    concentration_features: tuple[str, ...],
    reset_cursor: bool,
) -> dict[str, Any]:
    logger = get_run_logger()
    chunk_delta = pd.Timedelta(hours=max(float(chunk_hours or 24), 0.001))
    max_empty_skips = max(1, int(max_chunks or 1))
    state = await get_state(workflow_id=workflow_id, workspace_id=workspace_id) if workflow_id is not None else {}
    cursor_ts = parse_dt(state.get(cursor_key)) if isinstance(state, dict) else None
    if reset_cursor or cursor_ts is None or cursor_ts < total_start_ts or cursor_ts > total_end_ts:
        cursor_ts = total_start_ts
        if workflow_id is not None and reset_cursor:
            await set_state(workflow_id=workflow_id, workspace_id=workspace_id, key=cursor_key, value=format_dt(cursor_ts))

    skipped_empty_windows = 0
    while cursor_ts < total_end_ts:
        window_end_ts = min(cursor_ts + chunk_delta, total_end_ts)
        if skip_completed:
            selectable_stages = await _count_pending_stages_in_window(
                workspace_id=workspace_id,
                stage_index_featurestore_key=stage_index_featurestore_key,
                auto_manifest_featurestore_key=auto_manifest_featurestore_key,
                mode="historical",
                well_name=well_name,
                well_names=well_names,
                fleet_name=fleet_name,
                pad_name=pad_name,
                include_fleet_names=include_fleet_names,
                exclude_fleet_names=exclude_fleet_names,
                stage_num=stage_num,
                start_ts=cursor_ts,
                end_ts=window_end_ts,
                algorithm_version=algorithm_version,
                concentration_features=concentration_features,
            )
        else:
            selectable_stages = await _count_stage_candidates_in_window(
                workspace_id=workspace_id,
                stage_index_featurestore_key=stage_index_featurestore_key,
                well_name=well_name,
                well_names=well_names,
                fleet_name=fleet_name,
                pad_name=pad_name,
                include_fleet_names=include_fleet_names,
                exclude_fleet_names=exclude_fleet_names,
                stage_num=stage_num,
                start_ts=cursor_ts,
                end_ts=window_end_ts,
            )

        if selectable_stages > 0:
            return {
                "start_ts": cursor_ts,
                "end_ts": window_end_ts,
                "skipped_empty_windows": skipped_empty_windows,
                "selectable_stages": selectable_stages,
                "exhausted": False,
            }

        if workflow_id is not None:
            await set_state(workflow_id=workflow_id, workspace_id=workspace_id, key=cursor_key, value=format_dt(window_end_ts))
        cursor_ts = window_end_ts
        skipped_empty_windows += 1
        if skipped_empty_windows >= max_empty_skips:
            break

    exhausted = cursor_ts >= total_end_ts
    logger.debug(
        "Auto historical time-window cursor scan complete cursor=%s exhausted=%s skipped_empty_windows=%s",
        format_dt(cursor_ts),
        exhausted,
        skipped_empty_windows,
    )
    return {
        "start_ts": cursor_ts,
        "end_ts": cursor_ts if exhausted else min(cursor_ts + chunk_delta, total_end_ts),
        "skipped_empty_windows": skipped_empty_windows,
        "selectable_stages": 0,
        "exhausted": exhausted,
    }


def _is_aug18_substage_historical_progress_enabled(
    *,
    mode: str,
    auto_labels_featurestore_key: str,
    auto_summary_featurestore_key: str,
    auto_manifest_featurestore_key: str,
    algorithm_version: str,
) -> bool:
    if mode != "historical":
        return False
    key_blob = " ".join(
        [
            str(auto_labels_featurestore_key or ""),
            str(auto_summary_featurestore_key or ""),
            str(auto_manifest_featurestore_key or ""),
            str(algorithm_version or ""),
        ]
    ).lower()
    return any(token in key_blob for token in ("substage_silver_aug18", "substage_silver_aug27"))


async def _get_historical_stage_progress_summary(
    *,
    workspace_id: int,
    stage_index_featurestore_key: str,
    auto_manifest_featurestore_key: str,
    mode: str,
    well_name: str | None,
    well_names: list[str] | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    stage_num: float | None,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    algorithm_version: str,
    concentration_features: tuple[str, ...],
) -> dict[str, Any]:
    conditions = [
        "s.well_name IS NOT NULL",
        "s.stage_num IS NOT NULL",
        "CAST(s.stage_num AS DOUBLE PRECISION) > 0",
        "s.stage_start_ts IS NOT NULL",
        "s.stage_end_ts IS NOT NULL",
    ]
    params: dict[str, Any] = {
        "workflow_name": WORKFLOW_NAME,
        "mode": mode,
        "algorithm_version": algorithm_version,
    }
    requested_features_sql = " UNION ALL ".join(
        f"SELECT '{feature}' AS concentration_feature" for feature in concentration_features
    )
    apply_well_name_filters(conditions, params, "s.well_name", well_name=well_name, well_names=well_names)
    apply_fleet_filters(
        conditions,
        params,
        "s.fleet_name",
        fleet_name=fleet_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
    )
    if pad_name and str(pad_name).strip():
        conditions.append("s.pad_name = :pad_name")
        params["pad_name"] = str(pad_name).strip()
    if stage_num is not None:
        conditions.append("CAST(s.stage_num AS DOUBLE PRECISION) = :stage_num")
        params["stage_num"] = float(stage_num)
    if start_ts is not None:
        conditions.append("CAST(s.stage_end_ts AS TIMESTAMP) >= :start_time")
        params["start_time"] = format_dt(start_ts)
    if end_ts is not None:
        conditions.append("CAST(s.stage_start_ts AS TIMESTAMP) < :end_time")
        params["end_time"] = format_dt(end_ts)

    progress_df = await query_store(
        sql=f"""
            WITH requested_features AS (
                {requested_features_sql}
            ),
            stages AS (
                SELECT DISTINCT
                    s.well_name,
                    CAST(s.stage_num AS DOUBLE PRECISION) AS stage_num
                FROM featurestore:{stage_index_featurestore_key} s
                WHERE {" AND ".join(conditions)}
            ),
            stage_features AS (
                SELECT
                    s.well_name,
                    s.stage_num,
                    rf.concentration_feature,
                    EXISTS (
                        SELECT 1
                        FROM featurestore:{auto_manifest_featurestore_key} m
                        WHERE m.workflow_name = :workflow_name
                          AND m.mode = :mode
                          AND m.well_name = s.well_name
                          AND CAST(m.stage_num AS DOUBLE PRECISION) = s.stage_num
                          AND m.algorithm_version = :algorithm_version
                          AND m.concentration_feature = rf.concentration_feature
                          AND m.status IN ({AUTO_TERMINAL_PROGRESS_STATUS_SQL})
                          AND COALESCE(m.dry_run, false) = false
                    ) AS is_done
                FROM stages s
                CROSS JOIN requested_features rf
            ),
            per_stage AS (
                SELECT
                    well_name,
                    stage_num,
                    COUNT(*) AS expected_features,
                    SUM(CASE WHEN is_done THEN 1 ELSE 0 END) AS completed_features
                FROM stage_features
                GROUP BY well_name, stage_num
            )
            SELECT
                COUNT(*) AS total_stages,
                COALESCE(SUM(expected_features), 0) AS expected_stage_feature_runs,
                COALESCE(SUM(completed_features), 0) AS completed_stage_feature_runs,
                COALESCE(SUM(expected_features - completed_features), 0) AS remaining_stage_feature_runs,
                COUNT(*) FILTER (WHERE completed_features = expected_features) AS fully_processed_stages,
                COUNT(*) FILTER (WHERE completed_features > 0 AND completed_features < expected_features) AS partially_processed_stages,
                COUNT(*) FILTER (WHERE completed_features = 0) AS unprocessed_stages
            FROM per_stage
        """,
        workspace_id=workspace_id,
        params=params,
    )
    if progress_df.is_empty():
        return {
            "total_stages": 0,
            "expected_stage_feature_runs": 0,
            "completed_stage_feature_runs": 0,
            "remaining_stage_feature_runs": 0,
            "fully_processed_stages": 0,
            "partially_processed_stages": 0,
            "unprocessed_stages": 0,
            "pct_complete": 100.0,
            "is_complete": True,
        }

    row = progress_df.to_pandas().iloc[0].to_dict()
    progress = {
        "total_stages": int(row.get("total_stages") or 0),
        "expected_stage_feature_runs": int(row.get("expected_stage_feature_runs") or 0),
        "completed_stage_feature_runs": int(row.get("completed_stage_feature_runs") or 0),
        "remaining_stage_feature_runs": int(row.get("remaining_stage_feature_runs") or 0),
        "fully_processed_stages": int(row.get("fully_processed_stages") or 0),
        "partially_processed_stages": int(row.get("partially_processed_stages") or 0),
        "unprocessed_stages": int(row.get("unprocessed_stages") or 0),
    }
    expected = progress["expected_stage_feature_runs"]
    completed = progress["completed_stage_feature_runs"]
    progress["pct_complete"] = round((100.0 * completed / expected), 2) if expected else 100.0
    progress["is_complete"] = progress["remaining_stage_feature_runs"] == 0
    return progress


def _log_historical_stage_progress(
    logger: Any,
    *,
    label: str,
    progress: dict[str, Any] | None,
    cursor: str | None,
    total_start_ts: pd.Timestamp | None,
    total_end_ts: pd.Timestamp | None,
) -> None:
    if not progress:
        return
    logger.info(
        "Auto substage historical progress %s total_stages=%s fully_processed_stages=%s partially_processed_stages=%s unprocessed_stages=%s expected_stage_feature_runs=%s completed_stage_feature_runs=%s remaining_stage_feature_runs=%s pct_complete=%s cursor=%s total_range=(%s,%s)",
        label,
        progress.get("total_stages"),
        progress.get("fully_processed_stages"),
        progress.get("partially_processed_stages"),
        progress.get("unprocessed_stages"),
        progress.get("expected_stage_feature_runs"),
        progress.get("completed_stage_feature_runs"),
        progress.get("remaining_stage_feature_runs"),
        progress.get("pct_complete"),
        cursor,
        format_dt(total_start_ts),
        format_dt(total_end_ts),
    )
    if progress.get("is_complete"):
        logger.info(
            "Auto substage historical backfill COMPLETE total_stages=%s expected_stage_feature_runs=%s total_range=(%s,%s)",
            progress.get("total_stages"),
            progress.get("expected_stage_feature_runs"),
            format_dt(total_start_ts),
            format_dt(total_end_ts),
        )


async def _get_stage_index_scope_bounds(
    *,
    workspace_id: int,
    stage_index_featurestore_key: str,
    well_name: str | None,
    well_names: list[str] | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    stage_num: float | None,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    conditions = [
        "s.well_name IS NOT NULL",
        "s.stage_start_ts IS NOT NULL",
        "s.stage_end_ts IS NOT NULL",
    ]
    params: dict[str, Any] = {}
    apply_well_name_filters(conditions, params, "s.well_name", well_name=well_name, well_names=well_names)
    apply_fleet_filters(
        conditions,
        params,
        "s.fleet_name",
        fleet_name=fleet_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
    )
    if pad_name and str(pad_name).strip():
        conditions.append("s.pad_name = :pad_name")
        params["pad_name"] = str(pad_name).strip()
    if stage_num is not None:
        conditions.append("CAST(s.stage_num AS DOUBLE PRECISION) = :stage_num")
        params["stage_num"] = float(stage_num)

    bounds = await query_store(
        sql=f"""
            SELECT
                MIN(CAST(s.stage_start_ts AS TIMESTAMP)) AS start_ts,
                MAX(CAST(s.stage_end_ts AS TIMESTAMP)) AS end_ts
            FROM featurestore:{stage_index_featurestore_key} s
            WHERE {" AND ".join(conditions)}
        """,
        workspace_id=workspace_id,
        params=params,
    )
    if bounds.is_empty():
        return None, None
    row = bounds.to_pandas().iloc[0]
    start_ts = parse_dt(row.get("start_ts"))
    end_ts = parse_dt(row.get("end_ts"))
    if end_ts is not None:
        end_ts = end_ts + pd.Timedelta(microseconds=1)
    return start_ts, end_ts


async def _reset_auto_manifest_once(
    *,
    workspace_id: int,
    auto_manifest_featurestore_key: str,
    recompute_run_key: str,
    well_name: str | None,
    well_names: list[str] | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    stage_num: float | None,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    algorithm_version: str,
    concentration_features: tuple[str, ...],
    dry_run: bool,
) -> dict[str, Any]:
    key = str(recompute_run_key or "").strip()
    if not key:
        raise ValueError("recompute_run_key is required when recompute=true")
    if dry_run:
        return {"reset": False, "reason": "dry_run", "deleted": {}}

    targeted_wells = combine_well_names(well_name, well_names)
    include_fleets = _normalize_name_list(include_fleet_names)
    exclude_fleets = _normalize_name_list(exclude_fleet_names)
    if exclude_fleets and not targeted_wells:
        raise ValueError("recompute with exclude_fleet_names requires explicit well_name or well_names")

    feature_marker = ",".join(concentration_features)
    marker_conditions = [
        "m.workflow_name = :workflow_name",
        "m.mode = :mode",
        "m.status = :status",
        "m.run_id = :run_id",
        "m.algorithm_version = :algorithm_version",
        "m.requested_start_ts = :requested_start_ts",
        "m.requested_end_ts = :requested_end_ts",
        "m.concentration_feature = :feature_marker",
    ]
    marker_params: dict[str, Any] = {
        "workflow_name": WORKFLOW_NAME,
        "mode": "historical",
        "status": "recompute_started",
        "run_id": key,
        "algorithm_version": algorithm_version,
        "requested_start_ts": format_dt(start_ts),
        "requested_end_ts": format_dt(end_ts),
        "feature_marker": feature_marker,
    }
    marker_well = ",".join(targeted_wells) if targeted_wells else None
    if marker_well:
        marker_conditions.append("m.well_name = :marker_well")
        marker_params["marker_well"] = marker_well
    if fleet_name and str(fleet_name).strip():
        marker_conditions.append("m.fleet_name = :fleet_name")
        marker_params["fleet_name"] = str(fleet_name).strip()
    elif include_fleets:
        marker_conditions.append("m.fleet_name = :include_fleet_marker")
        marker_params["include_fleet_marker"] = ",".join(include_fleets)
    if pad_name and str(pad_name).strip():
        marker_conditions.append("m.pad_name = :pad_name")
        marker_params["pad_name"] = str(pad_name).strip()

    marker_df = await query_store(
        sql=f"""
            SELECT COUNT(*) AS marker_count
            FROM featurestore:{auto_manifest_featurestore_key} m
            WHERE {" AND ".join(marker_conditions)}
        """,
        workspace_id=workspace_id,
        params=marker_params,
    )
    if not marker_df.is_empty():
        marker_count = int(marker_df.to_pandas().iloc[0].get("marker_count") or 0)
        if marker_count > 0:
            return {"reset": False, "reason": "marker_exists", "deleted": {}}

    scopes: list[dict[str, str | None]] = []
    if targeted_wells:
        scopes = [{"well_name": name, "fleet_name": None, "pad_name": None} for name in targeted_wells]
    elif fleet_name and str(fleet_name).strip():
        scopes = [{"well_name": None, "fleet_name": str(fleet_name).strip(), "pad_name": pad_name}]
    elif include_fleets:
        scopes = [{"well_name": None, "fleet_name": name, "pad_name": pad_name} for name in include_fleets]
    else:
        scopes = [{"well_name": None, "fleet_name": None, "pad_name": pad_name}]

    deleted: dict[str, int] = {}
    for feature in concentration_features:
        total_deleted = 0
        for scope in scopes:
            filters = [
                {"field": "workflow_name", "op": "eq", "value": WORKFLOW_NAME},
                {"field": "mode", "op": "eq", "value": "historical"},
                {"field": "algorithm_version", "op": "eq", "value": algorithm_version},
                {"field": "concentration_feature", "op": "eq", "value": feature},
                # Recompute must reset any stage whose full stage window overlaps
                # the requested range. A stage can start before the range and still
                # be selected below, so filtering only on effective_start_ts would
                # leave stale terminal manifests behind.
                {"field": "effective_end_ts", "op": "gte", "value": format_dt(start_ts)},
                {"field": "effective_start_ts", "op": "lt", "value": format_dt(end_ts)},
            ]
            if scope.get("well_name"):
                filters.append({"field": "well_name", "op": "eq", "value": scope["well_name"]})
            if scope.get("fleet_name"):
                filters.append({"field": "fleet_name", "op": "eq", "value": scope["fleet_name"]})
            if scope.get("pad_name"):
                filters.append({"field": "pad_name", "op": "eq", "value": scope["pad_name"]})
            if stage_num is not None:
                filters.append({"field": "stage_num", "op": "eq", "value": float(stage_num)})
            count = await delete_featurestore_records(
                featurestore_key=auto_manifest_featurestore_key,
                workspace_id=workspace_id,
                filters=filters,
                require_primary_key_filter=False,
            )
            if count is not None:
                total_deleted += int(count)
        deleted[feature] = total_deleted

    completed_at = format_dt(now_utc())
    marker = {
        "manifest_id": f"{WORKFLOW_NAME}:{key}:recompute:{uuid4()}",
        "run_id": key,
        "workflow_name": WORKFLOW_NAME,
        "mode": "historical",
        "fleet_name": str(fleet_name).strip() if fleet_name and str(fleet_name).strip() else (",".join(include_fleets) if include_fleets else None),
        "pad_name": str(pad_name).strip() if pad_name and str(pad_name).strip() else None,
        "well_name": marker_well,
        "stage_num": float(stage_num) if stage_num is not None else None,
        "copper_continuous": float(stage_num) if stage_num is not None else None,
        "copper_stage_uid": None,
        "concentration_feature": feature_marker,
        "requested_start_ts": format_dt(start_ts),
        "requested_end_ts": format_dt(end_ts),
        "effective_start_ts": format_dt(start_ts),
        "effective_end_ts": format_dt(end_ts),
        "source_copper_featurestore_key": None,
        "auto_labels_featurestore_key": None,
        "auto_summary_featurestore_key": None,
        "source_signature": None,
        "source_row_count": 0.0,
        "label_rows_written": 0.0,
        "summary_rows_written": 0.0,
        "stage_closed": None,
        "mid_confirmed": None,
        "algorithm_version": algorithm_version,
        "dry_run": False,
        "status": "recompute_started",
        "error_message": "manifest reset marker",
        "started_at": completed_at,
        "completed_at": completed_at,
    }
    await _write_auto_manifest(
        workspace_id=workspace_id,
        manifest_featurestore_key=auto_manifest_featurestore_key,
        manifest=marker,
    )
    return {"reset": True, "reason": "marker_created", "deleted": deleted}


async def _load_copper_rows(
    *,
    workspace_id: int,
    copper_featurestore_key: str,
    well_name: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    stage_num: int | None = None,
    source_stage_column: str = "stage_num",
    filter_by_stage: bool = True,
) -> pd.DataFrame:
    logger = get_run_logger()
    params: dict[str, Any] = {
        "well_name": well_name,
        "start_time": format_dt(start_ts),
        "end_time": format_dt(end_ts),
    }
    stage_filter = ""
    allowed_stage_columns = {"stage_num", "copper_continuous", "copper_confirmed", "copper_provisional"}
    source_col = source_stage_column if source_stage_column in allowed_stage_columns else "stage_num"
    if stage_num is not None and filter_by_stage:
        # Source column comes from stage index. Continuous stages read
        # copper_continuous rows, confirmed stages read copper_confirmed rows,
        # and provisional stages read copper_provisional rows.
        params["stage_num"] = float(stage_num)
        stage_filter = f" AND {source_col} = :stage_num"

    logger.debug(
        "Auto copper-row load start well=%s stage=%s source_col=%s filter_by_stage=%s featurestore=%s start=%s end=%s",
        well_name,
        stage_num,
        source_col,
        filter_by_stage,
        copper_featurestore_key,
        format_dt(start_ts),
        format_dt(end_ts),
    )
    df = await query_store(
        sql=f"""
            SELECT {", ".join(COPPER_COLUMNS)}
            FROM featurestore:{copper_featurestore_key}
            WHERE well_name = :well_name
              AND record_ts >= :start_time
              AND record_ts <= :end_time
              {stage_filter}
        """,
        workspace_id=workspace_id,
        params=params,
    )
    if df.is_empty():
        logger.debug("Auto copper-row load complete well=%s stage=%s rows=0", well_name, stage_num)
        return pd.DataFrame(columns=COPPER_COLUMNS)
    out = df.to_pandas().copy()
    out["record_ts"] = pd.to_datetime(out["record_ts"], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
    out["created_ts"] = pd.to_datetime(out["created_ts"], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
    for col in ["copper_continuous", "copper_confirmed", "copper_provisional", "stage_num"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["well_name", "record_ts"]).reset_index(drop=True)
    logger.debug("Auto copper-row load complete well=%s stage=%s rows=%s", well_name, stage_num, len(out))
    return out


async def run_nextier_auto_labeling_v1(
    *,
    workspace_id: int,
    workflow_id: int | None = None,
    mode: str = "historical",
    copper_featurestore_key: str = COPPER_LABELS_FEATURESTORE_KEY,
    stage_index_featurestore_key: str = COPPER_STAGE_INDEX_FEATURESTORE_KEY,
    auto_labels_featurestore_key: str = AUTO_LABELS_FEATURESTORE_KEY,
    auto_summary_featurestore_key: str = AUTO_STAGE_SUMMARY_FEATURESTORE_KEY,
    auto_manifest_featurestore_key: str = AUTO_MANIFEST_FEATURESTORE_KEY,
    well_name: str | None = None,
    well_names: list[str] | None = None,
    fleet_name: str | None = None,
    pad_name: str | None = None,
    include_fleet_names: list[str] | None = None,
    exclude_fleet_names: list[str] | None = None,
    stage_num: float | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    lookback_hours: float = 24,
    chunk_hours: float | None = 24,
    max_chunks: int | None = 1,
    max_wells: int = 5,
    max_stages: int = 25,
    skip_completed: bool = True,
    delete_existing: bool = True,
    recompute: bool = False,
    recompute_run_key: str | None = None,
    dry_run: bool = False,
    algorithm_version: str = DEFAULT_AUTO_ALGORITHM_VERSION,
    concentration_feature: str = DEFAULT_CONCENTRATION_FEATURE,
    use_copper_tip_selector: bool = False,
) -> dict[str, Any]:
    logger = get_run_logger()
    mode = validate_mode(mode)
    copper_featurestore_key = copper_featurestore_key or COPPER_LABELS_FEATURESTORE_KEY
    stage_index_featurestore_key = stage_index_featurestore_key or COPPER_STAGE_INDEX_FEATURESTORE_KEY
    auto_labels_featurestore_key = auto_labels_featurestore_key or AUTO_LABELS_FEATURESTORE_KEY
    auto_summary_featurestore_key = auto_summary_featurestore_key or AUTO_STAGE_SUMMARY_FEATURESTORE_KEY
    auto_manifest_featurestore_key = auto_manifest_featurestore_key or AUTO_MANIFEST_FEATURESTORE_KEY
    algorithm_version = algorithm_version or DEFAULT_AUTO_ALGORITHM_VERSION
    concentration_features = _concentration_features_to_process(concentration_feature)
    requested_start_ts = parse_dt(start_time)
    requested_end_ts = parse_dt(end_time)
    effective_start_ts, effective_end_ts = resolve_window(mode, start_time, end_time, float(lookback_hours or 0))
    targeted_wells = combine_well_names(well_name, well_names)

    if mode == "historical" and (effective_start_ts is None or effective_end_ts is None):
        # Historical auto backfill can derive its bounds from the stage index.
        # This avoids scanning copper labels just to discover the range.
        scope_start_ts, scope_end_ts = await _get_stage_index_scope_bounds(
            workspace_id=workspace_id,
            stage_index_featurestore_key=stage_index_featurestore_key,
            well_name=well_name,
            well_names=well_names,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
            stage_num=stage_num,
        )
        if effective_start_ts is None:
            effective_start_ts = scope_start_ts
        if effective_end_ts is None:
            effective_end_ts = scope_end_ts

    recompute_reset = {"reset": False, "reason": "not_requested", "deleted": {}}
    if recompute:
        if mode != "historical":
            raise ValueError("recompute is only supported for historical Auto labeling")
        if effective_start_ts is None or effective_end_ts is None:
            raise ValueError("recompute requires a finite historical range after scope resolution")
        recompute_reset = await _reset_auto_manifest_once(
            workspace_id=workspace_id,
            auto_manifest_featurestore_key=auto_manifest_featurestore_key,
            recompute_run_key=str(recompute_run_key or "").strip(),
            well_name=well_name,
            well_names=well_names,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
            stage_num=stage_num,
            start_ts=effective_start_ts,
            end_ts=effective_end_ts,
            algorithm_version=algorithm_version,
            concentration_features=concentration_features,
            dry_run=dry_run,
        )
        logger.info("Auto historical recompute manifest reset result=%s", recompute_reset)

    historical_total_start_ts = effective_start_ts
    historical_total_end_ts = effective_end_ts
    historical_progress_enabled = _is_aug18_substage_historical_progress_enabled(
        mode=mode,
        auto_labels_featurestore_key=auto_labels_featurestore_key,
        auto_summary_featurestore_key=auto_summary_featurestore_key,
        auto_manifest_featurestore_key=auto_manifest_featurestore_key,
        algorithm_version=algorithm_version,
    )
    historical_progress_before: dict[str, Any] | None = None
    historical_cursor_key: str | None = None
    historical_window_exhausted = False
    if mode == "historical":
        if historical_total_start_ts is None or historical_total_end_ts is None:
            logger.info(
                "Auto historical has no stage-index bounds for scope=(wells=%s fleet=%s include_fleets=%s exclude_fleets=%s pad=%s stage=%s)",
                targeted_wells or well_name,
                fleet_name,
                include_fleet_names,
                exclude_fleet_names,
                pad_name,
                stage_num,
            )
            return {
                "stages_processed": 0,
                "mode": mode,
                "dry_run": bool(dry_run),
                "recompute_reset": recompute_reset,
                "historical_pending_after": 0,
                "historical_cursor_advanced_to": None,
                "historical_progress_before": None,
                "historical_progress_after": None,
                "results": [],
            }

        # Historical auto/substage backfill is time-window based. The cursor
        # advances over chunk_hours windows and stages are selected by overlap,
        # so partial stages at a window edge are still processed as full stages.
        historical_cursor_key = _auto_historical_cursor_key(
            stage_index_featurestore_key=stage_index_featurestore_key,
            auto_manifest_featurestore_key=auto_manifest_featurestore_key,
            algorithm_version=algorithm_version,
            concentration_features=concentration_features,
            well_name=well_name,
            well_names=well_names,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
            stage_num=stage_num,
            requested_start_ts=requested_start_ts,
            requested_end_ts=requested_end_ts,
        )
        historical_window = await _select_historical_time_window(
            workspace_id=workspace_id,
            workflow_id=workflow_id,
            cursor_key=historical_cursor_key,
            stage_index_featurestore_key=stage_index_featurestore_key,
            auto_manifest_featurestore_key=auto_manifest_featurestore_key,
            well_name=well_name,
            well_names=well_names,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
            stage_num=stage_num,
            total_start_ts=historical_total_start_ts,
            total_end_ts=historical_total_end_ts,
            chunk_hours=chunk_hours,
            max_chunks=max_chunks,
            skip_completed=skip_completed,
            algorithm_version=algorithm_version,
            concentration_features=concentration_features,
            reset_cursor=bool(recompute_reset.get("reset")),
        )
        effective_start_ts = historical_window["start_ts"]
        effective_end_ts = historical_window["end_ts"]
        historical_window_exhausted = bool(historical_window["exhausted"])
        logger.info(
            "Auto historical window selected cursor_key=%s total_range=(%s,%s) window=(%s,%s) skipped_empty_windows=%s selectable_stages=%s chunk_hours=%s max_chunks=%s exhausted=%s",
            historical_cursor_key,
            format_dt(historical_total_start_ts),
            format_dt(historical_total_end_ts),
            format_dt(effective_start_ts),
            format_dt(effective_end_ts),
            historical_window["skipped_empty_windows"],
            historical_window["selectable_stages"],
            chunk_hours,
            max_chunks,
            historical_window_exhausted,
        )

    logger.info(
        "Auto workflow planning start mode=%s scope=(wells=%s fleet=%s include_fleets=%s exclude_fleets=%s pad=%s stage=%s) range=(%s,%s) max_wells=%s max_stages=%s skip_completed=%s concentration_features=%s",
        mode,
        targeted_wells or well_name,
        fleet_name,
        include_fleet_names,
        exclude_fleet_names,
        pad_name,
        stage_num,
        format_dt(effective_start_ts),
        format_dt(effective_end_ts),
        max_wells,
        max_stages,
        skip_completed,
        ",".join(concentration_features),
    )
    if (
        historical_progress_enabled
        and historical_total_start_ts is not None
        and historical_total_end_ts is not None
    ):
        historical_progress_before = await _get_historical_stage_progress_summary(
            workspace_id=workspace_id,
            stage_index_featurestore_key=stage_index_featurestore_key,
            auto_manifest_featurestore_key=auto_manifest_featurestore_key,
            mode=mode,
            well_name=well_name,
            well_names=well_names,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
            stage_num=stage_num,
            start_ts=historical_total_start_ts,
            end_ts=historical_total_end_ts,
            algorithm_version=algorithm_version,
            concentration_features=concentration_features,
        )
        _log_historical_stage_progress(
            logger,
            label="before",
            progress=historical_progress_before,
            cursor=format_dt(effective_start_ts),
            total_start_ts=historical_total_start_ts,
            total_end_ts=historical_total_end_ts,
        )
    if mode == "historical" and historical_window_exhausted:
        logger.info(
            "Auto historical range exhausted; no candidate-stage query needed cursor=%s total_range=(%s,%s)",
            format_dt(effective_start_ts),
            format_dt(historical_total_start_ts),
            format_dt(historical_total_end_ts),
        )
        historical_progress_after = historical_progress_before
        if (
            historical_progress_enabled
            and historical_total_start_ts is not None
            and historical_total_end_ts is not None
        ):
            historical_progress_after = await _get_historical_stage_progress_summary(
                workspace_id=workspace_id,
                stage_index_featurestore_key=stage_index_featurestore_key,
                auto_manifest_featurestore_key=auto_manifest_featurestore_key,
                mode=mode,
                well_name=well_name,
                well_names=well_names,
                fleet_name=fleet_name,
                pad_name=pad_name,
                include_fleet_names=include_fleet_names,
                exclude_fleet_names=exclude_fleet_names,
                stage_num=stage_num,
                start_ts=historical_total_start_ts,
                end_ts=historical_total_end_ts,
                algorithm_version=algorithm_version,
                concentration_features=concentration_features,
            )
            _log_historical_stage_progress(
                logger,
                label="after_exhausted",
                progress=historical_progress_after,
                cursor=format_dt(effective_start_ts),
                total_start_ts=historical_total_start_ts,
                total_end_ts=historical_total_end_ts,
            )
        return {
            "stages_processed": 0,
            "mode": mode,
            "dry_run": bool(dry_run),
            "recompute_reset": recompute_reset,
            "historical_pending_after": 0,
            "historical_cursor_advanced_to": format_dt(effective_start_ts),
            "historical_progress_before": historical_progress_before,
            "historical_progress_after": historical_progress_after,
            "results": [],
        }
    stages = await _select_candidate_stages(
        workspace_id=workspace_id,
        stage_index_featurestore_key=stage_index_featurestore_key,
        auto_manifest_featurestore_key=auto_manifest_featurestore_key,
        mode=mode,
        well_name=well_name,
        well_names=well_names,
        fleet_name=fleet_name,
        pad_name=pad_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
        stage_num=stage_num,
        start_ts=effective_start_ts,
        end_ts=effective_end_ts,
        max_wells=int(max_wells or 5),
        max_stages=int(max_stages or 25),
        skip_completed=bool(skip_completed),
        algorithm_version=algorithm_version,
        concentration_features=concentration_features,
    )
    if stages.empty:
        historical_pending_after: int | None = None
        historical_cursor_advanced_to: str | None = None
        historical_progress_after: dict[str, Any] | None = None
        if (
            mode == "historical"
            and effective_start_ts is not None
            and effective_end_ts is not None
        ):
            historical_pending_after = await _count_pending_stages_in_window(
                workspace_id=workspace_id,
                stage_index_featurestore_key=stage_index_featurestore_key,
                auto_manifest_featurestore_key=auto_manifest_featurestore_key,
                mode=mode,
                well_name=well_name,
                well_names=well_names,
                fleet_name=fleet_name,
                pad_name=pad_name,
                include_fleet_names=include_fleet_names,
                exclude_fleet_names=exclude_fleet_names,
                stage_num=stage_num,
                start_ts=effective_start_ts,
                end_ts=effective_end_ts,
                algorithm_version=algorithm_version,
                concentration_features=concentration_features,
            )
            if historical_pending_after == 0 and workflow_id is not None and historical_cursor_key:
                await set_state(
                    workflow_id=workflow_id,
                    workspace_id=workspace_id,
                    key=historical_cursor_key,
                    value=format_dt(effective_end_ts),
                )
                historical_cursor_advanced_to = format_dt(effective_end_ts)
            logger.info(
                "Auto historical empty window pending_after=%s cursor=%s",
                historical_pending_after,
                historical_cursor_advanced_to or format_dt(effective_start_ts),
            )
        if (
            historical_progress_enabled
            and historical_total_start_ts is not None
            and historical_total_end_ts is not None
        ):
            historical_progress_after = await _get_historical_stage_progress_summary(
                workspace_id=workspace_id,
                stage_index_featurestore_key=stage_index_featurestore_key,
                auto_manifest_featurestore_key=auto_manifest_featurestore_key,
                mode=mode,
                well_name=well_name,
                well_names=well_names,
                fleet_name=fleet_name,
                pad_name=pad_name,
                include_fleet_names=include_fleet_names,
                exclude_fleet_names=exclude_fleet_names,
                stage_num=stage_num,
                start_ts=historical_total_start_ts,
                end_ts=historical_total_end_ts,
                algorithm_version=algorithm_version,
                concentration_features=concentration_features,
            )
            _log_historical_stage_progress(
                logger,
                label="after_empty_window",
                progress=historical_progress_after,
                cursor=historical_cursor_advanced_to or format_dt(effective_start_ts),
                total_start_ts=historical_total_start_ts,
                total_end_ts=historical_total_end_ts,
            )
        logger.info("No copper stages selected for Auto labeling mode=%s scope=(well=%s fleet=%s pad=%s stage=%s) range=(%s,%s)", mode, well_name, fleet_name, pad_name, stage_num, format_dt(effective_start_ts), format_dt(effective_end_ts))
        return {
            "stages_processed": 0,
            "mode": mode,
            "dry_run": bool(dry_run),
            "recompute_reset": recompute_reset,
            "historical_pending_after": historical_pending_after,
            "historical_cursor_advanced_to": historical_cursor_advanced_to,
            "historical_progress_before": historical_progress_before,
            "historical_progress_after": historical_progress_after,
            "results": [],
        }

    from nextier_core.auto_layer import compute_auto_labels_for_stage
    if use_copper_tip_selector:
        from nextier_core.auto_layer import compute_auto_labels_for_copper_tip
    else:
        compute_auto_labels_for_copper_tip = None
    from nextier_core.copper_layer_runtime import closed_valid_stage_ids_from_frame
    from nextier_utils.labeling.auto_mid_input import ensure_auto_mid_label_column
    _log_auto_ds_runtime(
        logger,
        algorithm_version=algorithm_version,
        compute_auto_labels_for_stage=compute_auto_labels_for_stage,
        compute_auto_labels_for_copper_tip=compute_auto_labels_for_copper_tip,
        closed_valid_stage_ids_from_frame=closed_valid_stage_ids_from_frame,
        ensure_auto_mid_label_column=ensure_auto_mid_label_column,
    )

    run_id = str(uuid4())
    processed_at = format_dt(now_utc())
    results: list[dict[str, Any]] = []
    write_buffer = _AutoWriteBuffer(
        workspace_id=workspace_id,
        labels_featurestore_key=auto_labels_featurestore_key,
        summary_featurestore_key=auto_summary_featurestore_key,
        manifest_featurestore_key=auto_manifest_featurestore_key,
    )

    logger.info(
        "Selected %s copper stages for Auto labeling mode=%s concentration_features=%s dry_run=%s delete_existing=%s range=(%s,%s)",
        len(stages), mode, ",".join(concentration_features), dry_run, delete_existing, format_dt(effective_start_ts), format_dt(effective_end_ts),
    )
    selected_stage_plan = {
        str(group_well): [int(float(value)) for value in group["stage_num"].to_list()]
        for group_well, group in stages.groupby("well_name", sort=False)
    }
    logger.info("Auto selected stage plan=%s", selected_stage_plan)

    for selected_well, well_stages in stages.groupby("well_name", sort=False):
        # Each selected stage is processed independently for each requested
        # concentration feature. The manifest records the exact source signature
        # so future historical runs can skip or deliberately recompute it.
        well_result_start = len(results)
        well_stage_nums = [int(float(value)) for value in well_stages["stage_num"].to_list()]
        logger.info(
            "Auto well processing start well=%s stages=%s stage_nums=%s",
            selected_well,
            len(well_stages),
            well_stage_nums,
        )

        for _, stage in well_stages.iterrows():
            stage_id = int(float(stage["stage_num"]))
            stage_start = stage["stage_start_ts"]
            stage_end = stage["stage_end_ts"]
            stage_results_start = len(results)
            use_tip_selector = compute_auto_labels_for_copper_tip is not None
            load_start = stage_start
            load_end = stage_end
            if use_tip_selector:
                # Give the DS copper-tip selector a small edge buffer so it can
                # resolve provisional/confirmed/continuous spans at stage edges.
                # The selected stage is still processed and written stage-by-stage.
                edge_buffer = pd.Timedelta(minutes=AUTO_TIP_STAGE_EDGE_BUFFER_MINUTES)
                load_start = stage_start - edge_buffer
                load_end = stage_end + edge_buffer
            logger.debug(
                "Auto stage-row load planning well=%s stage=%s start=%s end=%s load_start=%s load_end=%s tip_selector=%s",
                selected_well,
                stage_id,
                format_dt(stage_start),
                format_dt(stage_end),
                format_dt(load_start),
                format_dt(load_end),
                use_tip_selector,
            )
            stage_df = await _load_copper_rows(
                workspace_id=workspace_id,
                copper_featurestore_key=copper_featurestore_key,
                well_name=str(selected_well),
                start_ts=load_start,
                end_ts=load_end,
                stage_num=stage_id,
                source_stage_column=str(stage.get("source_stage_column") or "stage_num"),
                filter_by_stage=not use_tip_selector,
            )
            closed_ids = closed_valid_stage_ids_from_frame(stage_df) if not stage_df.empty else set()
            source_signature = _source_signature(stage_df)
            stage_closed = bool(stage_id in closed_ids or bool(stage.get("is_closed")))
            auto_input = None
            if not stage_df.empty:
                auto_input = ensure_auto_mid_label_column(_prepare_auto_input(stage_df), datetime_col="datetime_fmt")

            for selected_concentration_feature in concentration_features:
                started_at = format_dt(now_utc())
                conc_col_preferred = CONCENTRATION_FEATURE_TO_COLUMN[selected_concentration_feature]
                status = "written"
                error_message = None
                label_rows = pd.DataFrame()
                summary_rows = pd.DataFrame()
                mid_confirmed = False

                try:
                    if stage_df.empty or auto_input is None or auto_input.empty:
                        status = "skipped_no_copper_rows"
                        logger.debug(
                            "Auto skipped well=%s stage=%s concentration_feature=%s reason=no_copper_rows",
                            selected_well,
                            stage_id,
                            selected_concentration_feature,
                        )
                    else:
                        logger.debug("Auto stage compute start well=%s stage=%s concentration_feature=%s rows=%s", selected_well, stage_id, selected_concentration_feature, len(stage_df))
                        if compute_auto_labels_for_copper_tip is not None:
                            # Use the DS copper-tip entrypoint directly.
                            # The wrapper only avoids excluding selected stages:
                            # the DS selector returns the last max_tips ids, so
                            # pass the visible id count and then select the exact
                            # stage result requested by this workflow iteration.
                            tip_candidate_count = _auto_tip_candidate_count(auto_input)
                            tip_results = compute_auto_labels_for_copper_tip(
                                str(selected_well),
                                auto_input,
                                auto_input.get("copper_provisional"),
                                auto_input.get("copper_confirmed"),
                                auto_input.get("copper_continuous"),
                                max_tips=tip_candidate_count,
                                closed_ids=closed_ids,
                                datetime_col="datetime_fmt",
                                rate_col="rate_slurry",
                                conc_col_preferred=conc_col_preferred,
                                include_features=False,
                            )
                            matching_tip = next(
                                (
                                    result
                                    for result in tip_results
                                    if int(float(result.get("stage_id") or -1)) == stage_id
                                       and not result.get("error")
                                ),
                                None,
                            )
                            if matching_tip is None:
                                status = "skipped_no_tip_window"
                                logger.debug(
                                    "Auto beta skipped well=%s stage=%s concentration_feature=%s reason=no_matching_tip tip_candidates=%s",
                                    selected_well,
                                    stage_id,
                                    selected_concentration_feature,
                                    tip_candidate_count,
                                )
                                auto_result = {}
                            else:
                                auto_result = matching_tip
                        else:
                            auto_result = compute_auto_labels_for_stage(
                                well=str(selected_well),
                                df_stage=auto_input,
                                start_pos=0,
                                end_pos=len(auto_input) - 1,
                                stage_val=float(stage_id),
                                datetime_col="datetime_fmt",
                                rate_col="rate_slurry",
                                conc_col_preferred=conc_col_preferred,
                                stage_closed=stage_closed,
                            )
                        if status == "written":
                            frame_stage_df = stage_df
                            frame_auto_input = auto_input
                            if compute_auto_labels_for_copper_tip is not None:
                                frame_stage_df, frame_auto_input = _slice_stage_context_for_tip(
                                    stage_df=stage_df,
                                    auto_input=auto_input,
                                    tip_result=auto_result,
                                )
                            mid_confirmed = bool(auto_result.get("mid_confirmed", False))
                            logger.debug("Auto stage compute complete well=%s stage=%s concentration_feature=%s", selected_well, stage_id, selected_concentration_feature)
                            logger.debug("Auto frame build start well=%s stage=%s concentration_feature=%s", selected_well, stage_id, selected_concentration_feature)
                            label_rows, summary_rows = _build_auto_frames(
                                well_name=str(selected_well),
                                stage_num=stage_id,
                                stage_df=frame_stage_df,
                                auto_input=frame_auto_input,
                                auto_result=auto_result,
                                stage_closed=stage_closed,
                                mode=mode,
                                algorithm_version=algorithm_version,
                                processed_at=processed_at,
                                concentration_feature=selected_concentration_feature,
                            )

                            logger.debug("Auto frame build complete well=%s stage=%s concentration_feature=%s labels=%s summary=%s", selected_well, stage_id, selected_concentration_feature, len(label_rows), len(summary_rows))
                            logger.debug(
                                "Auto plan well=%s stage=%s concentration_feature=%s conc_col=%s rows=%s label_rows=%s stage_closed=%s mid_confirmed=%s stage_start_source=%s",
                                selected_well,
                                stage_id,
                                selected_concentration_feature,
                                summary_rows.iloc[0].get("conc_col") if not summary_rows.empty else None,
                                len(stage_df),
                                len(label_rows),
                                stage_closed,
                                mid_confirmed,
                                summary_rows.iloc[0].get("stage_start_source") if not summary_rows.empty else None,
                            )

                        if not dry_run:
                            if delete_existing:
                                logger.debug("Auto delete start well=%s stage=%s concentration_feature=%s", selected_well, stage_id, selected_concentration_feature)
                                deleted = await _delete_auto_outputs(
                                    workspace_id=workspace_id,
                                    well_name=str(selected_well),
                                    stage_num=stage_id,
                                    start_ts=stage_start,
                                    end_ts=stage_end,
                                    auto_labels_featurestore_key=auto_labels_featurestore_key,
                                    auto_summary_featurestore_key=auto_summary_featurestore_key,
                                    concentration_feature=selected_concentration_feature,
                                )
                                logger.debug(
                                    "Auto delete complete well=%s stage=%s concentration_feature=%s deleted=%s",
                                    selected_well,
                                    stage_id,
                                    selected_concentration_feature,
                                    deleted,
                                )
                            if not label_rows.empty:
                                await write_buffer.add_labels(label_rows)
                            if not summary_rows.empty:
                                write_buffer.add_summary(summary_rows)
                            logger.debug(
                                "Auto write buffered well=%s stage=%s concentration_feature=%s labels=%s summary=%s buffered_label_rows=%s",
                                selected_well,
                                stage_id,
                                selected_concentration_feature,
                                len(label_rows),
                                len(summary_rows),
                                write_buffer.label_rows,
                            )
                        else:
                            status = "dry_run"
                except Exception as exc:
                    logger.exception(
                        "Auto labeling failed well=%s stage=%s concentration_feature=%s",
                        selected_well,
                        stage_id,
                        selected_concentration_feature,
                    )
                    status = "failed"
                    error_message = str(exc)
                    if dry_run:
                        raise

                completed_at = format_dt(now_utc())
                manifest = {
                    "manifest_id": f"{run_id}:{selected_well}:{stage_id}:{selected_concentration_feature}:{uuid4()}",
                    "run_id": run_id,
                    "workflow_name": WORKFLOW_NAME,
                    "mode": mode,
                    "fleet_name": stage.get("fleet_name"),
                    "pad_name": stage.get("pad_name"),
                    "well_name": str(selected_well),
                    "stage_num": float(stage_id),
                    "copper_continuous": float(stage_id),
                    "copper_stage_uid": stage.get("stage_uid"),
                    "concentration_feature": selected_concentration_feature,
                    "requested_start_ts": format_dt(requested_start_ts),
                    "requested_end_ts": format_dt(requested_end_ts),
                    "effective_start_ts": format_dt(stage.get("stage_start_ts")),
                    "effective_end_ts": format_dt(stage.get("stage_end_ts")),
                    "source_copper_featurestore_key": copper_featurestore_key,
                    "auto_labels_featurestore_key": auto_labels_featurestore_key,
                    "auto_summary_featurestore_key": auto_summary_featurestore_key,
                    "source_signature": source_signature,
                    "source_row_count": float(len(stage_df)),
                    "label_rows_written": float(len(label_rows)),
                    "summary_rows_written": float(len(summary_rows)),
                    "stage_closed": bool(stage_closed),
                    "mid_confirmed": bool(mid_confirmed),
                    "algorithm_version": algorithm_version,
                    "dry_run": bool(dry_run),
                    "status": status,
                    "error_message": error_message,
                    "started_at": started_at,
                    "completed_at": completed_at,
                }
                if not dry_run:
                    write_buffer.add_manifest(manifest)
                if status == "failed":
                    if not dry_run:
                        await write_buffer.flush_all(reason="before_failure")
                    raise RuntimeError(error_message or "Auto labeling failed")
                logger.debug("Auto stage processing complete well=%s stage=%s concentration_feature=%s status=%s", selected_well, stage_id, selected_concentration_feature, status)
                results.append(
                    {
                        "well_name": str(selected_well),
                        "stage_num": stage_id,
                        "status": status,
                        "source_rows": len(stage_df),
                        "label_rows": len(label_rows),
                        "stage_closed": stage_closed,
                        "mid_confirmed": mid_confirmed,
                        "concentration_feature": selected_concentration_feature,
                    }
                )
            if not dry_run:
                stage_results = results[stage_results_start:]
                await write_buffer.flush_all(reason=f"stage_complete:{selected_well}:{stage_id}")
                stage_status_counts: dict[str, int] = {}
                for item in stage_results:
                    status = str(item.get("status") or "unknown")
                    stage_status_counts[status] = stage_status_counts.get(status, 0) + 1
                logger.info(
                    "Auto stage buffered writes committed well=%s stage=%s results=%s statuses=%s",
                    selected_well,
                    stage_id,
                    len(stage_results),
                    stage_status_counts,
                )

        well_results = results[well_result_start:]
        status_counts: dict[str, int] = {}
        for item in well_results:
            result_status = str(item.get("status") or "unknown")
            status_counts[result_status] = status_counts.get(result_status, 0) + 1
        logger.info(
            "Auto well processing complete well=%s stages=%s results=%s statuses=%s",
            selected_well,
            len(well_stages),
            len(well_results),
            status_counts,
        )

    if not dry_run:
        await write_buffer.flush_all(reason="final")

    historical_pending_after: int | None = None
    historical_cursor_advanced_to: str | None = None
    historical_progress_after: dict[str, Any] | None = None
    if (
        mode == "historical"
        and effective_start_ts is not None
        and effective_end_ts is not None
    ):
        historical_pending_after = await _count_pending_stages_in_window(
            workspace_id=workspace_id,
            stage_index_featurestore_key=stage_index_featurestore_key,
            auto_manifest_featurestore_key=auto_manifest_featurestore_key,
            mode=mode,
            well_name=well_name,
            well_names=well_names,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
            stage_num=stage_num,
            start_ts=effective_start_ts,
            end_ts=effective_end_ts,
            algorithm_version=algorithm_version,
            concentration_features=concentration_features,
        )
        if historical_pending_after == 0 and workflow_id is not None and historical_cursor_key:
            await set_state(
                workflow_id=workflow_id,
                workspace_id=workspace_id,
                key=historical_cursor_key,
                value=format_dt(effective_end_ts),
            )
            historical_cursor_advanced_to = format_dt(effective_end_ts)
        logger.info(
            "Auto historical window %s pending_after=%s cursor=%s",
            "complete" if historical_pending_after == 0 else "still_pending",
            historical_pending_after,
            historical_cursor_advanced_to or format_dt(effective_start_ts),
        )
    if (
        historical_progress_enabled
        and historical_total_start_ts is not None
        and historical_total_end_ts is not None
    ):
        historical_progress_after = await _get_historical_stage_progress_summary(
            workspace_id=workspace_id,
            stage_index_featurestore_key=stage_index_featurestore_key,
            auto_manifest_featurestore_key=auto_manifest_featurestore_key,
            mode=mode,
            well_name=well_name,
            well_names=well_names,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
            stage_num=stage_num,
            start_ts=historical_total_start_ts,
            end_ts=historical_total_end_ts,
            algorithm_version=algorithm_version,
            concentration_features=concentration_features,
        )
        _log_historical_stage_progress(
            logger,
            label="after",
            progress=historical_progress_after,
            cursor=historical_cursor_advanced_to or format_dt(effective_start_ts),
            total_start_ts=historical_total_start_ts,
            total_end_ts=historical_total_end_ts,
        )

    logger.info("Auto labeling complete results=%s stages=%s concentration_features=%s mode=%s dry_run=%s", len(results), len(stages), ",".join(concentration_features), mode, dry_run)
    return {
        "run_id": run_id,
        "stages_selected": len(stages),
        "results_processed": len(results),
        "mode": mode,
        "dry_run": bool(dry_run),
        "concentration_features": list(concentration_features),
        "recompute_reset": recompute_reset,
        "historical_pending_after": historical_pending_after,
        "historical_cursor_advanced_to": historical_cursor_advanced_to,
        "historical_progress_before": historical_progress_before,
        "historical_progress_after": historical_progress_after,
        "results": results,
    }


@flow(name="nextier-auto-labeling-v1")
async def nextier_auto_labeling_v1_flow(
    workspace_id: int,
    workflow_id: int,
    mode: str = "historical",
    copper_featurestore_key: str = COPPER_LABELS_FEATURESTORE_KEY,
    stage_index_featurestore_key: str = COPPER_STAGE_INDEX_FEATURESTORE_KEY,
    auto_labels_featurestore_key: str = AUTO_LABELS_FEATURESTORE_KEY,
    auto_summary_featurestore_key: str = AUTO_STAGE_SUMMARY_FEATURESTORE_KEY,
    auto_manifest_featurestore_key: str = AUTO_MANIFEST_FEATURESTORE_KEY,
    well_name: str | None = None,
    well_names: list[str] | None = None,
    fleet_name: str | None = None,
    pad_name: str | None = None,
    include_fleet_names: list[str] | None = None,
    exclude_fleet_names: list[str] | None = None,
    stage_num: float | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    lookback_hours: float = 24,
    chunk_hours: float | None = 24,
    max_chunks: int | None = 1,
    max_wells: int = 5,
    max_stages: int = 25,
    skip_completed: bool = True,
    delete_existing: bool = True,
    recompute: bool = False,
    recompute_run_key: str | None = None,
    dry_run: bool = False,
    algorithm_version: str = DEFAULT_AUTO_ALGORITHM_VERSION,
    concentration_feature: str = DEFAULT_CONCENTRATION_FEATURE,
    use_copper_tip_selector: bool = False,
):
    return await run_nextier_auto_labeling_v1(
        workspace_id=workspace_id,
        workflow_id=workflow_id,
        mode=mode,
        copper_featurestore_key=copper_featurestore_key,
        stage_index_featurestore_key=stage_index_featurestore_key,
        auto_labels_featurestore_key=auto_labels_featurestore_key,
        auto_summary_featurestore_key=auto_summary_featurestore_key,
        auto_manifest_featurestore_key=auto_manifest_featurestore_key,
        well_name=well_name,
        well_names=well_names,
        fleet_name=fleet_name,
        pad_name=pad_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
        stage_num=stage_num,
        start_time=start_time,
        end_time=end_time,
        lookback_hours=lookback_hours,
        chunk_hours=chunk_hours,
        max_chunks=max_chunks,
        max_wells=max_wells,
        max_stages=max_stages,
        skip_completed=skip_completed,
        delete_existing=delete_existing,
        recompute=recompute,
        recompute_run_key=recompute_run_key,
        dry_run=dry_run,
        algorithm_version=algorithm_version,
        concentration_feature=concentration_feature,
        use_copper_tip_selector=use_copper_tip_selector,
    )
