from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
import polars as pl
from prefect import get_run_logger

from nixdlt.workflow_sdk.platform_tasks import (
    delete_featurestore_records,
    query_store,
    write_featurestore,
)


SOURCE_DATASTORE_KEY = "merged_fleet_stream_customer_full_v2"
BRONZE_LABELS_FEATURESTORE_KEY = "nextier_bronze_labels_v1"
COPPER_LABELS_FEATURESTORE_KEY = "nextier_copper_labels_v1"
COPPER_STAGE_INDEX_FEATURESTORE_KEY = "nextier_copper_stage_index_v1"
MANIFEST_FEATURESTORE_KEY = "nextier_labeling_processing_manifest_v1"

DEFAULT_ALGORITHM_VERSION = "nextier_bronze_copper_fullwell_v1"

RAW_TELEMETRY_COLUMNS = [
    "created_ts",
    "fleet_name",
    "pad_name",
    "record_ts",
    "name",
    "id",
    "api_num",
    "rate_slurry",
    "prop_conc_target",
    "prop_conc_blend_denso",
    "prop_conc_blend_auger",
    "prop_conc_inline",
    "press_mainline",
]

BRONZE_COLUMNS = [
    "telemetry_point_id",
    "fleet_name",
    "pad_name",
    "well_name",
    "well_id",
    "api_num",
    "record_ts",
    "created_ts",
    "bronze_continuous",
    "rate_slurry",
    "press_mainline",
    "prop_conc_blend_denso",
    "prop_conc_blend_auger",
    "prop_conc_inline",
    "prop_conc_target",
    "algorithm_version",
    "source_mode",
    "processed_at",
]


def format_dt(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.strftime("%Y-%m-%d %H:%M:%S.%f")


def parse_dt(value: Any) -> pd.Timestamp | None:
    if value is None or value == "":
        return None
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts.tz_convert("UTC").tz_localize(None)


def now_utc() -> pd.Timestamp:
    return pd.Timestamp(datetime.utcnow())


def last_non_null(series: pd.Series) -> Any:
    values = series.dropna()
    if values.empty:
        return None
    return values.iloc[-1]


def validate_mode(mode: str) -> str:
    normalized = (mode or "background").strip().lower()
    if normalized not in {"live", "background", "historical", "debug"}:
        raise ValueError("mode must be one of: live, background, historical, debug")
    return normalized


def _normalize_name_list(values: list[str] | None) -> list[str]:
    if not values:
        return []
    return [str(value).strip() for value in values if str(value).strip()]


def normalize_name_list(values: list[str] | None) -> list[str]:
    return _normalize_name_list(values)


def combine_well_names(well_name: str | None, well_names: list[str] | None) -> list[str]:
    combined: list[str] = []
    if well_name and str(well_name).strip():
        combined.append(str(well_name).strip())
    combined.extend(_normalize_name_list(well_names))
    return list(dict.fromkeys(combined))


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


def apply_well_name_filters(
    conditions: list[str],
    params: dict[str, Any],
    field_sql: str,
    *,
    well_name: str | None = None,
    well_names: list[str] | None = None,
) -> list[str]:
    names = combine_well_names(well_name, well_names)
    if len(names) == 1:
        conditions.append(f"{field_sql} = :well_name")
        params["well_name"] = names[0]
    elif len(names) > 1:
        _add_named_list_filter(conditions, params, field_sql, names, "well_name")
    return names


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


def resolve_window(
    mode: str,
    start_time: str | None,
    end_time: str | None,
    lookback_hours: float,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    start_ts = parse_dt(start_time)
    end_ts = parse_dt(end_time)

    if end_ts is None and mode in {"live", "background", "debug"}:
        end_ts = now_utc()
    if start_ts is None and end_ts is not None and float(lookback_hours or 0) > 0:
        start_ts = end_ts - timedelta(hours=float(lookback_hours))
    if start_ts is not None and end_ts is not None and end_ts < start_ts:
        start_ts, end_ts = end_ts, start_ts
    return start_ts, end_ts


async def get_raw_scope_bounds(
    workspace_id: int,
    source_datastore_key: str,
    well_name: str | None = None,
    well_names: list[str] | None = None,
    fleet_name: str | None = None,
    pad_name: str | None = None,
    include_fleet_names: list[str] | None = None,
    exclude_fleet_names: list[str] | None = None,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    conditions = ["record_ts IS NOT NULL", "name IS NOT NULL"]
    params: dict[str, Any] = {}
    apply_well_name_filters(conditions, params, "name", well_name=well_name, well_names=well_names)
    apply_fleet_filters(
        conditions,
        params,
        "fleet_name",
        fleet_name=fleet_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
    )
    if pad_name and str(pad_name).strip():
        conditions.append("pad_name = :pad_name")
        params["pad_name"] = str(pad_name).strip()

    bounds = await query_store(
        sql=f"""
            SELECT MIN(record_ts) AS start_ts, MAX(record_ts) AS end_ts
            FROM datastore:{source_datastore_key}
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

async def get_well_index_scope_bounds(
    workspace_id: int,
    source_datastore_key: str,
    well_name: str | None = None,
    well_names: list[str] | None = None,
    fleet_name: str | None = None,
    pad_name: str | None = None,
    include_fleet_names: list[str] | None = None,
    exclude_fleet_names: list[str] | None = None,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """Return historical source bounds from the well index, avoiding raw telemetry scans."""
    well_index_key = well_index_featurestore_for_source(source_datastore_key)
    conditions = [
        "w.name IS NOT NULL",
        "w.first_record_ts IS NOT NULL",
        "w.last_record_ts IS NOT NULL",
    ]
    params: dict[str, Any] = {}
    apply_well_name_filters(conditions, params, "w.name", well_name=well_name, well_names=well_names)
    apply_fleet_filters(
        conditions,
        params,
        "w.fleet_name",
        fleet_name=fleet_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
    )
    if pad_name and str(pad_name).strip():
        conditions.append("w.pad_name = :pad_name")
        params["pad_name"] = str(pad_name).strip()

    bounds = await query_store(
        sql=f"""
            SELECT
                MIN(CAST(w.first_record_ts AS TIMESTAMP)) AS start_ts,
                MAX(CAST(w.last_record_ts AS TIMESTAMP)) AS end_ts
            FROM featurestore:{well_index_key} w
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


def build_time_chunks(
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    chunk_hours: float | None,
    max_chunks: int | None,
) -> list[tuple[int, pd.Timestamp | None, pd.Timestamp | None]]:
    if start_ts is None or end_ts is None:
        return [(0, start_ts, end_ts)]
    if end_ts <= start_ts:
        return []
    hours = float(chunk_hours or 0)
    if hours <= 0:
        return [(0, start_ts, end_ts)]

    chunks: list[tuple[int, pd.Timestamp | None, pd.Timestamp | None]] = []
    cursor = start_ts
    index = 0
    cap = int(max_chunks) if max_chunks is not None and int(max_chunks) > 0 else None
    while cursor < end_ts and (cap is None or len(chunks) < cap):
        next_ts = min(cursor + pd.Timedelta(hours=hours), end_ts)
        chunks.append((index, cursor, next_ts))
        cursor = next_ts
        index += 1
    return chunks


def _historical_scope_marker_value(value: str | None) -> str:
    if value is None or not str(value).strip():
        return "__ALL__"
    return str(value).strip()


async def _write_historical_chunk_exhausted_marker(
    *,
    workspace_id: int,
    manifest_featurestore_key: str,
    source_datastore_key: str,
    well_name: str | None,
    fleet_name: str | None,
    pad_name: str | None,
    chunk_start_ts: pd.Timestamp,
    chunk_end_ts: pd.Timestamp,
    chunk_hours: float,
    chunk_index: int,
    algorithm_version: str,
) -> None:
    completed_at = format_dt(now_utc())
    marker_well = _historical_scope_marker_value(well_name)
    manifest = {
        "manifest_id": f"nextier_labeling_orchestration_v1:{marker_well}:chunk_exhausted:{chunk_index}:{uuid4()}",
        "run_id": "planner",
        "workflow_name": "nextier_labeling_orchestration_v1",
        "mode": "historical",
        "fleet_name": str(fleet_name).strip() if fleet_name and str(fleet_name).strip() else None,
        "pad_name": str(pad_name).strip() if pad_name and str(pad_name).strip() else None,
        "well_name": marker_well,
        "requested_start_ts": format_dt(chunk_start_ts),
        "requested_end_ts": format_dt(chunk_end_ts),
        "effective_start_ts": None,
        "effective_end_ts": format_dt(chunk_end_ts),
        "lookback_hours": 0.0,
        "chunk_hours": float(chunk_hours),
        "chunk_index": float(chunk_index),
        "source_datastore_key": source_datastore_key,
        "source_row_count": 0.0,
        "source_min_record_ts": None,
        "source_max_record_ts": None,
        "bronze_status": "not_run",
        "copper_status": "not_run",
        "stage_index_status": "not_run",
        "rows_bronze_written": 0.0,
        "rows_copper_written": 0.0,
        "stage_count_written": 0.0,
        "algorithm_version": algorithm_version,
        "dry_run": False,
        "status": "chunk_exhausted",
        "error_message": None,
        "started_at": completed_at,
        "completed_at": completed_at,
    }
    await write_manifest(workspace_id, manifest, manifest_featurestore_key)


async def _select_historical_planner_start_chunk(
    *,
    workspace_id: int,
    manifest_featurestore_key: str,
    well_name: str | None,
    fleet_name: str | None,
    pad_name: str | None,
    chunk_hours: float,
    algorithm_version: str,
) -> int:
    params: dict[str, Any] = {
        "mode": "historical",
        "chunk_hours": float(chunk_hours),
        "algorithm_version": algorithm_version,
        "marker_workflow_name": "nextier_labeling_orchestration_v1",
        "bronze_workflow_name": "nextier_bronze_labeling_v1",
        "copper_workflow_name": "nextier_copper_labeling_v1",
        "marker_well_name": _historical_scope_marker_value(well_name),
    }
    marker_conditions = [
        "m.workflow_name = :marker_workflow_name",
        "m.mode = :mode",
        "m.status = 'chunk_exhausted'",
        "m.algorithm_version = :algorithm_version",
        "COALESCE(m.dry_run, false) = false",
        "CAST(m.chunk_hours AS DOUBLE PRECISION) = :chunk_hours",
        "m.well_name = :marker_well_name",
    ]
    use_completed_cursor = bool(well_name and str(well_name).strip())
    completed_conditions = [
        "(m.workflow_name = :bronze_workflow_name OR m.workflow_name = :copper_workflow_name)",
        "m.mode = :mode",
        "m.status = 'written'",
        "m.algorithm_version = :algorithm_version",
        "COALESCE(m.dry_run, false) = false",
        "CAST(m.chunk_hours AS DOUBLE PRECISION) = :chunk_hours",
    ]
    if fleet_name and str(fleet_name).strip():
        marker_conditions.append("m.fleet_name = :fleet_name")
        completed_conditions.append("m.fleet_name = :fleet_name")
        params["fleet_name"] = str(fleet_name).strip()
    else:
        marker_conditions.append("m.fleet_name IS NULL")
    if pad_name and str(pad_name).strip():
        marker_conditions.append("m.pad_name = :pad_name")
        completed_conditions.append("m.pad_name = :pad_name")
        params["pad_name"] = str(pad_name).strip()
    else:
        marker_conditions.append("m.pad_name IS NULL")
    if well_name and str(well_name).strip():
        completed_conditions.append("m.well_name = :well_name")
        params["well_name"] = str(well_name).strip()

    completed_cursor_sql = "(SELECT max_completed_chunk FROM completed WHERE max_completed_chunk IS NOT NULL),\n                    " if use_completed_cursor else ""

    cursor_df = await query_store(
        sql=f"""
            WITH exhausted AS (
                SELECT MAX(CAST(m.chunk_index AS BIGINT)) AS max_exhausted_chunk
                FROM featurestore:{manifest_featurestore_key} m
                WHERE {" AND ".join(marker_conditions)}
            ),
            completed AS (
                SELECT MAX(CAST(m.chunk_index AS BIGINT)) AS max_completed_chunk
                FROM featurestore:{manifest_featurestore_key} m
                WHERE {" AND ".join(completed_conditions)}
            )
            SELECT
                COALESCE(
                    (SELECT max_exhausted_chunk + 1 FROM exhausted WHERE max_exhausted_chunk IS NOT NULL),
                    {completed_cursor_sql}
                    0
                ) AS start_chunk
        """,
        workspace_id=workspace_id,
        params=params,
    )
    if cursor_df.is_empty():
        return 0
    value = cursor_df.to_pandas().iloc[0].get("start_chunk")
    if value is None or pd.isna(value):
        return 0
    return max(0, int(value))


def well_index_featurestore_for_source(source_datastore_key: str) -> str:
    source_key = (source_datastore_key or "").strip()
    if source_key == "live_fleet_stream_customer_full_v1":
        return "live_well_index_v1"
    if source_key == "historian_telemetry_v1":
        return "well_index_v1"
    return "merged_well_index_v1"


async def select_historical_pending_work(
    *,
    workspace_id: int,
    source_datastore_key: str,
    manifest_featurestore_key: str,
    well_name: str | None,
    well_names: list[str] | None = None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    chunk_hours: float,
    max_chunks: int | None,
    max_wells: int,
    algorithm_version: str = DEFAULT_ALGORITHM_VERSION,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Select historical work from well-index bounds and manifest progress.

    This planner deliberately avoids raw telemetry scans. The well index gives
    each well's available time range, and the manifest gives the latest covered
    bronze/copper chunk. Python then expands the next pending chunks for the
    selected wells.
    """
    del dry_run
    start_ts = parse_dt(start_ts)
    end_ts = parse_dt(end_ts)
    if start_ts is None or end_ts is None:
        return []

    chunk_seconds = int(float(chunk_hours) * 3600)
    if chunk_seconds <= 0:
        return []

    chunks_per_well = int(max_chunks) if max_chunks is not None and int(max_chunks) > 0 else 1000000
    well_limit = max(1, int(max_wells or 1))
    total_chunks = int(np.ceil((end_ts - start_ts).total_seconds() / chunk_seconds))
    if total_chunks <= 0:
        return []

    well_index_key = well_index_featurestore_for_source(source_datastore_key)
    conditions = [
        "w.name IS NOT NULL",
        "w.first_record_ts IS NOT NULL",
        "w.last_record_ts IS NOT NULL",
        "CAST(w.last_record_ts AS TIMESTAMP) >= :start_time",
        "CAST(w.first_record_ts AS TIMESTAMP) < :end_time",
    ]
    params: dict[str, Any] = {
        "start_time": format_dt(start_ts),
        "end_time": format_dt(end_ts),
        "chunk_hours": float(chunk_hours),
        "mode": "historical",
        "algorithm_version": algorithm_version,
        "bronze_workflow_name": "nextier_bronze_labeling_v1",
        "copper_workflow_name": "nextier_copper_labeling_v1",
    }
    targeted_wells = apply_well_name_filters(conditions, params, "w.name", well_name=well_name, well_names=well_names)
    apply_fleet_filters(
        conditions,
        params,
        "w.fleet_name",
        fleet_name=fleet_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
    )
    if pad_name and str(pad_name).strip():
        conditions.append("w.pad_name = :pad_name")
        params["pad_name"] = str(pad_name).strip()

    progress_df = await query_store(
        sql=f"""
            WITH indexed_wells AS (
                SELECT
                    w.name AS well_name,
                    CAST(w.first_record_ts AS TIMESTAMP) AS first_ts,
                    CAST(w.last_record_ts AS TIMESTAMP) AS last_ts
                FROM featurestore:{well_index_key} w
                WHERE {" AND ".join(conditions)}
            ),
            manifest_progress AS (
                SELECT
                    m.well_name,
                    MAX(
                        CASE
                            WHEN m.workflow_name = :bronze_workflow_name
                            THEN CAST(
                                FLOOR(
                                    EXTRACT(EPOCH FROM (CAST(m.requested_end_ts AS TIMESTAMP) - CAST(:start_time AS TIMESTAMP)))
                                    / (:chunk_hours * 3600.0)
                                    - 0.000001
                                ) AS BIGINT
                            )
                            ELSE NULL
                        END
                    ) AS bronze_max_chunk,
                    MAX(
                        CASE
                            WHEN m.workflow_name = :copper_workflow_name
                            THEN CAST(
                                FLOOR(
                                    EXTRACT(EPOCH FROM (CAST(m.requested_end_ts AS TIMESTAMP) - CAST(:start_time AS TIMESTAMP)))
                                    / (:chunk_hours * 3600.0)
                                    - 0.000001
                                ) AS BIGINT
                            )
                            ELSE NULL
                        END
                    ) AS copper_max_chunk
                FROM featurestore:{manifest_featurestore_key} m
                WHERE (m.workflow_name = :bronze_workflow_name OR m.workflow_name = :copper_workflow_name)
                  AND m.mode = :mode
                  AND m.algorithm_version = :algorithm_version
                  AND m.status = 'written'
                  AND COALESCE(m.dry_run, false) = false
                  AND CAST(m.chunk_hours AS DOUBLE PRECISION) = :chunk_hours
                  AND m.chunk_index IS NOT NULL
                  AND m.requested_start_ts IS NOT NULL
                  AND m.requested_end_ts IS NOT NULL
                  AND CAST(m.requested_start_ts AS TIMESTAMP) < :end_time
                  AND CAST(m.requested_end_ts AS TIMESTAMP) > :start_time
                GROUP BY m.well_name
            )
            SELECT
                iw.well_name,
                iw.first_ts,
                iw.last_ts,
                mp.bronze_max_chunk,
                mp.copper_max_chunk
            FROM indexed_wells iw
            LEFT JOIN manifest_progress mp ON mp.well_name = iw.well_name
            ORDER BY iw.first_ts ASC, iw.well_name ASC
        """,
        workspace_id=workspace_id,
        params=params,
    )
    if targeted_wells:
        present_wells: set[str] = set()
        if not progress_df.is_empty():
            present_wells = {str(name) for name in progress_df["well_name"].drop_nulls().to_list()}
        missing_targeted_wells = [name for name in targeted_wells if name not in present_wells]
    else:
        missing_targeted_wells = []

    if missing_targeted_wells:
        raw_conditions = [
            "name IS NOT NULL",
            "record_ts IS NOT NULL",
            "record_ts >= :start_time",
            "record_ts < :end_time",
        ]
        raw_params = {
            "start_time": params["start_time"],
            "end_time": params["end_time"],
            "chunk_hours": params["chunk_hours"],
            "mode": params["mode"],
            "algorithm_version": params["algorithm_version"],
            "bronze_workflow_name": params["bronze_workflow_name"],
            "copper_workflow_name": params["copper_workflow_name"],
        }
        apply_well_name_filters(raw_conditions, raw_params, "name", well_names=missing_targeted_wells)
        apply_fleet_filters(
            raw_conditions,
            raw_params,
            "fleet_name",
            fleet_name=fleet_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
        )
        if pad_name and str(pad_name).strip():
            raw_conditions.append("pad_name = :pad_name")
            raw_params["pad_name"] = str(pad_name).strip()
        raw_progress_df = await query_store(
            sql=f"""
                WITH indexed_wells AS (
                    SELECT
                        name AS well_name,
                        MIN(record_ts) AS first_ts,
                        MAX(record_ts) AS last_ts
                    FROM datastore:{source_datastore_key}
                    WHERE {" AND ".join(raw_conditions)}
                    GROUP BY name
                ),
                manifest_progress AS (
                    SELECT
                        m.well_name,
                        MAX(
                            CASE
                                WHEN m.workflow_name = :bronze_workflow_name
                                THEN CAST(
                                    FLOOR(
                                        EXTRACT(EPOCH FROM (CAST(m.requested_end_ts AS TIMESTAMP) - CAST(:start_time AS TIMESTAMP)))
                                        / (:chunk_hours * 3600.0)
                                        - 0.000001
                                    ) AS BIGINT
                                )
                                ELSE NULL
                            END
                        ) AS bronze_max_chunk,
                        MAX(
                            CASE
                                WHEN m.workflow_name = :copper_workflow_name
                                THEN CAST(
                                    FLOOR(
                                        EXTRACT(EPOCH FROM (CAST(m.requested_end_ts AS TIMESTAMP) - CAST(:start_time AS TIMESTAMP)))
                                        / (:chunk_hours * 3600.0)
                                        - 0.000001
                                    ) AS BIGINT
                                )
                                ELSE NULL
                            END
                        ) AS copper_max_chunk
                    FROM featurestore:{manifest_featurestore_key} m
                    WHERE (m.workflow_name = :bronze_workflow_name OR m.workflow_name = :copper_workflow_name)
                      AND m.mode = :mode
                      AND m.algorithm_version = :algorithm_version
                      AND m.status = 'written'
                      AND COALESCE(m.dry_run, false) = false
                      AND CAST(m.chunk_hours AS DOUBLE PRECISION) = :chunk_hours
                      AND m.chunk_index IS NOT NULL
                      AND m.requested_start_ts IS NOT NULL
                      AND m.requested_end_ts IS NOT NULL
                      AND CAST(m.requested_start_ts AS TIMESTAMP) < :end_time
                      AND CAST(m.requested_end_ts AS TIMESTAMP) > :start_time
                    GROUP BY m.well_name
                )
                SELECT
                    iw.well_name,
                    iw.first_ts,
                    iw.last_ts,
                    mp.bronze_max_chunk,
                    mp.copper_max_chunk
                FROM indexed_wells iw
                LEFT JOIN manifest_progress mp ON mp.well_name = iw.well_name
                ORDER BY iw.first_ts ASC, iw.well_name ASC
            """,
            workspace_id=workspace_id,
            params=raw_params,
        )
        if progress_df.is_empty():
            progress_df = raw_progress_df
        elif not raw_progress_df.is_empty():
            progress_df = pl.concat([progress_df, raw_progress_df], how="diagonal")
    if progress_df.is_empty():
        return []

    logger = get_run_logger()
    candidates: list[dict[str, Any]] = []
    progress = progress_df.to_pandas()
    for row in progress.to_dict("records"):
        first_ts = parse_dt(row.get("first_ts"))
        last_ts = parse_dt(row.get("last_ts"))
        if first_ts is None or last_ts is None:
            continue

        first_chunk = max(0, int(np.floor((first_ts - start_ts).total_seconds() / chunk_seconds)))
        last_chunk = min(
            total_chunks - 1,
            int(np.floor((last_ts - start_ts).total_seconds() / chunk_seconds)),
        )
        if last_chunk < first_chunk:
            continue

        bronze_max = row.get("bronze_max_chunk")
        copper_max = row.get("copper_max_chunk")
        bronze_next = int(bronze_max) + 1 if pd.notna(bronze_max) else first_chunk
        copper_next = int(copper_max) + 1 if pd.notna(copper_max) else first_chunk
        next_chunk = max(first_chunk, min(bronze_next, copper_next))
        logger.info(
            "Historical planner well=%s first_ts=%s last_ts=%s first_chunk=%s last_chunk=%s bronze_max_chunk=%s copper_max_chunk=%s next_chunk=%s",
            row.get("well_name"),
            format_dt(first_ts),
            format_dt(last_ts),
            first_chunk,
            last_chunk,
            None if pd.isna(bronze_max) else int(bronze_max),
            None if pd.isna(copper_max) else int(copper_max),
            next_chunk,
        )
        if next_chunk <= last_chunk:
            candidates.append(
                {
                    "well_name": str(row["well_name"]),
                    "first_ts": first_ts,
                    "next_chunk": next_chunk,
                    "last_chunk": last_chunk,
                }
            )

    if not candidates:
        return []

    selected = sorted(
        candidates,
        key=lambda item: (int(item["next_chunk"]), item["first_ts"], str(item["well_name"])),
    )[:well_limit]

    rows: list[dict[str, Any]] = []
    for item in selected:
        next_chunk = int(item["next_chunk"])
        last_chunk = int(item["last_chunk"])
        chunk_count = min(chunks_per_well, last_chunk - next_chunk + 1)
        selected_chunks = [next_chunk + offset for offset in range(chunk_count)]
        logger.info(
            "Historical planner selected well=%s chunks=%s",
            item["well_name"],
            selected_chunks,
        )
        for offset in range(chunk_count):
            chunk_index = next_chunk + offset
            chunk_start_ts = start_ts + pd.Timedelta(seconds=chunk_index * chunk_seconds)
            chunk_end_ts = min(
                start_ts + pd.Timedelta(seconds=(chunk_index + 1) * chunk_seconds),
                end_ts,
            )
            rows.append(
                {
                    "well_name": str(item["well_name"]),
                    "chunk_index": chunk_index,
                    "chunk_start_ts": chunk_start_ts,
                    "chunk_end_ts": chunk_end_ts,
                }
            )
    return rows


async def get_historical_chunk_manifest_status(
    *,
    workspace_id: int,
    manifest_featurestore_key: str,
    well_name: str,
    chunk_start_ts: pd.Timestamp,
    chunk_end_ts: pd.Timestamp,
    chunk_hours: float,
    chunk_index: int,
    algorithm_version: str = DEFAULT_ALGORITHM_VERSION,
) -> dict[str, bool]:
    """Return whether bronze/copper already wrote a finite historical chunk."""
    status_df = await query_store(
        sql=f"""
            SELECT m.workflow_name, COUNT(*) AS written_rows
            FROM featurestore:{manifest_featurestore_key} m
            WHERE (m.workflow_name = :bronze_workflow_name OR m.workflow_name = :copper_workflow_name)
              AND m.mode = :mode
              AND m.well_name = :well_name
              AND m.algorithm_version = :algorithm_version
              AND m.status = 'written'
              AND COALESCE(m.dry_run, false) = false
              AND CAST(m.chunk_hours AS DOUBLE PRECISION) = :chunk_hours
              AND CAST(m.chunk_index AS BIGINT) = :chunk_index
              AND m.requested_start_ts = :requested_start_ts
              AND m.requested_end_ts = :requested_end_ts
            GROUP BY m.workflow_name
        """,
        workspace_id=workspace_id,
        params={
            "bronze_workflow_name": "nextier_bronze_labeling_v1",
            "copper_workflow_name": "nextier_copper_labeling_v1",
            "mode": "historical",
            "well_name": str(well_name).strip(),
            "algorithm_version": algorithm_version,
            "chunk_hours": float(chunk_hours),
            "chunk_index": int(chunk_index),
            "requested_start_ts": format_dt(chunk_start_ts),
            "requested_end_ts": format_dt(chunk_end_ts),
        },
    )
    status = {
        "bronze": False,
        "copper": False,
    }
    if status_df.is_empty():
        return status

    for row in status_df.to_pandas().to_dict("records"):
        workflow_name = row.get("workflow_name")
        written_rows = int(row.get("written_rows") or 0)
        if workflow_name == "nextier_bronze_labeling_v1" and written_rows > 0:
            status["bronze"] = True
        elif workflow_name == "nextier_copper_labeling_v1" and written_rows > 0:
            status["copper"] = True
    return status


def _append_manifest_reset_scope_filters(
    filters: list[dict[str, Any]],
    *,
    well_name: str | None,
    fleet_name: str | None,
    pad_name: str | None,
) -> None:
    if well_name and str(well_name).strip():
        filters.append({"field": "well_name", "op": "eq", "value": str(well_name).strip()})
    if fleet_name and str(fleet_name).strip():
        filters.append({"field": "fleet_name", "op": "eq", "value": str(fleet_name).strip()})
    if pad_name and str(pad_name).strip():
        filters.append({"field": "pad_name", "op": "eq", "value": str(pad_name).strip()})


async def reset_historical_labeling_manifest_once(
    *,
    workspace_id: int,
    manifest_featurestore_key: str,
    source_datastore_key: str,
    recompute_run_key: str,
    well_name: str | None,
    well_names: list[str] | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    chunk_hours: float,
    algorithm_version: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Clear historical bronze/copper manifest progress once for a recompute campaign."""
    key = str(recompute_run_key or "").strip()
    if not key:
        raise ValueError("recompute_run_key is required when recompute=true")
    if dry_run:
        return {"reset": False, "reason": "dry_run", "deleted": {}}

    targeted_wells = combine_well_names(well_name, well_names)
    include_fleets = normalize_name_list(include_fleet_names)
    exclude_fleets = normalize_name_list(exclude_fleet_names)
    if exclude_fleets and not targeted_wells:
        raise ValueError("recompute with exclude_fleet_names requires explicit well_name or well_names")
    marker_well = ",".join(targeted_wells) if targeted_wells else _historical_scope_marker_value(well_name)

    base_marker_params: dict[str, Any] = {
        "workflow_name": "nextier_labeling_orchestration_v1",
        "mode": "historical",
        "status": "recompute_started",
        "run_id": key,
        "algorithm_version": algorithm_version,
        "chunk_hours": float(chunk_hours),
        "requested_start_ts": format_dt(start_ts),
        "requested_end_ts": format_dt(end_ts),
    }
    marker_conditions = [
        "m.workflow_name = :workflow_name",
        "m.mode = :mode",
        "m.status = :status",
        "m.run_id = :run_id",
        "m.algorithm_version = :algorithm_version",
        "CAST(m.chunk_hours AS DOUBLE PRECISION) = :chunk_hours",
        "m.requested_start_ts = :requested_start_ts",
        "m.requested_end_ts = :requested_end_ts",
    ]
    if marker_well:
        marker_conditions.append("m.well_name = :marker_well")
        base_marker_params["marker_well"] = marker_well
    if fleet_name and str(fleet_name).strip():
        marker_conditions.append("m.fleet_name = :fleet_name")
        base_marker_params["fleet_name"] = str(fleet_name).strip()
    elif include_fleets:
        marker_conditions.append("m.fleet_name = :include_fleet_marker")
        base_marker_params["include_fleet_marker"] = ",".join(include_fleets)
    if pad_name and str(pad_name).strip():
        marker_conditions.append("m.pad_name = :pad_name")
        base_marker_params["pad_name"] = str(pad_name).strip()

    marker_df = await query_store(
        sql=f"""
            SELECT COUNT(*) AS marker_count
            FROM featurestore:{manifest_featurestore_key} m
            WHERE {" AND ".join(marker_conditions)}
        """,
        workspace_id=workspace_id,
        params=base_marker_params,
    )
    if not marker_df.is_empty():
        marker_count = int(marker_df.to_pandas().iloc[0].get("marker_count") or 0)
        if marker_count > 0:
            return {"reset": False, "reason": "marker_exists", "deleted": {}}

    workflow_names = ["nextier_bronze_labeling_v1", "nextier_copper_labeling_v1"]
    scopes: list[dict[str, str | None]] = []
    if targeted_wells:
        scopes = [{"well_name": name, "fleet_name": None, "pad_name": None} for name in targeted_wells]
    elif fleet_name and str(fleet_name).strip():
        scopes = [{"well_name": None, "fleet_name": str(fleet_name).strip(), "pad_name": pad_name}]
    elif include_fleets:
        scopes = [{"well_name": None, "fleet_name": name, "pad_name": pad_name} for name in include_fleets]
    else:
        scopes = [{"well_name": None, "fleet_name": None, "pad_name": pad_name}]

    deleted: dict[str, int | None] = {}
    for workflow_name in workflow_names:
        total_deleted = 0
        for scope in scopes:
            filters = [
                {"field": "workflow_name", "op": "eq", "value": workflow_name},
                {"field": "mode", "op": "eq", "value": "historical"},
                {"field": "algorithm_version", "op": "eq", "value": algorithm_version},
                {"field": "chunk_hours", "op": "eq", "value": float(chunk_hours)},
                {"field": "requested_start_ts", "op": "lt", "value": format_dt(end_ts)},
                {"field": "requested_end_ts", "op": "gt", "value": format_dt(start_ts)},
            ]
            _append_manifest_reset_scope_filters(
                filters,
                well_name=scope.get("well_name"),
                fleet_name=scope.get("fleet_name"),
                pad_name=scope.get("pad_name"),
            )
            count = await delete_featurestore_records(
                featurestore_key=manifest_featurestore_key,
                workspace_id=workspace_id,
                filters=filters,
                require_primary_key_filter=False,
            )
            if count is not None:
                total_deleted += int(count)
        deleted[workflow_name] = total_deleted

    completed_at = format_dt(now_utc())
    marker = {
        "manifest_id": f"nextier_labeling_orchestration_v1:{key}:recompute:{uuid4()}",
        "run_id": key,
        "workflow_name": "nextier_labeling_orchestration_v1",
        "mode": "historical",
        "fleet_name": str(fleet_name).strip() if fleet_name and str(fleet_name).strip() else (",".join(include_fleets) if include_fleets else None),
        "pad_name": str(pad_name).strip() if pad_name and str(pad_name).strip() else None,
        "well_name": marker_well,
        "requested_start_ts": format_dt(start_ts),
        "requested_end_ts": format_dt(end_ts),
        "effective_start_ts": format_dt(start_ts),
        "effective_end_ts": format_dt(end_ts),
        "lookback_hours": 0.0,
        "chunk_hours": float(chunk_hours),
        "chunk_index": None,
        "source_datastore_key": source_datastore_key,
        "source_row_count": 0.0,
        "source_min_record_ts": None,
        "source_max_record_ts": None,
        "bronze_status": "not_run",
        "copper_status": "not_run",
        "stage_index_status": "not_run",
        "rows_bronze_written": 0.0,
        "rows_copper_written": 0.0,
        "stage_count_written": 0.0,
        "algorithm_version": algorithm_version,
        "dry_run": False,
        "status": "recompute_started",
        "error_message": "manifest reset marker",
        "started_at": completed_at,
        "completed_at": completed_at,
    }
    await write_manifest(workspace_id, marker, manifest_featurestore_key)
    return {"reset": True, "reason": "marker_created", "deleted": deleted}


async def get_raw_well_bounds(
    workspace_id: int,
    source_datastore_key: str,
    well_name: str,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    return await get_raw_scope_bounds(
        workspace_id=workspace_id,
        source_datastore_key=source_datastore_key,
        well_name=well_name,
    )


async def select_raw_wells(
    workspace_id: int,
    source_datastore_key: str,
    manifest_featurestore_key: str,
    workflow_name: str,
    mode: str,
    well_name: str | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    max_wells: int,
    skip_completed: bool = False,
    algorithm_version: str = DEFAULT_ALGORITHM_VERSION,
) -> list[str]:
    conditions = ["t.name IS NOT NULL", "t.record_ts IS NOT NULL"]
    params: dict[str, Any] = {}
    if well_name and str(well_name).strip():
        conditions.append("t.name = :well_name")
        params["well_name"] = str(well_name).strip()
    apply_fleet_filters(
        conditions,
        params,
        "t.fleet_name",
        fleet_name=fleet_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
    )
    if pad_name and str(pad_name).strip():
        conditions.append("t.pad_name = :pad_name")
        params["pad_name"] = str(pad_name).strip()
    selection_time_field = "t.created_ts" if mode == "live" else "t.record_ts"
    if start_ts is not None:
        conditions.append(f"{selection_time_field} >= :start_time")
        params["start_time"] = format_dt(start_ts)
    if end_ts is not None:
        conditions.append(f"{selection_time_field} < :end_time")
        params["end_time"] = format_dt(end_ts)

    exclusion = ""
    if skip_completed:
        window_filter = ""
        if start_ts is not None and end_ts is not None:
            window_filter = """
                  AND m.requested_start_ts = :manifest_start_time
                  AND m.requested_end_ts = :manifest_end_time
            """
            params["manifest_start_time"] = format_dt(start_ts)
            params["manifest_end_time"] = format_dt(end_ts)
        exclusion = f"""
            AND NOT EXISTS (
                SELECT 1
                FROM featurestore:{manifest_featurestore_key} m
                WHERE m.workflow_name = :workflow_name
                  AND m.mode = :mode
                  AND m.well_name = t.name
                  AND m.algorithm_version = :algorithm_version
                  AND m.status = 'written'
                  AND COALESCE(m.dry_run, false) = false
                  {window_filter}
            )
        """
        params.update(
            {
                "workflow_name": workflow_name,
                "mode": mode,
                "algorithm_version": algorithm_version,
            }
        )

    fairness_enabled = not skip_completed and mode in {"live", "background"}
    if fairness_enabled:
        params.update(
            {
                "workflow_name": workflow_name,
                "mode": mode,
                "algorithm_version": algorithm_version,
            }
        )
        sql = f"""
            WITH eligible AS (
                SELECT
                    t.name,
                    MIN({selection_time_field}) AS first_ts,
                    MAX({selection_time_field}) AS latest_ts
                FROM datastore:{source_datastore_key} t
                WHERE {" AND ".join(conditions)}
                GROUP BY t.name
            ),
            manifest_last AS (
                SELECT
                    m.well_name,
                    MAX(CAST(COALESCE(m.completed_at, m.started_at) AS TIMESTAMP)) AS last_processed_at
                FROM featurestore:{manifest_featurestore_key} m
                WHERE m.workflow_name = :workflow_name
                  AND m.mode = :mode
                  AND m.algorithm_version = :algorithm_version
                  AND COALESCE(m.dry_run, false) = false
                GROUP BY m.well_name
            )
            SELECT e.name, e.first_ts
            FROM eligible e
            LEFT JOIN manifest_last ml ON ml.well_name = e.name
            ORDER BY ml.last_processed_at ASC NULLS FIRST, e.latest_ts DESC, e.name ASC
            LIMIT {int(max_wells)}
        """
    else:
        sql = f"""
            SELECT t.name, MIN({selection_time_field}) AS first_ts
            FROM datastore:{source_datastore_key} t
            WHERE {" AND ".join(conditions)}
              {exclusion}
            GROUP BY t.name
            ORDER BY first_ts ASC, t.name ASC
            LIMIT {int(max_wells)}
        """

    wells_df = await query_store(
        sql=sql,
        workspace_id=workspace_id,
        params=params,
    )
    if wells_df.is_empty():
        return []
    return [str(name) for name in wells_df["name"].drop_nulls().to_list()]


async def select_bronze_wells(
    workspace_id: int,
    bronze_featurestore_key: str,
    manifest_featurestore_key: str,
    workflow_name: str,
    mode: str,
    well_name: str | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    max_wells: int,
    skip_completed: bool = False,
    algorithm_version: str = DEFAULT_ALGORITHM_VERSION,
) -> list[str]:
    conditions = ["b.well_name IS NOT NULL", "b.record_ts IS NOT NULL"]
    params: dict[str, Any] = {}
    if well_name and str(well_name).strip():
        conditions.append("b.well_name = :well_name")
        params["well_name"] = str(well_name).strip()
    apply_fleet_filters(
        conditions,
        params,
        "b.fleet_name",
        fleet_name=fleet_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
    )
    if pad_name and str(pad_name).strip():
        conditions.append("b.pad_name = :pad_name")
        params["pad_name"] = str(pad_name).strip()
    selection_time_field = "b.created_ts" if mode == "live" else "b.record_ts"
    if start_ts is not None:
        conditions.append(f"{selection_time_field} >= :start_time")
        params["start_time"] = format_dt(start_ts)
    if end_ts is not None:
        conditions.append(f"{selection_time_field} < :end_time")
        params["end_time"] = format_dt(end_ts)

    exclusion = ""
    if skip_completed:
        window_filter = ""
        if start_ts is not None and end_ts is not None:
            window_filter = """
                  AND m.requested_start_ts = :manifest_start_time
                  AND m.requested_end_ts = :manifest_end_time
            """
            params["manifest_start_time"] = format_dt(start_ts)
            params["manifest_end_time"] = format_dt(end_ts)
        exclusion = f"""
            AND NOT EXISTS (
                SELECT 1
                FROM featurestore:{manifest_featurestore_key} m
                WHERE m.workflow_name = :workflow_name
                  AND m.mode = :mode
                  AND m.well_name = b.well_name
                  AND m.algorithm_version = :algorithm_version
                  AND m.status = 'written'
                  AND COALESCE(m.dry_run, false) = false
                  {window_filter}
            )
        """
        params.update(
            {
                "workflow_name": workflow_name,
                "mode": mode,
                "algorithm_version": algorithm_version,
            }
        )

    fairness_enabled = not skip_completed and mode in {"live", "background"}
    if fairness_enabled:
        params.update(
            {
                "workflow_name": workflow_name,
                "mode": mode,
                "algorithm_version": algorithm_version,
            }
        )
        sql = f"""
            WITH eligible AS (
                SELECT
                    b.well_name,
                    MIN({selection_time_field}) AS first_ts,
                    MAX({selection_time_field}) AS latest_ts
                FROM featurestore:{bronze_featurestore_key} b
                WHERE {" AND ".join(conditions)}
                GROUP BY b.well_name
            ),
            manifest_last AS (
                SELECT
                    m.well_name,
                    MAX(CAST(COALESCE(m.completed_at, m.started_at) AS TIMESTAMP)) AS last_processed_at
                FROM featurestore:{manifest_featurestore_key} m
                WHERE m.workflow_name = :workflow_name
                  AND m.mode = :mode
                  AND m.algorithm_version = :algorithm_version
                  AND COALESCE(m.dry_run, false) = false
                GROUP BY m.well_name
            )
            SELECT e.well_name, e.first_ts
            FROM eligible e
            LEFT JOIN manifest_last ml ON ml.well_name = e.well_name
            ORDER BY ml.last_processed_at ASC NULLS FIRST, e.latest_ts DESC, e.well_name ASC
            LIMIT {int(max_wells)}
        """
    else:
        sql = f"""
            SELECT b.well_name, MIN({selection_time_field}) AS first_ts
            FROM featurestore:{bronze_featurestore_key} b
            WHERE {" AND ".join(conditions)}
              {exclusion}
            GROUP BY b.well_name
            ORDER BY first_ts ASC, b.well_name ASC
            LIMIT {int(max_wells)}
        """

    wells_df = await query_store(
        sql=sql,
        workspace_id=workspace_id,
        params=params,
    )
    if wells_df.is_empty():
        return []
    return [str(name) for name in wells_df["well_name"].drop_nulls().to_list()]


async def load_raw_window(
    workspace_id: int,
    source_datastore_key: str,
    well_name: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
) -> pl.DataFrame:
    conditions = ["name = :well_name", "record_ts IS NOT NULL"]
    params: dict[str, Any] = {"well_name": well_name}
    if start_ts is not None:
        conditions.append("record_ts >= :start_time")
        params["start_time"] = format_dt(start_ts)
    if end_ts is not None:
        conditions.append("record_ts < :end_time")
        params["end_time"] = format_dt(end_ts)

    return await query_store(
        sql=f"""
            SELECT {", ".join(RAW_TELEMETRY_COLUMNS)}
            FROM datastore:{source_datastore_key}
            WHERE {" AND ".join(conditions)}
            ORDER BY name ASC, record_ts ASC, created_ts ASC
        """,
        workspace_id=workspace_id,
        params=params,
    )


async def load_bronze_window(
    workspace_id: int,
    bronze_featurestore_key: str,
    well_name: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
) -> pd.DataFrame:
    conditions = ["well_name = :well_name", "record_ts IS NOT NULL"]
    params: dict[str, Any] = {"well_name": well_name}
    if start_ts is not None:
        conditions.append("record_ts >= :start_time")
        params["start_time"] = format_dt(start_ts)
    if end_ts is not None:
        conditions.append("record_ts < :end_time")
        params["end_time"] = format_dt(end_ts)

    df = await query_store(
        sql=f"""
            SELECT {", ".join(BRONZE_COLUMNS)}
            FROM featurestore:{bronze_featurestore_key}
            WHERE {" AND ".join(conditions)}
            ORDER BY well_name ASC, record_ts ASC, created_ts ASC, telemetry_point_id ASC
        """,
        workspace_id=workspace_id,
        params=params,
    )
    if df.is_empty():
        return pd.DataFrame(columns=BRONZE_COLUMNS)
    out = df.to_pandas().copy()
    out["record_ts"] = pd.to_datetime(out["record_ts"], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
    out["created_ts"] = pd.to_datetime(out["created_ts"], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
    return out.dropna(subset=["well_name", "record_ts"]).reset_index(drop=True)


def prepare_raw_frame(raw_df: pl.DataFrame) -> pd.DataFrame:
    if raw_df.is_empty():
        return pd.DataFrame()
    df = raw_df.to_pandas().copy()
    # Raw datastores use `name`; featurestore-shaped frames may already use
    # `well_name`. Normalize once so the later `name -> well_name` rename
    # cannot create duplicate columns.
    if "name" not in df.columns and "well_name" in df.columns:
        df = df.rename(columns={"well_name": "name"})
    elif "name" in df.columns and "well_name" in df.columns:
        df = df.drop(columns=["well_name"])
    if "id" not in df.columns and "well_id" in df.columns:
        df = df.rename(columns={"well_id": "id"})
    elif "id" in df.columns and "well_id" in df.columns:
        df = df.drop(columns=["well_id"])
    df["record_ts"] = pd.to_datetime(df["record_ts"], errors="coerce", utc=True)
    df["created_ts"] = pd.to_datetime(df["created_ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["name", "record_ts"]).copy()
    if df.empty:
        return df
    df["record_ts"] = df["record_ts"].dt.tz_convert("UTC").dt.tz_localize(None)
    df["created_ts"] = df["created_ts"].dt.tz_convert("UTC").dt.tz_localize(None)
    df = df.sort_values(["name", "record_ts", "created_ts"], kind="mergesort")
    df["_row_seq"] = df.groupby(["name", "record_ts"], sort=False).cumcount()
    df["telemetry_point_id"] = (
        df["name"].astype(str)
        + ":"
        + df["record_ts"].map(format_dt).astype(str)
        + ":"
        + df["_row_seq"].astype(str)
    )
    return df.reset_index(drop=True)


def compute_bronze_labels(
    raw_df: pl.DataFrame,
    *,
    mode: str,
    algorithm_version: str,
    processed_at: str,
) -> pd.DataFrame:
    from nextier_core.bronze_substage_labeling import label_bronze_substages_in_window

    df = prepare_raw_frame(raw_df)
    if df.empty:
        return pd.DataFrame(columns=BRONZE_COLUMNS)

    bronze_input = df.rename(columns={"record_ts": "datetime_fmt"}).copy()
    bronze_input = bronze_input.sort_values("datetime_fmt", kind="mergesort").reset_index(drop=True)
    labels_out = label_bronze_substages_in_window(
        bronze_input,
        start_pos=0,
        end_pos=len(bronze_input) - 1,
        rate_col="rate_slurry",
        pressure_col="press_mainline",
        datetime_col="datetime_fmt",
    )
    labels = labels_out.get("labels") if isinstance(labels_out, dict) else None
    if labels is None or len(labels) != len(df):
        label_series = pd.Series(["NA"] * len(df))
    else:
        label_series = pd.Series(labels).fillna("NA").astype(str).reset_index(drop=True)

    out = df.copy()
    out["bronze_continuous"] = label_series
    out["algorithm_version"] = algorithm_version
    out["source_mode"] = mode
    out["processed_at"] = processed_at
    out = out.rename(columns={"name": "well_name", "id": "well_id"})
    return out[BRONZE_COLUMNS]


def compute_copper_labels(
    bronze_df: pd.DataFrame,
    *,
    mode: str,
    algorithm_version: str,
    processed_at: str,
    start_ordinal_by_well: dict[str, int] | None = None,
) -> pd.DataFrame:
    from nextier_core.assign_copper_confirmed import assign_copper_confirmed
    from nextier_core.assign_copper_provisional import assign_copper_provisional
    from nextier_core.expand_copper_contextual import expand_copper_contextual

    if bronze_df.empty:
        return pd.DataFrame()

    out = bronze_df.copy()
    out["record_ts"] = pd.to_datetime(out["record_ts"], errors="coerce")
    out = out.dropna(subset=["well_name", "record_ts"]).sort_values(
        ["well_name", "record_ts"], kind="mergesort"
    )

    copper_frames: list[pd.DataFrame] = []
    for _, group in out.groupby("well_name", sort=False):
        g = group.reset_index(drop=True).copy()
        well_name = str(g["well_name"].iloc[0])
        start_ordinal = 1
        if start_ordinal_by_well:
            start_ordinal = max(1, int(start_ordinal_by_well.get(well_name, 1) or 1))
        labels = g["bronze_continuous"].fillna("NA").astype(str).reset_index(drop=True)
        dts = g["record_ts"].reset_index(drop=True)
        provisional, _ = assign_copper_provisional(labels, start_ordinal=start_ordinal)
        confirmed, _ = assign_copper_confirmed(labels, start_ordinal=start_ordinal)
        continuous = expand_copper_contextual(dts, confirmed, labels=labels, only_closed=True)
        g["copper_provisional"] = np.asarray(provisional, dtype=np.int32)
        g["copper_confirmed"] = np.asarray(confirmed, dtype=np.int32)
        g["copper_continuous"] = np.asarray(continuous, dtype=np.int32)
        g["stage_num"] = g["copper_continuous"]
        g["algorithm_version"] = algorithm_version
        g["source_mode"] = mode
        g["processed_at"] = processed_at
        copper_frames.append(g)

    if not copper_frames:
        return pd.DataFrame()
    return canonicalize_copper_serving_labels(pd.concat(copper_frames, ignore_index=True), mode=mode)


def canonicalize_copper_serving_labels(copper_df: pd.DataFrame, *, mode: str) -> pd.DataFrame:
    """Keep row-level copper labels suitable for serving.

    The DS provisional series is intentionally eager: it can contain many
    candidate stage numbers while a partial live/background window is still
    uncertain. Dashboards and downstream auto labeling should not treat every
    such candidate as a stage. Continuous and confirmed labels are preserved;
    provisional-only labels are restricted to the current edge candidate for
    live/background and removed for historical backfills.
    """
    if copper_df.empty or "copper_provisional" not in copper_df.columns:
        return copper_df

    normalized_mode = str(mode or "").strip().lower()
    out = copper_df.copy()
    out["record_ts"] = pd.to_datetime(out["record_ts"], errors="coerce")

    frames: list[pd.DataFrame] = []
    for _, group in out.groupby("well_name", sort=False):
        g = group.sort_values("record_ts", kind="mergesort").copy()
        provisional = pd.to_numeric(g["copper_provisional"], errors="coerce").fillna(0).astype(int)
        confirmed = pd.to_numeric(g.get("copper_confirmed", 0), errors="coerce").fillna(0).astype(int)
        continuous = pd.to_numeric(g.get("copper_continuous", 0), errors="coerce").fillna(0).astype(int)

        protected = (confirmed > 0) | (continuous > 0)
        provisional_only = (provisional > 0) & ~protected

        # Keep row state monotonic by priority. A row may carry lower-priority
        # DS labels from the same stage, but it must not carry a different
        # provisional/confirmed stage once continuous or confirmed is known.
        continuous_mask = continuous > 0
        confirmed_mask = (confirmed > 0) & ~continuous_mask
        confirmed = confirmed.mask(continuous_mask & (confirmed != continuous), 0)
        provisional = provisional.mask(continuous_mask & (provisional != continuous), 0)
        provisional = provisional.mask(confirmed_mask & (provisional != confirmed), 0)

        if normalized_mode == "historical":
            provisional = provisional.mask(provisional_only, 0)
        elif provisional_only.any():
            latest_record_ts = g["record_ts"].max()
            edge_cutoff = latest_record_ts - pd.Timedelta(minutes=30) if pd.notna(latest_record_ts) else latest_record_ts
            candidates: list[tuple[pd.Timestamp, int]] = []
            for stage_id in sorted({int(v) for v in provisional[provisional_only].to_list() if int(v) > 0}):
                stage_rows = g.loc[provisional == stage_id]
                if stage_rows.empty:
                    continue
                candidates.append((stage_rows["record_ts"].max(), stage_id))
            keep_stage_id: int | None = None
            if candidates:
                latest_end, latest_stage_id = max(candidates, key=lambda item: (item[0], item[1]))
                if pd.isna(edge_cutoff) or latest_end >= edge_cutoff:
                    keep_stage_id = int(latest_stage_id)
            drop_mask = provisional_only & (provisional != int(keep_stage_id or 0))
            provisional = provisional.mask(drop_mask, 0)

        g["copper_provisional"] = provisional.astype("int32")
        if "copper_confirmed" in g.columns:
            g["copper_confirmed"] = confirmed.astype("int32")
        frames.append(g)

    if not frames:
        return out.iloc[0:0].copy()
    return pd.concat(frames, ignore_index=True)


def stage_bounds(df: pd.DataFrame, column: str) -> dict[int, tuple[Any, Any]]:
    if df.empty or column not in df.columns:
        return {}
    vals = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    result: dict[int, tuple[Any, Any]] = {}
    for stage_id in sorted({int(v) for v in vals if int(v) > 0}):
        g = df.loc[vals == stage_id]
        if not g.empty:
            result[stage_id] = (g["record_ts"].min(), g["record_ts"].max())
    return result


def safe_mean(series: Any) -> float | None:
    if series is None:
        return None
    values = pd.to_numeric(series, errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def safe_max(series: Any) -> float | None:
    if series is None:
        return None
    values = pd.to_numeric(series, errors="coerce").dropna()
    return None if values.empty else float(values.max())


def build_copper_stage_index(
    copper_df: pd.DataFrame,
    *,
    mode: str,
    algorithm_version: str,
    processed_at: str,
) -> pd.DataFrame:
    if copper_df.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for well_name, group in copper_df.groupby("well_name", sort=True):
        g = group.sort_values("record_ts", kind="mergesort").copy()
        provisional_bounds = stage_bounds(g, "copper_provisional")
        confirmed_bounds = stage_bounds(g, "copper_confirmed")
        continuous_bounds = stage_bounds(g, "copper_continuous")
        stage_ids = sorted(set(provisional_bounds) | set(confirmed_bounds) | set(continuous_bounds))

        for stage_id in stage_ids:
            if stage_id <= 0:
                continue
            # Serving priority is continuous > confirmed > provisional. The
            # chosen source column controls both the visible stage window and
            # the rows used by downstream auto/substage labeling.
            if stage_id in continuous_bounds:
                source_stage_column = "copper_continuous"
                stage_status = "continuous"
            elif stage_id in confirmed_bounds:
                source_stage_column = "copper_confirmed"
                stage_status = "confirmed"
            else:
                source_stage_column = "copper_provisional"
                stage_status = "provisional"

            source_vals = pd.to_numeric(g[source_stage_column], errors="coerce").fillna(0).astype(int)
            stage = g.loc[source_vals == stage_id].copy()
            if stage.empty:
                continue
            start_ts = stage["record_ts"].min()
            end_ts = stage["record_ts"].max()
            bronze_labels = set(stage.get("bronze_continuous", pd.Series(dtype=object)).fillna("NA").astype(str).to_list())
            p_start, p_end = provisional_bounds.get(stage_id, (None, None))
            c_start, c_end = confirmed_bounds.get(stage_id, (None, None))
            x_start, x_end = continuous_bounds.get(stage_id, (None, None))
            rows.append(
                {
                    "stage_uid": f"{well_name}:{stage_id}",
                    "fleet_name": last_non_null(stage["fleet_name"]),
                    "pad_name": last_non_null(stage["pad_name"]),
                    "well_name": str(well_name),
                    "well_id": last_non_null(stage["well_id"]),
                    "api_num": last_non_null(stage["api_num"]),
                    "stage_num": float(stage_id),
                    "stage_start_ts": format_dt(start_ts),
                    "stage_end_ts": format_dt(end_ts),
                    "provisional_start_ts": format_dt(p_start),
                    "provisional_end_ts": format_dt(p_end),
                    "confirmed_start_ts": format_dt(c_start),
                    "confirmed_end_ts": format_dt(c_end),
                    "continuous_start_ts": format_dt(x_start),
                    "continuous_end_ts": format_dt(x_end),
                    "is_closed": "LOW" in bronze_labels,
                    "has_mid": "MID" in bronze_labels,
                    "sample_count": float(len(stage)),
                    "avg_rate_slurry": safe_mean(stage.get("rate_slurry")),
                    "max_rate_slurry": safe_max(stage.get("rate_slurry")),
                    "avg_press_mainline": safe_mean(stage.get("press_mainline")),
                    "max_press_mainline": safe_max(stage.get("press_mainline")),
                    "stage_status": stage_status,
                    "source_stage_column": source_stage_column,
                    "algorithm_version": algorithm_version,
                    "source_mode": mode,
                    "processed_at": processed_at,
                }
            )
    return pd.DataFrame(rows)


def _copper_stage_status_rank(status: Any) -> int:
    value = str(status or "").strip().lower()
    if value == "continuous":
        return 3
    if value == "confirmed":
        return 2
    if value == "provisional":
        return 1
    return 0


def normalize_copper_stage_index_windows(stage_index_df: pd.DataFrame) -> pd.DataFrame:
    if stage_index_df.empty:
        return stage_index_df

    required = {"well_name", "stage_num", "stage_start_ts", "stage_end_ts"}
    if not required.issubset(stage_index_df.columns):
        return stage_index_df

    normalized_frames: list[pd.DataFrame] = []
    work = stage_index_df.copy()
    work["stage_start_ts"] = pd.to_datetime(work["stage_start_ts"], errors="coerce")
    work["stage_end_ts"] = pd.to_datetime(work["stage_end_ts"], errors="coerce")
    work = work.dropna(subset=["well_name", "stage_start_ts", "stage_end_ts"])

    for _, group in work.groupby("well_name", sort=True):
        rows = group.sort_values(["stage_start_ts", "stage_end_ts", "stage_num"], kind="mergesort").to_dict("records")
        accepted: list[dict[str, Any]] = []
        for row in rows:
            current = dict(row)
            while accepted and current["stage_start_ts"] < accepted[-1]["stage_end_ts"]:
                previous = accepted[-1]
                current_rank = _copper_stage_status_rank(current.get("stage_status"))
                previous_rank = _copper_stage_status_rank(previous.get("stage_status"))
                if current_rank > previous_rank:
                    previous["stage_end_ts"] = current["stage_start_ts"]
                    if previous["stage_start_ts"] > previous["stage_end_ts"]:
                        accepted.pop()
                        continue
                else:
                    current["stage_start_ts"] = previous["stage_end_ts"]
                    if current["stage_start_ts"] > current["stage_end_ts"]:
                        current = None
                    break
            if current is not None:
                accepted.append(current)
        if accepted:
            normalized_frames.append(pd.DataFrame(accepted))

    if not normalized_frames:
        return stage_index_df.iloc[0:0].copy()

    normalized = pd.concat(normalized_frames, ignore_index=True)
    normalized = normalized.loc[normalized["stage_end_ts"] > normalized["stage_start_ts"]].reset_index(drop=True)
    if normalized.empty:
        return stage_index_df.iloc[0:0].copy()

    normalized["stage_start_ts"] = normalized["stage_start_ts"].map(format_dt)
    normalized["stage_end_ts"] = normalized["stage_end_ts"].map(format_dt)
    return normalized[stage_index_df.columns]


def validate_copper_stage_index_no_overlaps(stage_index_df: pd.DataFrame) -> None:
    if stage_index_df.empty:
        return

    required = {"well_name", "stage_num", "stage_start_ts", "stage_end_ts"}
    if not required.issubset(stage_index_df.columns):
        return

    overlaps: list[dict[str, Any]] = []
    work = stage_index_df.copy()
    work["stage_start_ts"] = pd.to_datetime(work["stage_start_ts"], errors="coerce")
    work["stage_end_ts"] = pd.to_datetime(work["stage_end_ts"], errors="coerce")
    work = work.dropna(subset=["well_name", "stage_start_ts", "stage_end_ts"])

    for well_name, group in work.groupby("well_name", sort=True):
        ordered = group.sort_values(["stage_start_ts", "stage_end_ts", "stage_num"], kind="mergesort")
        previous: dict[str, Any] | None = None
        for row in ordered.to_dict("records"):
            if previous and row["stage_start_ts"] < previous["stage_end_ts"]:
                overlaps.append(
                    {
                        "well_name": well_name,
                        "stage_num": row.get("stage_num"),
                        "stage_start_ts": format_dt(row.get("stage_start_ts")),
                        "previous_stage_num": previous.get("stage_num"),
                        "previous_stage_end_ts": format_dt(previous.get("stage_end_ts")),
                    }
                )
                if len(overlaps) >= 10:
                    break
            if previous is None or row["stage_end_ts"] > previous["stage_end_ts"]:
                previous = row
        if len(overlaps) >= 10:
            break

    if overlaps:
        raise ValueError(f"Copper stage window overlap detected; refusing to write corrupted stage index: {overlaps}")


async def get_copper_stage_recompute_context(
    *,
    workspace_id: int,
    stage_index_featurestore_key: str,
    well_name: str,
    before_ts: pd.Timestamp | None,
    after_ts: pd.Timestamp | None = None,
    algorithm_version: str | None = None,
    boundary_context_hours: float = 0.5,
    max_boundary_expansion_hours: float = 96.0,
) -> dict[str, Any]:
    """Return a stage-safe recompute window and the next stage ordinal.

    ``before_ts`` is the first timestamp we would like to recompute after the
    fixed chunk/context calculation. That timestamp may still cut through an
    existing served stage. In that case we walk left to the start of the
    boundary stage, add a small cushion, and repeat until the start no longer
    falls inside any stage window.

    ``after_ts`` is the requested end of the recompute window. When it cuts
    through an existing served stage, we similarly walk right to that stage's
    end plus a small cushion. This prevents replacing stage-index rows for a
    partial stage window. Live/current-edge windows may still end at the latest
    available data; the helper can only expand into already known stage-index
    windows, never into future data that does not exist yet.
    """
    if before_ts is None:
        return {
            "recompute_start_ts": None,
            "recompute_end_ts": after_ts,
            "start_ordinal": 1,
            "context": "full_window",
            "boundary_expansions": 0,
            "right_boundary_expansions": 0,
        }

    boundary_context_hours = max(0.0, float(boundary_context_hours or 0.0))
    max_boundary_expansion_hours = max(0.0, float(max_boundary_expansion_hours or 0.0))
    original_before_ts = before_ts
    original_after_ts = after_ts
    safe_start_ts = before_ts
    safe_end_ts = after_ts
    boundary_expansions = 0
    right_boundary_expansions = 0
    boundary_stage_nums: list[int] = []
    right_boundary_stage_nums: list[int] = []
    max_expansion_start = original_before_ts - pd.Timedelta(hours=max_boundary_expansion_hours)
    max_expansion_end = (
        original_after_ts + pd.Timedelta(hours=max_boundary_expansion_hours)
        if original_after_ts is not None
        else None
    )

    base_conditions = [
        "well_name = :well_name",
        "stage_start_ts IS NOT NULL",
    ]
    if algorithm_version:
        base_conditions.append("algorithm_version = :algorithm_version")

    # Walk left while the current start is inside a stage. This is what turns a
    # chunk boundary into a stage-safe boundary and avoids partial-stage
    # recalculations that can assign multiple provisional/confirmed numbers to
    # the same physical stage.
    while True:
        params: dict[str, Any] = {
            "well_name": well_name,
            "before_ts": format_dt(safe_start_ts),
        }
        if algorithm_version:
            params["algorithm_version"] = algorithm_version
        overlap_conditions = [
            *base_conditions,
            "stage_end_ts IS NOT NULL",
            "CAST(stage_start_ts AS TIMESTAMP) <= :before_ts",
            "CAST(stage_end_ts AS TIMESTAMP) >= :before_ts",
        ]
        overlap_df = await query_store(
            sql=f"""
                SELECT
                    CAST(stage_num AS DOUBLE PRECISION) AS stage_num,
                    CAST(stage_start_ts AS TIMESTAMP) AS stage_start_ts,
                    CAST(stage_end_ts AS TIMESTAMP) AS stage_end_ts
                FROM featurestore:{stage_index_featurestore_key}
                WHERE {" AND ".join(overlap_conditions)}
                ORDER BY CAST(stage_start_ts AS TIMESTAMP) ASC
                LIMIT 1
            """,
            workspace_id=workspace_id,
            params=params,
        )
        if overlap_df.is_empty():
            break

        row = overlap_df.to_pandas().iloc[0].to_dict()
        stage_start_ts = parse_dt(row.get("stage_start_ts"))
        stage_num = pd.to_numeric(pd.Series([row.get("stage_num")]), errors="coerce").iloc[0]
        if stage_start_ts is None or pd.isna(stage_num):
            break

        candidate_start = stage_start_ts - pd.Timedelta(hours=boundary_context_hours)
        if candidate_start >= safe_start_ts:
            break
        if candidate_start < max_expansion_start:
            safe_start_ts = max_expansion_start
            boundary_expansions += 1
            boundary_stage_nums.append(int(stage_num))
            break

        safe_start_ts = candidate_start
        boundary_expansions += 1
        boundary_stage_nums.append(int(stage_num))

    # Walk right while the requested end lands inside an existing stage. This is
    # the mirror image of the left boundary handling above. Without it,
    # delete-and-replace of stage-index rows can remove a complete prior stage
    # while the recompute frame only contains the first part of that stage.
    while safe_end_ts is not None:
        params = {
            "well_name": well_name,
            "after_ts": format_dt(safe_end_ts),
        }
        if algorithm_version:
            params["algorithm_version"] = algorithm_version
        overlap_conditions = [
            *base_conditions,
            "stage_end_ts IS NOT NULL",
            "CAST(stage_start_ts AS TIMESTAMP) <= :after_ts",
            "CAST(stage_end_ts AS TIMESTAMP) >= :after_ts",
        ]
        overlap_df = await query_store(
            sql=f"""
                SELECT
                    CAST(stage_num AS DOUBLE PRECISION) AS stage_num,
                    CAST(stage_start_ts AS TIMESTAMP) AS stage_start_ts,
                    CAST(stage_end_ts AS TIMESTAMP) AS stage_end_ts
                FROM featurestore:{stage_index_featurestore_key}
                WHERE {" AND ".join(overlap_conditions)}
                ORDER BY CAST(stage_end_ts AS TIMESTAMP) DESC
                LIMIT 1
            """,
            workspace_id=workspace_id,
            params=params,
        )
        if overlap_df.is_empty():
            break

        row = overlap_df.to_pandas().iloc[0].to_dict()
        stage_end_ts = parse_dt(row.get("stage_end_ts"))
        stage_num = pd.to_numeric(pd.Series([row.get("stage_num")]), errors="coerce").iloc[0]
        if stage_end_ts is None or pd.isna(stage_num):
            break

        candidate_end = stage_end_ts + pd.Timedelta(hours=boundary_context_hours)
        if candidate_end <= safe_end_ts:
            break
        if max_expansion_end is not None and candidate_end > max_expansion_end:
            safe_end_ts = max_expansion_end
            right_boundary_expansions += 1
            right_boundary_stage_nums.append(int(stage_num))
            break

        safe_end_ts = candidate_end
        right_boundary_expansions += 1
        right_boundary_stage_nums.append(int(stage_num))

    params = {
        "well_name": well_name,
        "before_ts": format_dt(safe_start_ts),
    }
    if algorithm_version:
        params["algorithm_version"] = algorithm_version
    # Only fully-left stages are allowed to anchor the next ordinal. A stage
    # that merely starts before safe_start may still overlap the recompute
    # window and can be deleted/replaced below; counting it as prior context
    # would skip stage numbers when that same stage is not recreated.
    prior_conditions = [
        *base_conditions,
        "stage_end_ts IS NOT NULL",
        "CAST(stage_end_ts AS TIMESTAMP) < :before_ts",
    ]
    df = await query_store(
        sql=f"""
            SELECT MAX(CAST(stage_num AS DOUBLE PRECISION)) AS max_stage_num
            FROM featurestore:{stage_index_featurestore_key}
            WHERE {" AND ".join(prior_conditions)}
        """,
        workspace_id=workspace_id,
        params=params,
    )
    if df.is_empty():
        max_stage_num = pd.NA
    else:
        row = df.to_pandas().iloc[0].to_dict()
        max_stage_num = pd.to_numeric(pd.Series([row.get("max_stage_num")]), errors="coerce").iloc[0]

    if pd.isna(max_stage_num):
        start_ordinal = 1
        context = "boundary_expanded_no_prior_stage" if boundary_expansions else "no_prior_stage"
    else:
        start_ordinal = max(1, int(max_stage_num) + 1)
        context = "boundary_expanded_after_prior_stage" if boundary_expansions else "after_prior_stage"

    return {
        "recompute_start_ts": safe_start_ts,
        "recompute_end_ts": safe_end_ts,
        "start_ordinal": start_ordinal,
        "context": context,
        "boundary_expansions": boundary_expansions,
        "right_boundary_expansions": right_boundary_expansions,
        "boundary_stage_nums": boundary_stage_nums,
        "right_boundary_stage_nums": right_boundary_stage_nums,
        "requested_recompute_start_ts": original_before_ts,
        "requested_recompute_end_ts": original_after_ts,
        "max_boundary_expansion_hours": max_boundary_expansion_hours,
        "boundary_context_hours": boundary_context_hours,
    }


async def get_copper_stage_start_ordinal(
    *,
    workspace_id: int,
    stage_index_featurestore_key: str,
    well_name: str,
    before_ts: pd.Timestamp | None,
    algorithm_version: str | None = None,
) -> int:
    context = await get_copper_stage_recompute_context(
        workspace_id=workspace_id,
        stage_index_featurestore_key=stage_index_featurestore_key,
        well_name=well_name,
        before_ts=before_ts,
        algorithm_version=algorithm_version,
    )
    return int(context["start_ordinal"])


def to_polars_for_write(df: pd.DataFrame) -> pl.DataFrame:
    if df.empty:
        return pl.DataFrame()
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].map(format_dt)
    return pl.from_pandas(out)


async def featurestore_window_has_rows(
    *,
    workspace_id: int,
    featurestore_key: str,
    well_name: str,
    timestamp_field: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
) -> bool:
    conditions = [f"{timestamp_field} IS NOT NULL", "well_name = :well_name"]
    params: dict[str, Any] = {"well_name": well_name}
    if start_ts is not None:
        conditions.append(f"{timestamp_field} >= :start_time")
        params["start_time"] = format_dt(start_ts)
    if end_ts is not None:
        conditions.append(f"{timestamp_field} < :end_time")
        params["end_time"] = format_dt(end_ts)

    df = await query_store(
        sql=f"""
            SELECT 1 AS has_rows
            FROM featurestore:{featurestore_key}
            WHERE {" AND ".join(conditions)}
            LIMIT 1
        """,
        workspace_id=workspace_id,
        params=params,
    )
    return not df.is_empty()


async def delete_bronze_outputs(
    workspace_id: int,
    well_name: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    bronze_featurestore_key: str = BRONZE_LABELS_FEATURESTORE_KEY,
) -> int | None:
    filters = [{"field": "well_name", "op": "eq", "value": well_name}]
    if start_ts is not None:
        filters.append({"field": "record_ts", "op": "gte", "value": format_dt(start_ts)})
    if end_ts is not None:
        filters.append({"field": "record_ts", "op": "lt", "value": format_dt(end_ts)})
    return await delete_featurestore_records(
        featurestore_key=bronze_featurestore_key,
        workspace_id=workspace_id,
        filters=filters,
        require_primary_key_filter=False,
    )


async def delete_copper_stage_index_outputs(
    workspace_id: int,
    well_name: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    stage_index_featurestore_key: str = COPPER_STAGE_INDEX_FEATURESTORE_KEY,
) -> int | None:
    stage_filters = [{"field": "well_name", "op": "eq", "value": well_name}]
    # Delete windows that overlap the recompute interval, not only windows whose
    # start falls inside it. Live/background can otherwise leave stale stage rows
    # behind when a later pass corrects the stage number or boundary.
    if start_ts is not None:
        stage_filters.append({"field": "stage_end_ts", "op": "gte", "value": format_dt(start_ts)})
    if end_ts is not None:
        stage_filters.append({"field": "stage_start_ts", "op": "lt", "value": format_dt(end_ts)})
    return await delete_featurestore_records(
        featurestore_key=stage_index_featurestore_key,
        workspace_id=workspace_id,
        filters=stage_filters,
        require_primary_key_filter=False,
    )


async def delete_copper_outputs(
    workspace_id: int,
    well_name: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    copper_featurestore_key: str = COPPER_LABELS_FEATURESTORE_KEY,
    stage_index_featurestore_key: str = COPPER_STAGE_INDEX_FEATURESTORE_KEY,
) -> dict[str, int | None]:
    label_filters = [{"field": "well_name", "op": "eq", "value": well_name}]
    if start_ts is not None:
        label_filters.append({"field": "record_ts", "op": "gte", "value": format_dt(start_ts)})
    if end_ts is not None:
        label_filters.append({"field": "record_ts", "op": "lt", "value": format_dt(end_ts)})
    deleted = {
        copper_featurestore_key: await delete_featurestore_records(
            featurestore_key=copper_featurestore_key,
            workspace_id=workspace_id,
            filters=label_filters,
            require_primary_key_filter=False,
        )
    }
    deleted[stage_index_featurestore_key] = await delete_copper_stage_index_outputs(
        workspace_id=workspace_id,
        well_name=well_name,
        start_ts=start_ts,
        end_ts=end_ts,
        stage_index_featurestore_key=stage_index_featurestore_key,
    )
    return deleted


def make_manifest_row(
    *,
    run_id: str,
    workflow_name: str,
    mode: str,
    source_datastore_key: str,
    well_name: str,
    requested_start_ts: pd.Timestamp | None,
    requested_end_ts: pd.Timestamp | None,
    effective_start_ts: pd.Timestamp | None,
    effective_end_ts: pd.Timestamp | None,
    lookback_hours: float,
    source_df: pd.DataFrame,
    chunk_hours: float | None = None,
    chunk_index: int | None = None,
    bronze_rows: int,
    copper_rows: int,
    stage_rows: int,
    algorithm_version: str,
    dry_run: bool,
    status: str,
    started_at: str,
    completed_at: str,
    error_message: str | None = None,
) -> dict[str, Any]:
    ts_column = "record_ts" if "record_ts" in source_df.columns else None
    source_min = source_df[ts_column].min() if ts_column and not source_df.empty else None
    source_max = source_df[ts_column].max() if ts_column and not source_df.empty else None
    fleet_name = last_non_null(source_df["fleet_name"]) if "fleet_name" in source_df.columns and not source_df.empty else None
    pad_name = last_non_null(source_df["pad_name"]) if "pad_name" in source_df.columns and not source_df.empty else None
    return {
        "manifest_id": f"{run_id}:{well_name}:{uuid4()}",
        "run_id": run_id,
        "workflow_name": workflow_name,
        "mode": mode,
        "fleet_name": fleet_name,
        "pad_name": pad_name,
        "well_name": well_name,
        "requested_start_ts": format_dt(requested_start_ts),
        "requested_end_ts": format_dt(requested_end_ts),
        "effective_start_ts": format_dt(effective_start_ts),
        "effective_end_ts": format_dt(effective_end_ts),
        "lookback_hours": float(lookback_hours or 0),
        "chunk_hours": float(chunk_hours) if chunk_hours is not None else None,
        "chunk_index": float(chunk_index) if chunk_index is not None else None,
        "source_datastore_key": source_datastore_key,
        "source_row_count": float(len(source_df)),
        "source_min_record_ts": format_dt(source_min),
        "source_max_record_ts": format_dt(source_max),
        "bronze_status": "written" if bronze_rows else "empty",
        "copper_status": "written" if copper_rows else "not_run",
        "stage_index_status": "written" if stage_rows else "not_run",
        "rows_bronze_written": float(bronze_rows),
        "rows_copper_written": float(copper_rows),
        "stage_count_written": float(stage_rows),
        "algorithm_version": algorithm_version,
        "dry_run": bool(dry_run),
        "status": status,
        "error_message": error_message,
        "started_at": started_at,
        "completed_at": completed_at,
    }


async def write_manifest(
    workspace_id: int,
    manifest: dict[str, Any],
    manifest_featurestore_key: str = MANIFEST_FEATURESTORE_KEY,
) -> None:
    await write_featurestore(
        featurestore_key=manifest_featurestore_key,
        workspace_id=workspace_id,
        df=pl.from_pandas(pd.DataFrame([manifest])),
        upsert=True,
    )
