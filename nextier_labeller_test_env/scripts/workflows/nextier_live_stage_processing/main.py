from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import polars as pl
from prefect import flow, get_run_logger, task

from nixdlt.workflow_sdk.platform_tasks import (
    delete_featurestore_records,
    get_state,
    query_store,
    set_state,
    write_featurestore,
)


SOURCE_DATASTORE_KEY = "live_fleet_stream_customer_full_v1"
TARGET_FEATURESTORE_KEY = "live_td_stage_index_v1"
BRONZE_LABELS_FEATURESTORE_KEY = "live_bronze_labels_v1"
GOLD_LABELS_FEATURESTORE_KEY = "live_gold_labels_v1"
PLATINUM_LABELS_FEATURESTORE_KEY = "live_platinum_labels_v1"
CHECKPOINT_KEY = "__checkpoint__live_stage_created_ts"
TD_STATE_KEY = "td_stage_state_by_well"
STAGE_METHOD = "nextier_dash_td_gap_3600s"
BRONZE_MODEL_NAME = "nextier_dash_bronze"
GOLD_MODEL_NAME = "nextier_dash_gold"
PLATINUM_MODEL_NAME = "nextier_dash_platinum"


def _format_dt(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return ts.strftime("%Y-%m-%d %H:%M:%S.%f")


def _weighted_avg(
    old_avg: Any,
    old_count: Any,
    new_avg: Any,
    new_count: Any,
) -> float | None:
    old_count = float(old_count or 0)
    new_count = float(new_count or 0)
    total = old_count + new_count
    if total <= 0:
        return None
    old_value = 0.0 if old_avg is None or pd.isna(old_avg) else float(old_avg)
    new_value = 0.0 if new_avg is None or pd.isna(new_avg) else float(new_avg)
    return ((old_value * old_count) + (new_value * new_count)) / total


def _max_value(left: Any, right: Any) -> float | None:
    values = [v for v in (left, right) if v is not None and not pd.isna(v)]
    if not values:
        return None
    return float(max(values))


def _empty_raw_rows_like(raw_df: pl.DataFrame) -> pl.DataFrame:
    return raw_df.head(0) if not raw_df.is_empty() else pl.DataFrame()


def _filter_out_wells(df: pl.DataFrame, well_names: set[str]) -> pl.DataFrame:
    if df.is_empty() or not well_names:
        return df
    out = df.filter(~pl.col("name").cast(pl.Utf8).is_in(sorted(well_names)))
    return out if not out.is_empty() else df.head(0)


def _empty_gold_labels() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "name": [],
            "stage_num_td": [],
            "datetime_fmt": [],
            "substage": [],
            "model_name": [],
            "source_type": [],
            "created_at": [],
            "updated_at": [],
        }
    )


def _empty_bronze_labels() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "name": [],
            "stage_num_td": [],
            "datetime_fmt": [],
            "substage": [],
            "rate_slurry": [],
            "press_mainline": [],
            "prop_conc_blend_denso": [],
            "model_name": [],
            "source_type": [],
            "created_at": [],
            "updated_at": [],
        }
    )


def _empty_platinum_labels() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "name": [],
            "stage_num_td": [],
            "datetime_fmt": [],
            "substage": [],
            "rate_slurry": [],
            "press_mainline": [],
            "prop_conc_blend_denso": [],
            "inferred_design_rate": [],
            "inferred_pressure": [],
            "model_name": [],
            "source_type": [],
            "created_at": [],
            "updated_at": [],
        }
    )


@task(name="assign-td-stages")
def assign_td_stages(
    raw_df: pl.DataFrame,
    td_state_by_well: dict[str, dict[str, Any]],
    td_gap_seconds: int,
) -> tuple[pl.DataFrame, dict[str, dict[str, Any]]]:
    if raw_df.is_empty():
        return raw_df, td_state_by_well

    df = raw_df.to_pandas()
    df = df.dropna(subset=["name", "record_ts", "created_ts"]).copy()
    if df.empty:
        return pl.DataFrame(), td_state_by_well

    df["record_ts"] = pd.to_datetime(df["record_ts"], errors="coerce")
    df["created_ts"] = pd.to_datetime(df["created_ts"], errors="coerce")
    df = df.dropna(subset=["record_ts", "created_ts"]).copy()
    if df.empty:
        return pl.DataFrame(), td_state_by_well

    df["_row_id"] = range(len(df))
    df = df.sort_values(["name", "record_ts", "_row_id"], kind="mergesort")
    stage_nums: list[int] = []
    next_state = dict(td_state_by_well or {})

    for well_name, group in df.groupby("name", sort=False):
        previous = next_state.get(str(well_name), {})
        previous_record_ts = pd.to_datetime(
            previous.get("last_record_ts"),
            errors="coerce",
        )
        current_stage = int(previous.get("last_stage_num_td") or 1)
        assigned_for_group: list[int] = []

        for record_ts in group["record_ts"]:
            if pd.notna(previous_record_ts):
                diff_seconds = (record_ts - previous_record_ts).total_seconds()
                if diff_seconds >= td_gap_seconds:
                    current_stage += 1
            assigned_for_group.append(current_stage)
            previous_record_ts = record_ts

        stage_nums.extend(assigned_for_group)
        next_state[str(well_name)] = {
            "last_record_ts": _format_dt(previous_record_ts),
            "last_stage_num_td": current_stage,
        }

    df["stage_num_td"] = stage_nums
    df = df.drop(columns=["_row_id"])
    return pl.from_pandas(df), next_state


@task(name="reconcile-td-state-with-existing-windows")
def reconcile_td_state_with_existing_windows(
    td_state_by_well: dict[str, dict[str, Any]],
    existing_windows: pl.DataFrame,
) -> dict[str, dict[str, Any]]:
    """Keep TD state at least as current as the persisted stage index."""
    next_state = dict(td_state_by_well or {})
    if existing_windows.is_empty():
        return next_state

    windows_df = existing_windows.to_pandas()
    if windows_df.empty:
        return next_state

    windows_df["stage_end_ts"] = pd.to_datetime(
        windows_df["stage_end_ts"],
        errors="coerce",
    )
    windows_df = windows_df.dropna(
        subset=["name", "stage_num_td", "stage_end_ts"],
    )
    if windows_df.empty:
        return next_state

    for well_name, group in windows_df.groupby("name", sort=False):
        group = group.sort_values(["stage_end_ts", "stage_num_td"], kind="mergesort")
        latest = group.iloc[-1]
        latest_end = latest["stage_end_ts"]
        latest_stage = int(latest["stage_num_td"])
        previous = next_state.get(str(well_name), {})
        previous_record_ts = pd.to_datetime(
            previous.get("last_record_ts"),
            errors="coerce",
        )
        if pd.isna(previous_record_ts) or latest_end > previous_record_ts:
            next_state[str(well_name)] = {
                "last_record_ts": _format_dt(latest_end),
                "last_stage_num_td": latest_stage,
            }
    return next_state


@task(name="split-late-telemetry-rows")
def split_late_telemetry_rows(
    raw_df: pl.DataFrame,
    td_state_by_well: dict[str, dict[str, Any]],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if raw_df.is_empty():
        return raw_df, raw_df

    df = raw_df.to_pandas()
    df["record_ts"] = pd.to_datetime(df["record_ts"], errors="coerce")
    late_mask = pd.Series(False, index=df.index)

    for well_name, group in df.groupby("name", sort=False):
        previous = td_state_by_well.get(str(well_name), {})
        previous_record_ts = pd.to_datetime(
            previous.get("last_record_ts"),
            errors="coerce",
        )
        if pd.isna(previous_record_ts):
            continue
        late_mask.loc[group.index] = group["record_ts"] <= previous_record_ts

    normal_df = df.loc[~late_mask].copy()
    late_df = df.loc[late_mask].copy()
    return (
        pl.from_pandas(normal_df) if not normal_df.empty else _empty_raw_rows_like(raw_df),
        pl.from_pandas(late_df) if not late_df.empty else _empty_raw_rows_like(raw_df),
    )


@task(name="assign-late-rows-to-existing-stages")
def assign_late_rows_to_existing_stages(
    late_df: pl.DataFrame,
    existing_windows: pl.DataFrame,
) -> tuple[pl.DataFrame, list[str]]:
    if late_df.is_empty():
        return late_df, []
    if existing_windows.is_empty():
        well_names = sorted(str(name) for name in late_df["name"].drop_nulls().unique())
        return late_df.head(0), well_names

    rows_df = late_df.to_pandas()
    windows_df = existing_windows.to_pandas()
    rows_df["record_ts"] = pd.to_datetime(rows_df["record_ts"], errors="coerce")
    windows_df["stage_start_ts"] = pd.to_datetime(
        windows_df["stage_start_ts"],
        errors="coerce",
    )
    windows_df["stage_end_ts"] = pd.to_datetime(
        windows_df["stage_end_ts"],
        errors="coerce",
    )
    windows_df = windows_df.dropna(
        subset=["name", "stage_num_td", "stage_start_ts", "stage_end_ts"],
    )

    assigned_stage_nums: list[int] = []
    safe_row_indexes: list[Any] = []
    repair_required: list[dict[str, Any]] = []
    for _, row in rows_df.iterrows():
        well_name = str(row["name"])
        record_ts = row["record_ts"]
        matching = windows_df[
            (windows_df["name"].astype(str) == well_name)
            & (windows_df["stage_start_ts"] <= record_ts)
            & (windows_df["stage_end_ts"] >= record_ts)
        ]

        if len(matching) == 1:
            assigned_stage_nums.append(int(matching.iloc[0]["stage_num_td"]))
            safe_row_indexes.append(row.name)
            continue

        repair_required.append(
            {
                "name": well_name,
                "record_ts": _format_dt(record_ts),
                "matching_stage_windows": len(matching),
            }
        )

    repair_wells = sorted({str(item["name"]) for item in repair_required})
    safe_df = rows_df.loc[safe_row_indexes].copy()
    if safe_df.empty:
        return late_df.head(0), repair_wells

    safe_df["stage_num_td"] = assigned_stage_nums
    return pl.from_pandas(safe_df), repair_wells


@task(name="validate-stage-window-overlaps")
def validate_stage_window_overlaps(
    existing_windows: pl.DataFrame,
    windows_to_write: pl.DataFrame,
) -> None:
    if windows_to_write.is_empty():
        return

    existing_df = (
        existing_windows.to_pandas() if not existing_windows.is_empty() else pd.DataFrame()
    )
    write_df = windows_to_write.to_pandas()
    if existing_df.empty:
        check_df = write_df
    else:
        keys = {
            (str(row["name"]), int(row["stage_num_td"]))
            for _, row in write_df.iterrows()
            if pd.notna(row.get("name")) and pd.notna(row.get("stage_num_td"))
        }
        existing_df = existing_df[
            ~existing_df.apply(
                lambda row: (str(row["name"]), int(row["stage_num_td"])) in keys,
                axis=1,
            )
        ]
        affected_wells = {str(name) for name in write_df["name"].dropna().unique()}
        existing_df = existing_df[existing_df["name"].astype(str).isin(affected_wells)]
        check_df = pd.concat([existing_df, write_df], ignore_index=True)

    if check_df.empty:
        return

    check_df["stage_start_ts"] = pd.to_datetime(
        check_df["stage_start_ts"],
        errors="coerce",
    )
    check_df["stage_end_ts"] = pd.to_datetime(
        check_df["stage_end_ts"],
        errors="coerce",
    )
    check_df = check_df.dropna(
        subset=["name", "stage_num_td", "stage_start_ts", "stage_end_ts"],
    )
    overlaps: list[dict[str, Any]] = []
    for well_name, group in check_df.groupby("name", sort=True):
        group = group.sort_values(["stage_start_ts", "stage_num_td"], kind="mergesort")
        previous_end = None
        previous_stage = None
        for _, row in group.iterrows():
            if previous_end is not None and row["stage_start_ts"] <= previous_end:
                overlaps.append(
                    {
                        "name": str(well_name),
                        "stage_num_td": int(row["stage_num_td"]),
                        "stage_start_ts": _format_dt(row["stage_start_ts"]),
                        "previous_stage_num_td": previous_stage,
                        "previous_stage_end_ts": _format_dt(previous_end),
                    }
                )
            previous_end = row["stage_end_ts"]
            previous_stage = int(row["stage_num_td"])

    if overlaps:
        raise ValueError(
            "TD stage window overlap detected; refusing to write corrupted stage index: "
            f"{overlaps[:10]}"
        )


@task(name="build-stage-windows")
def build_stage_windows(classified_df: pl.DataFrame) -> pl.DataFrame:
    if classified_df.is_empty():
        return pl.DataFrame()

    df = classified_df.to_pandas()
    if df.empty:
        return pl.DataFrame()

    grouped = df.groupby(["name", "stage_num_td"], as_index=False, sort=True)
    out = grouped.agg(
        fleet_name=("fleet_name", "last"),
        pad_name=("pad_name", "last"),
        well_id=("id", "last"),
        api_num=("api_num", "last"),
        stage_start_ts=("record_ts", "min"),
        stage_end_ts=("record_ts", "max"),
        first_created_ts=("created_ts", "min"),
        last_created_ts=("created_ts", "max"),
        sample_count=("record_ts", "size"),
        avg_rate_slurry=("rate_slurry", "mean"),
        max_rate_slurry=("rate_slurry", "max"),
        avg_press_mainline=("press_mainline", "mean"),
        max_press_mainline=("press_mainline", "max"),
        avg_prop_conc_blend_denso=("prop_conc_blend_denso", "mean"),
        max_prop_conc_blend_denso=("prop_conc_blend_denso", "max"),
    )
    out["stage_detection_method"] = STAGE_METHOD

    for col in [
        "stage_start_ts",
        "stage_end_ts",
        "first_created_ts",
        "last_created_ts",
    ]:
        out[col] = out[col].map(_format_dt)

    return pl.from_pandas(out)


@task(name="merge-existing-stage-windows")
def merge_existing_stage_windows(
    new_windows: pl.DataFrame,
    existing_windows: pl.DataFrame,
) -> pl.DataFrame:
    if new_windows.is_empty():
        return new_windows
    if existing_windows.is_empty():
        return new_windows

    new_df = new_windows.to_pandas()
    existing_df = existing_windows.to_pandas()
    existing_by_key = {
        (str(row["name"]), int(row["stage_num_td"])): row
        for _, row in existing_df.iterrows()
        if pd.notna(row.get("name")) and pd.notna(row.get("stage_num_td"))
    }

    merged_rows = []
    for _, row in new_df.iterrows():
        key = (str(row["name"]), int(row["stage_num_td"]))
        old = existing_by_key.get(key)
        if old is None:
            merged_rows.append(row.to_dict())
            continue

        old_count = old.get("sample_count")
        new_count = row.get("sample_count")
        merged = row.to_dict()
        merged["stage_start_ts"] = min(
            pd.to_datetime(old.get("stage_start_ts")),
            pd.to_datetime(row.get("stage_start_ts")),
        )
        merged["stage_end_ts"] = max(
            pd.to_datetime(old.get("stage_end_ts")),
            pd.to_datetime(row.get("stage_end_ts")),
        )
        merged["first_created_ts"] = min(
            pd.to_datetime(old.get("first_created_ts")),
            pd.to_datetime(row.get("first_created_ts")),
        )
        merged["last_created_ts"] = max(
            pd.to_datetime(old.get("last_created_ts")),
            pd.to_datetime(row.get("last_created_ts")),
        )
        merged["sample_count"] = float(old_count or 0) + float(new_count or 0)
        merged["avg_rate_slurry"] = _weighted_avg(
            old.get("avg_rate_slurry"),
            old_count,
            row.get("avg_rate_slurry"),
            new_count,
        )
        merged["avg_press_mainline"] = _weighted_avg(
            old.get("avg_press_mainline"),
            old_count,
            row.get("avg_press_mainline"),
            new_count,
        )
        merged["avg_prop_conc_blend_denso"] = _weighted_avg(
            old.get("avg_prop_conc_blend_denso"),
            old_count,
            row.get("avg_prop_conc_blend_denso"),
            new_count,
        )
        merged["max_rate_slurry"] = _max_value(
            old.get("max_rate_slurry"),
            row.get("max_rate_slurry"),
        )
        merged["max_press_mainline"] = _max_value(
            old.get("max_press_mainline"),
            row.get("max_press_mainline"),
        )
        merged["max_prop_conc_blend_denso"] = _max_value(
            old.get("max_prop_conc_blend_denso"),
            row.get("max_prop_conc_blend_denso"),
        )

        for col in [
            "stage_start_ts",
            "stage_end_ts",
            "first_created_ts",
            "last_created_ts",
        ]:
            merged[col] = _format_dt(merged[col])

        merged_rows.append(merged)

    return pl.from_pandas(pd.DataFrame(merged_rows))


@task(name="build-bronze-labels")
def build_bronze_labels(
    stage_rows: pl.DataFrame,
    stage_windows: pl.DataFrame,
) -> pl.DataFrame:
    if stage_rows.is_empty() or stage_windows.is_empty():
        return _empty_bronze_labels()

    from nextier_core.bronze_substage_labeling import label_bronze_substages_in_window

    rows_df = stage_rows.to_pandas()
    windows_df = stage_windows.to_pandas()
    if rows_df.empty or windows_df.empty:
        return _empty_bronze_labels()

    rows_df = rows_df.rename(columns={"record_ts": "datetime_fmt"}).copy()
    rows_df["datetime_fmt"] = pd.to_datetime(rows_df["datetime_fmt"], errors="coerce")
    rows_df = rows_df.dropna(subset=["name", "datetime_fmt"])
    if rows_df.empty:
        return _empty_bronze_labels()

    now = _format_dt(datetime.utcnow())
    output_frames: list[pd.DataFrame] = []
    for _, window in windows_df.iterrows():
        well_name = str(window["name"])
        stage_num_td = int(window["stage_num_td"])
        start_ts = pd.to_datetime(window["stage_start_ts"], errors="coerce")
        end_ts = pd.to_datetime(window["stage_end_ts"], errors="coerce")
        if pd.isna(start_ts) or pd.isna(end_ts):
            continue

        stage_df = rows_df[
            (rows_df["name"].astype(str) == well_name)
            & (rows_df["datetime_fmt"] >= start_ts)
            & (rows_df["datetime_fmt"] <= end_ts)
        ].sort_values("datetime_fmt", kind="mergesort")
        if stage_df.empty:
            continue

        stage_df = stage_df.reset_index(drop=True)
        labels_out = label_bronze_substages_in_window(
            stage_df,
            start_pos=0,
            end_pos=len(stage_df) - 1,
            rate_col="rate_slurry",
            pressure_col="press_mainline",
            datetime_col="datetime_fmt",
        )
        labels = labels_out.get("labels") if isinstance(labels_out, dict) else None
        if labels is None:
            continue

        out = pd.DataFrame(
            {
                "name": stage_df["name"].astype(str).to_numpy(),
                "stage_num_td": stage_num_td,
                "datetime_fmt": stage_df["datetime_fmt"].map(_format_dt).to_numpy(),
                "substage": pd.Series(labels).fillna("NA").astype(str).to_numpy(),
                "rate_slurry": pd.to_numeric(
                    stage_df.get("rate_slurry"),
                    errors="coerce",
                ).to_numpy(),
                "press_mainline": pd.to_numeric(
                    stage_df.get("press_mainline"),
                    errors="coerce",
                ).to_numpy(),
                "prop_conc_blend_denso": pd.to_numeric(
                    stage_df.get("prop_conc_blend_denso"),
                    errors="coerce",
                ).to_numpy(),
                "model_name": BRONZE_MODEL_NAME,
                "source_type": "model",
                "created_at": now,
                "updated_at": now,
            }
        )
        output_frames.append(out)

    if not output_frames:
        return _empty_bronze_labels()

    return pl.from_pandas(pd.concat(output_frames, ignore_index=True))


@task(name="build-platinum-labels")
def build_platinum_labels(
    stage_rows: pl.DataFrame,
    stage_windows: pl.DataFrame,
    bronze_labels: pl.DataFrame,
    td_state_by_well: dict[str, dict[str, Any]],
) -> pl.DataFrame:
    if (
        stage_rows.is_empty()
        or stage_windows.is_empty()
        or bronze_labels.is_empty()
    ):
        return _empty_platinum_labels()

    import numpy as np

    from nextier_core.data_utils import _normalize_well_name
    from nextier_core.stage_utils import label_substages_in_window
    from nextier_utils.config.client_mode_utils import strip_prestart_from_labels

    rows_df = stage_rows.to_pandas()
    windows_df = stage_windows.to_pandas()
    bronze_df = bronze_labels.to_pandas()
    if rows_df.empty or windows_df.empty or bronze_df.empty:
        return _empty_platinum_labels()

    rows_df = rows_df.rename(columns={"record_ts": "datetime_fmt"}).copy()
    rows_df["datetime_fmt"] = pd.to_datetime(rows_df["datetime_fmt"], errors="coerce")
    rows_df = rows_df.dropna(subset=["name", "datetime_fmt"])
    bronze_df["datetime_fmt"] = pd.to_datetime(
        bronze_df["datetime_fmt"],
        errors="coerce",
    )
    bronze_df = bronze_df.dropna(subset=["name", "stage_num_td", "datetime_fmt"])
    if rows_df.empty or bronze_df.empty:
        return _empty_platinum_labels()

    now = _format_dt(datetime.utcnow())
    output_frames: list[pd.DataFrame] = []
    for _, window in windows_df.iterrows():
        well_name = str(window["name"])
        stage_num_td = int(window["stage_num_td"])
        start_ts = pd.to_datetime(window["stage_start_ts"], errors="coerce")
        end_ts = pd.to_datetime(window["stage_end_ts"], errors="coerce")
        if pd.isna(start_ts) or pd.isna(end_ts):
            continue

        stage_df = rows_df[
            (rows_df["name"].astype(str) == well_name)
            & (rows_df["datetime_fmt"] >= start_ts)
            & (rows_df["datetime_fmt"] <= end_ts)
        ].sort_values("datetime_fmt", kind="mergesort")
        if stage_df.empty:
            continue

        bronze_stage_df = bronze_df[
            (bronze_df["name"].astype(str) == well_name)
            & (pd.to_numeric(bronze_df["stage_num_td"], errors="coerce") == stage_num_td)
        ].sort_values("datetime_fmt", kind="mergesort")
        if bronze_stage_df.empty:
            continue

        mid_rows = bronze_stage_df[
            bronze_stage_df["substage"].astype(str).isin(["MID_TRIAL", "MID"])
        ]
        design_rate = np.nan
        inferred_pressure = None
        if not mid_rows.empty:
            rate_vals = pd.to_numeric(
                mid_rows["rate_slurry"],
                errors="coerce",
            ).dropna()
            pressure_vals = pd.to_numeric(
                mid_rows["press_mainline"],
                errors="coerce",
            ).dropna()
            dr = float(rate_vals.median()) if not rate_vals.empty else np.nan
            ip = float(pressure_vals.median()) if not pressure_vals.empty else np.nan
            design_rate = dr if np.isfinite(dr) and dr > 0 else np.nan
            inferred_pressure = ip if np.isfinite(ip) and ip > 0 else None

        design_lookup = pd.DataFrame(
            [
                {
                    "well_norm": _normalize_well_name(well_name),
                    "stage_number": float(stage_num_td),
                    "design_rate": design_rate,
                }
            ]
        )

        stage_df = stage_df.reset_index(drop=True)
        dt_diffs = stage_df["datetime_fmt"].diff().dt.total_seconds().dropna()
        dt_override = float(dt_diffs.median()) if not dt_diffs.empty else 1.0
        stage_df["stage_num"] = float(stage_num_td)
        labels_full = label_substages_in_window(
            stage_df,
            start_pos=0,
            end_pos=len(stage_df) - 1,
            design_lookup=design_lookup,
            dt_override=dt_override,
            allow_design_rate_recompute=False,
            stage_end_confirmed=True,
        )

        if isinstance(labels_full, dict):
            lab = labels_full.get("labels")
            bron = bronze_stage_df["substage"].reset_index(drop=True)
            if lab is not None and len(lab) == len(bron):
                stage_end_mask = lab == "stage_end"
                if stage_end_mask.any():
                    ie = int(np.flatnonzero(stage_end_mask)[0])
                    remainder_bronze = bron.iloc[ie + 1 :].reset_index(drop=True)
                    j_pre = None
                    for j in range(len(remainder_bronze)):
                        if remainder_bronze.iloc[j] != "PRE":
                            continue
                        has_low_or_pre_low_before = remainder_bronze.iloc[:j].isin(
                            ["LOW", "PRE_LOW"]
                        ).any()
                        has_mid_trial_after = (
                            remainder_bronze.iloc[j + 1 :] == "MID_TRIAL"
                        ).any()
                        if has_low_or_pre_low_before and has_mid_trial_after:
                            j_pre = j
                            break
                    if j_pre is not None:
                        start_pos_new = (ie + 1) + j_pre
                        if start_pos_new <= len(stage_df) - 1:
                            second = label_substages_in_window(
                                stage_df,
                                start_pos=start_pos_new,
                                end_pos=len(stage_df) - 1,
                                design_lookup=design_lookup,
                                dt_override=dt_override,
                                allow_design_rate_recompute=False,
                                stage_end_confirmed=True,
                            )
                            if (
                                isinstance(second, dict)
                                and second.get("labels") is not None
                            ):
                                second_labels = second["labels"]
                                full_labels = lab.copy()
                                full_labels.iloc[ie + 1 : ie + 1 + j_pre] = "NA"
                                full_labels.iloc[ie + 1 + j_pre :] = second_labels.values
                                labels_full = {**labels_full, "labels": full_labels}

        labels_full = strip_prestart_from_labels(labels_full)
        labels = labels_full.get("labels") if isinstance(labels_full, dict) else None
        if labels is None:
            continue
        labels = pd.Series(labels).replace({"design": "slurry", "DESIGN": "slurry"})

        out = pd.DataFrame(
            {
                "name": stage_df["name"].astype(str).to_numpy(),
                "stage_num_td": stage_num_td,
                "datetime_fmt": stage_df["datetime_fmt"].map(_format_dt).to_numpy(),
                "substage": labels.fillna("NA").astype(str).to_numpy(),
                "rate_slurry": pd.to_numeric(
                    stage_df.get("rate_slurry"),
                    errors="coerce",
                ).to_numpy(),
                "press_mainline": pd.to_numeric(
                    stage_df.get("press_mainline"),
                    errors="coerce",
                ).to_numpy(),
                "prop_conc_blend_denso": pd.to_numeric(
                    stage_df.get("prop_conc_blend_denso"),
                    errors="coerce",
                ).to_numpy(),
                "inferred_design_rate": design_rate,
                "inferred_pressure": inferred_pressure,
                "model_name": PLATINUM_MODEL_NAME,
                "source_type": "model",
                "created_at": now,
                "updated_at": now,
            }
        )
        output_frames.append(out)

    if not output_frames:
        return _empty_platinum_labels()

    return pl.from_pandas(pd.concat(output_frames, ignore_index=True))


@task(name="build-gold-labels")
def build_gold_labels(
    stage_rows: pl.DataFrame,
    stage_windows: pl.DataFrame,
    td_state_by_well: dict[str, dict[str, Any]],
) -> pl.DataFrame:
    if stage_rows.is_empty() or stage_windows.is_empty():
        return _empty_gold_labels()

    from nextier_core.gold_substage_labeling import label_substages_in_window_gold
    from nextier_core.stage_utils import detect_stage_window

    rows_df = stage_rows.to_pandas()
    windows_df = stage_windows.to_pandas()
    if rows_df.empty or windows_df.empty:
        return _empty_gold_labels()

    rows_df = rows_df.rename(columns={"record_ts": "datetime_fmt"}).copy()
    rows_df["datetime_fmt"] = pd.to_datetime(rows_df["datetime_fmt"], errors="coerce")
    rows_df = rows_df.dropna(subset=["name", "datetime_fmt"])
    if rows_df.empty:
        return _empty_gold_labels()

    now = _format_dt(datetime.utcnow())
    output_frames: list[pd.DataFrame] = []
    for _, window in windows_df.iterrows():
        well_name = str(window["name"])
        stage_num_td = int(window["stage_num_td"])
        start_ts = pd.to_datetime(window["stage_start_ts"], errors="coerce")
        end_ts = pd.to_datetime(window["stage_end_ts"], errors="coerce")
        if pd.isna(start_ts) or pd.isna(end_ts):
            continue

        stage_df = rows_df[
            (rows_df["name"].astype(str) == well_name)
            & (rows_df["datetime_fmt"] >= start_ts)
            & (rows_df["datetime_fmt"] <= end_ts)
        ].sort_values("datetime_fmt", kind="mergesort")
        if stage_df.empty:
            continue

        stage_df = stage_df.reset_index(drop=True)
        dt_diffs = stage_df["datetime_fmt"].diff().dt.total_seconds().dropna()
        dt_override = float(dt_diffs.median()) if not dt_diffs.empty else 1.0
        start_pos, end_pos, stage_end_confirmed, _ = detect_stage_window(
            stage_df,
            rate_col="rate_slurry",
            datetime_col="datetime_fmt",
            dt_override=dt_override,
        )
        labels_out = label_substages_in_window_gold(
            stage_df,
            start_pos=start_pos,
            end_pos=end_pos,
            design_lookup=None,
            rate_col="rate_slurry",
            datetime_col="datetime_fmt",
            prop_conc_col="prop_conc_blend_denso",
            stage_end_confirmed=stage_end_confirmed,
        )
        labels = labels_out.get("labels") if isinstance(labels_out, dict) else None
        if labels is None:
            continue

        window_df = stage_df.iloc[start_pos : end_pos + 1].reset_index(drop=True)
        out = pd.DataFrame(
            {
                "name": window_df["name"].astype(str).to_numpy(),
                "stage_num_td": stage_num_td,
                "datetime_fmt": window_df["datetime_fmt"].map(_format_dt).to_numpy(),
                "substage": pd.Series(labels).fillna("NA").astype(str).to_numpy(),
                "model_name": GOLD_MODEL_NAME,
                "source_type": "model",
                "created_at": now,
                "updated_at": now,
            }
        )
        output_frames.append(out)

    if not output_frames:
        return _empty_gold_labels()

    return pl.from_pandas(pd.concat(output_frames, ignore_index=True))


@flow(name="nextier-live-stage-processing")
async def nextier_live_stage_processing_flow(
    workspace_id: int,
    workflow_id: int,
    batch_limit: int = 100000,
    td_gap_seconds: int = 3600,
):
    logger = get_run_logger()
    state = await get_state(workflow_id=workflow_id, workspace_id=workspace_id)

    raw_df = await query_store(
        sql=f"""
            SELECT
              created_ts,
              fleet_name,
              pad_name,
              record_ts,
              name,
              id,
              api_num,
              rate_slurry,
              prop_conc_blend_denso,
              press_mainline
            FROM datastore:{SOURCE_DATASTORE_KEY}
            WHERE
              created_ts IS NOT NULL
              AND record_ts IS NOT NULL
              AND name IS NOT NULL
              AND created_ts > :{CHECKPOINT_KEY}
            ORDER BY
              created_ts ASC,
              record_ts ASC,
              name ASC
            LIMIT {batch_limit}
        """,
        workspace_id=workspace_id,
        params={**state, "batch_limit": int(batch_limit)},
    )

    if raw_df.is_empty():
        logger.info("No new live telemetry rows to process")
        return {"rows_processed": 0, "stage_windows_upserted": 0}

    existing_windows = await query_store(
        sql=f"""
            SELECT
              name,
              stage_num_td,
              fleet_name,
              pad_name,
              well_id,
              api_num,
              stage_start_ts,
              stage_end_ts,
              first_created_ts,
              last_created_ts,
              sample_count,
              avg_rate_slurry,
              max_rate_slurry,
              avg_press_mainline,
              max_press_mainline,
              avg_prop_conc_blend_denso,
              max_prop_conc_blend_denso,
              stage_detection_method
            FROM featurestore:{TARGET_FEATURESTORE_KEY}
        """,
        workspace_id=workspace_id,
    )
    effective_td_state = reconcile_td_state_with_existing_windows(
        td_state_by_well=state.get(TD_STATE_KEY) or {},
        existing_windows=existing_windows,
    )
    normal_raw_df, late_raw_df = split_late_telemetry_rows(
        raw_df=raw_df,
        td_state_by_well=effective_td_state,
    )
    classified_normal_df, next_td_state = assign_td_stages(
        raw_df=normal_raw_df,
        td_state_by_well=effective_td_state,
        td_gap_seconds=int(td_gap_seconds),
    )
    classified_late_df, repair_wells = assign_late_rows_to_existing_stages(
        late_df=late_raw_df,
        existing_windows=existing_windows,
    )
    repair_well_set = set(repair_wells)
    classified_normal_df = _filter_out_wells(classified_normal_df, repair_well_set)
    classified_late_df = _filter_out_wells(classified_late_df, repair_well_set)
    classified_frames = [
        df for df in [classified_normal_df, classified_late_df] if not df.is_empty()
    ]
    windows_to_write = pl.DataFrame()
    bronze_labels = _empty_bronze_labels()
    platinum_labels = _empty_platinum_labels()
    gold_labels = _empty_gold_labels()

    if classified_frames:
        classified_df = pl.concat(classified_frames, how="diagonal_relaxed")
        new_windows = build_stage_windows(classified_df)
        if not new_windows.is_empty():
            windows_to_write = merge_existing_stage_windows(new_windows, existing_windows)
            validate_stage_window_overlaps(
                existing_windows=existing_windows,
                windows_to_write=windows_to_write,
            )

            await write_featurestore(
                featurestore_key=TARGET_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                df=windows_to_write,
                upsert=True,
            )

            windows_pd = windows_to_write.to_pandas()
            affected_params: dict[str, Any] = {}
            affected_values: list[str] = []
            for idx, row in windows_pd.reset_index(drop=True).iterrows():
                name_key = f"affected_name_{idx}"
                start_key = f"affected_start_{idx}"
                end_key = f"affected_end_{idx}"
                affected_params[name_key] = str(row["name"])
                affected_params[start_key] = _format_dt(row["stage_start_ts"])
                affected_params[end_key] = _format_dt(row["stage_end_ts"])
                affected_values.append(
                    f"SELECT :{name_key} AS name, "
                    f"CAST(:{start_key} AS timestamp) AS stage_start_ts, "
                    f"CAST(:{end_key} AS timestamp) AS stage_end_ts"
                )

            affected_stage_rows = await query_store(
                sql=f"""
                    WITH affected_windows AS (
                      {" UNION ALL ".join(affected_values)}
                    )
                    SELECT DISTINCT
                      t.created_ts,
                      t.fleet_name,
                      t.pad_name,
                      t.record_ts,
                      t.name,
                      t.id,
                      t.api_num,
                      t.rate_slurry,
                      t.prop_conc_blend_denso,
                      t.press_mainline
                    FROM datastore:{SOURCE_DATASTORE_KEY} t
                    JOIN affected_windows w
                      ON t.name = w.name
                     AND t.record_ts >= w.stage_start_ts
                     AND t.record_ts <= w.stage_end_ts
                    ORDER BY
                      t.name ASC,
                      t.record_ts ASC
                """,
                workspace_id=workspace_id,
                params=affected_params,
            )
            bronze_labels = build_bronze_labels(
                stage_rows=affected_stage_rows,
                stage_windows=windows_to_write,
            )
            if not bronze_labels.is_empty():
                await write_featurestore(
                    featurestore_key=BRONZE_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=bronze_labels,
                    upsert=True,
                )

            platinum_labels = build_platinum_labels(
                stage_rows=affected_stage_rows,
                stage_windows=windows_to_write,
                bronze_labels=bronze_labels,
                td_state_by_well=next_td_state,
            )
            if not platinum_labels.is_empty():
                await write_featurestore(
                    featurestore_key=PLATINUM_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=platinum_labels,
                    upsert=True,
                )

            gold_labels = build_gold_labels(
                stage_rows=affected_stage_rows,
                stage_windows=windows_to_write,
                td_state_by_well=next_td_state,
            )
            if not gold_labels.is_empty():
                await write_featurestore(
                    featurestore_key=GOLD_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=gold_labels,
                    upsert=True,
                )

    repair_stage_windows = pl.DataFrame()
    repair_bronze_labels = _empty_bronze_labels()
    repair_platinum_labels = _empty_platinum_labels()
    repair_gold_labels = _empty_gold_labels()
    if repair_wells:
        logger.warning("Running full TD stage repair for wells: %s", repair_wells)
        repair_params: dict[str, Any] = {}
        repair_values: list[str] = []
        for idx, well_name in enumerate(repair_wells):
            key = f"repair_well_{idx}"
            repair_params[key] = well_name
            repair_values.append(f"SELECT :{key} AS name")

        repair_raw_df = await query_store(
            sql=f"""
                WITH repair_wells AS (
                  {" UNION ALL ".join(repair_values)}
                )
                SELECT
                  t.created_ts,
                  t.fleet_name,
                  t.pad_name,
                  t.record_ts,
                  t.name,
                  t.id,
                  t.api_num,
                  t.rate_slurry,
                  t.prop_conc_blend_denso,
                  t.press_mainline
                FROM datastore:{SOURCE_DATASTORE_KEY} t
                JOIN repair_wells w
                  ON t.name = w.name
                WHERE
                  t.created_ts IS NOT NULL
                  AND t.record_ts IS NOT NULL
                  AND t.name IS NOT NULL
                ORDER BY
                  t.name ASC,
                  t.record_ts ASC,
                  t.created_ts ASC
            """,
            workspace_id=workspace_id,
            params=repair_params,
        )
        repair_classified_df, repair_td_state = assign_td_stages(
            raw_df=repair_raw_df,
            td_state_by_well={},
            td_gap_seconds=int(td_gap_seconds),
        )
        repair_stage_windows = build_stage_windows(repair_classified_df)
        if repair_raw_df.is_empty() or repair_stage_windows.is_empty():
            raise ValueError(
                "Targeted TD stage repair produced no replacement rows; refusing to delete existing generated rows"
            )
        validate_stage_window_overlaps(
            existing_windows=pl.DataFrame(),
            windows_to_write=repair_stage_windows,
        )

        for well_name in repair_wells:
            filters = [{"field": "name", "op": "eq", "value": well_name}]
            await delete_featurestore_records(
                featurestore_key=BRONZE_LABELS_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                filters=filters,
            )
            await delete_featurestore_records(
                featurestore_key=GOLD_LABELS_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                filters=filters,
            )
            await delete_featurestore_records(
                featurestore_key=PLATINUM_LABELS_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                filters=filters,
            )
            await delete_featurestore_records(
                featurestore_key=TARGET_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                filters=filters,
            )

        if not repair_stage_windows.is_empty():
            await write_featurestore(
                featurestore_key=TARGET_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                df=repair_stage_windows,
                upsert=True,
            )
            repair_bronze_labels = build_bronze_labels(
                stage_rows=repair_raw_df,
                stage_windows=repair_stage_windows,
            )
            if not repair_bronze_labels.is_empty():
                await write_featurestore(
                    featurestore_key=BRONZE_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=repair_bronze_labels,
                    upsert=True,
                )
            repair_platinum_labels = build_platinum_labels(
                stage_rows=repair_raw_df,
                stage_windows=repair_stage_windows,
                bronze_labels=repair_bronze_labels,
                td_state_by_well=repair_td_state,
            )
            if not repair_platinum_labels.is_empty():
                await write_featurestore(
                    featurestore_key=PLATINUM_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=repair_platinum_labels,
                    upsert=True,
                )
            repair_gold_labels = build_gold_labels(
                stage_rows=repair_raw_df,
                stage_windows=repair_stage_windows,
                td_state_by_well=repair_td_state,
            )
            if not repair_gold_labels.is_empty():
                await write_featurestore(
                    featurestore_key=GOLD_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=repair_gold_labels,
                    upsert=True,
                )
            next_td_state.update(repair_td_state)

    checkpoint = raw_df["created_ts"].max()
    await set_state(
        workflow_id=workflow_id,
        workspace_id=workspace_id,
        key=CHECKPOINT_KEY,
        value=str(checkpoint),
    )
    await set_state(
        workflow_id=workflow_id,
        workspace_id=workspace_id,
        key=TD_STATE_KEY,
        value=next_td_state,
    )

    logger.info(
        "Processed %s rows, upserted %s TD stage windows, upserted %s Bronze labels, upserted %s Platinum labels, upserted %s Gold labels, repaired %s wells, rewrote %s repair TD stage windows through created_ts=%s",
        len(raw_df),
        len(windows_to_write),
        len(bronze_labels),
        len(platinum_labels),
        len(gold_labels),
        len(repair_wells),
        len(repair_stage_windows),
        checkpoint,
    )
    return {
        "rows_processed": len(raw_df),
        "stage_windows_upserted": len(windows_to_write),
        "bronze_labels_upserted": len(bronze_labels),
        "platinum_labels_upserted": len(platinum_labels),
        "gold_labels_upserted": len(gold_labels),
        "repair_wells": repair_wells,
        "repair_stage_windows_rewritten": len(repair_stage_windows),
        "repair_bronze_labels_rewritten": len(repair_bronze_labels),
        "repair_platinum_labels_rewritten": len(repair_platinum_labels),
        "repair_gold_labels_rewritten": len(repair_gold_labels),
        CHECKPOINT_KEY: str(checkpoint),
    }
