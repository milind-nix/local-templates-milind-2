from __future__ import annotations

from pathlib import Path
import sys
from typing import Any
from uuid import uuid4

TEMPLATE_ROOT = Path(__file__).resolve().parents[3]
if str(TEMPLATE_ROOT) not in sys.path:
    sys.path.insert(0, str(TEMPLATE_ROOT))

import numpy as np
import pandas as pd
import polars as pl
from prefect import flow, get_run_logger

from nixdlt.workflow_sdk.platform_tasks import delete_featurestore_records, query_store, write_featurestore
from scripts.workflows.nextier_labeling_common_v1.common import (
    SOURCE_DATASTORE_KEY,
    apply_fleet_filters,
    apply_well_name_filters,
    build_time_chunks,
    format_dt,
    get_copper_stage_recompute_context,
    get_raw_scope_bounds,
    get_well_index_scope_bounds,
    load_raw_window,
    make_manifest_row,
    normalize_name_list,
    now_utc,
    parse_dt,
    resolve_window,
    select_raw_wells,
    to_polars_for_write,
    validate_mode,
    well_index_featurestore_for_source,
    write_manifest,
)

WORKFLOW_NAME = "nextier_titanium_stage_orchestration_v1"
DEFAULT_ALGORITHM_VERSION = "nextier_titanium_stage_v1"
TITANIUM_LABELS_FEATURESTORE_KEY = "nextier_titanium_labels_v1"
TITANIUM_STAGE_INDEX_FEATURESTORE_KEY = "nextier_titanium_stage_index_v1"
TITANIUM_MANIFEST_FEATURESTORE_KEY = "nextier_titanium_processing_manifest_v1"
CONC_COLUMNS = {
    "auger": "prop_conc_blend_auger",
    "denso": "prop_conc_blend_denso",
    "inline": "prop_conc_inline",
    "target": "prop_conc_target",
}


def _safe_num(value: Any) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return float(value)


def _stage_value(row: pd.Series) -> float:
    final_value = _safe_num(row.get("titanium_final"))
    first_value = _safe_num(row.get("titanium_first"))
    return final_value if final_value > 0 else first_value


async def _previous_stage_offset(
    *,
    workspace_id: int,
    stage_index_featurestore_key: str,
    well_name: str,
    before_ts: pd.Timestamp | None,
    algorithm_version: str,
) -> int:
    if before_ts is None:
        return 0
    df = await query_store(
        sql=f"""
            SELECT COALESCE(MAX(CAST(stage_num AS DOUBLE PRECISION)), 0) AS max_stage_num
            FROM featurestore:{stage_index_featurestore_key}
            WHERE well_name = :well_name
              AND algorithm_version = :algorithm_version
              AND stage_end_ts IS NOT NULL
              AND CAST(stage_end_ts AS TIMESTAMP) < CAST(:before_ts AS TIMESTAMP)
        """,
        workspace_id=workspace_id,
        params={"well_name": well_name, "before_ts": format_dt(before_ts), "algorithm_version": algorithm_version},
    )
    if df.is_empty():
        return 0
    value = df.to_pandas().iloc[0].get("max_stage_num")
    return int(value or 0)


def _build_stage_index(labels: pd.DataFrame, run_id: str, algorithm_version: str, mode: str) -> pd.DataFrame:
    if labels.empty:
        return pd.DataFrame()
    frame = labels.copy().sort_values(["record_ts", "created_ts", "telemetry_point_id"], na_position="last")
    frame["_stage"] = frame.apply(_stage_value, axis=1).astype(float)
    frame["_status"] = np.where(frame["titanium_final"].fillna(0).astype(float) > 0, "final", "first")
    frame = frame[frame["_stage"] > 0].copy()
    if frame.empty:
        return pd.DataFrame()

    # Split on stage/status changes and large telemetry gaps. This keeps the
    # compact index faithful to exactly what Titanium labeled in row data.
    ts = pd.to_datetime(frame["record_ts"], errors="coerce")
    change = (frame["_stage"] != frame["_stage"].shift()) | (frame["_status"] != frame["_status"].shift())
    change |= ts.diff().dt.total_seconds().fillna(0) > 300
    frame["_segment"] = change.cumsum()

    rows: list[dict[str, Any]] = []
    for _, group in frame.groupby("_segment", sort=True):
        stage_num = float(group["_stage"].iloc[0])
        status = str(group["_status"].iloc[0])
        start_ts = group["record_ts"].min()
        end_ts = group["record_ts"].max()
        base = {
            # Deterministic key: repeated live/background/historical runs should
            # replace the same logical Titanium segment instead of appending a
            # new random stage-index row for the same well/stage/time.
            "stage_uid": f"{algorithm_version}:{group['well_name'].iloc[0]}:{int(stage_num)}:{status}:{format_dt(start_ts)}",
            "fleet_name": group["fleet_name"].dropna().iloc[-1] if group["fleet_name"].notna().any() else None,
            "pad_name": group["pad_name"].dropna().iloc[-1] if group["pad_name"].notna().any() else None,
            "well_name": group["well_name"].iloc[0],
            "well_id": group["well_id"].dropna().iloc[-1] if group["well_id"].notna().any() else None,
            "api_num": group["api_num"].dropna().iloc[-1] if group["api_num"].notna().any() else None,
            "stage_num": stage_num,
            "stage_start_ts": start_ts,
            "stage_end_ts": end_ts,
            "first_start_ts": start_ts if status == "first" else None,
            "first_end_ts": end_ts if status == "first" else None,
            "final_start_ts": start_ts if status == "final" else None,
            "final_end_ts": end_ts if status == "final" else None,
            "sample_count": float(len(group)),
            "avg_rate_slurry": group["rate_slurry"].astype(float).mean() if "rate_slurry" in group else None,
            "max_rate_slurry": group["rate_slurry"].astype(float).max() if "rate_slurry" in group else None,
            "avg_press_mainline": group["press_mainline"].astype(float).mean() if "press_mainline" in group else None,
            "max_press_mainline": group["press_mainline"].astype(float).max() if "press_mainline" in group else None,
            "stage_status": status,
            "source_stage_column": "titanium_final" if status == "final" else "titanium_first",
            "algorithm_version": algorithm_version,
            "source_mode": mode,
            "processed_at": now_utc(),
        }
        rows.append(base)
    return pd.DataFrame(rows)


async def _delete_outputs(
    workspace_id: int,
    well_name: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    labels_key: str,
    stage_index_key: str,
) -> dict[str, int | None]:
    label_filters = [{"field": "well_name", "op": "eq", "value": well_name}]
    if start_ts is not None:
        label_filters.append({"field": "record_ts", "op": "gte", "value": format_dt(start_ts)})
    if end_ts is not None:
        label_filters.append({"field": "record_ts", "op": "lt", "value": format_dt(end_ts)})
    stage_filters = [{"field": "well_name", "op": "eq", "value": well_name}]
    if start_ts is not None:
        stage_filters.append({"field": "stage_end_ts", "op": "gte", "value": format_dt(start_ts)})
    if end_ts is not None:
        stage_filters.append({"field": "stage_start_ts", "op": "lt", "value": format_dt(end_ts)})
    return {
        labels_key: await delete_featurestore_records(
            featurestore_key=labels_key,
            workspace_id=workspace_id,
            filters=label_filters,
            require_primary_key_filter=False,
        ),
        stage_index_key: await delete_featurestore_records(
            featurestore_key=stage_index_key,
            workspace_id=workspace_id,
            filters=stage_filters,
            require_primary_key_filter=False,
        ),
    }


async def _select_titanium_historical_pending_work(
    *,
    workspace_id: int,
    source_datastore_key: str,
    manifest_featurestore_key: str,
    well_name: str | None,
    well_names: list[str] | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    chunk_hours: float,
    max_chunks: int | None,
    max_wells: int,
    skip_completed: bool,
    algorithm_version: str,
) -> list[dict[str, Any]]:
    """Select Titanium historical work well-first from manifest progress.

    Historical Titanium should not walk global time chunks looking for wells.
    It first resolves wells in scope, then advances each well through its own
    pending chunks. This mirrors the corrected silver planner semantics while
    using Titanium's own manifest rows.
    """
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
        "workflow_name": WORKFLOW_NAME,
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

    if skip_completed:
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
                            CAST(
                                FLOOR(
                                    EXTRACT(EPOCH FROM (CAST(m.requested_end_ts AS TIMESTAMP) - CAST(:start_time AS TIMESTAMP)))
                                    / (:chunk_hours * 3600.0)
                                    - 0.000001
                                ) AS BIGINT
                            )
                        ) AS max_chunk
                    FROM featurestore:{manifest_featurestore_key} m
                    WHERE m.workflow_name = :workflow_name
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
                    mp.max_chunk
                FROM indexed_wells iw
                LEFT JOIN manifest_progress mp ON mp.well_name = iw.well_name
                ORDER BY iw.first_ts ASC, iw.well_name ASC
            """,
            workspace_id=workspace_id,
            params=params,
        )
    else:
        progress_df = await query_store(
            sql=f"""
                SELECT
                    w.name AS well_name,
                    CAST(w.first_record_ts AS TIMESTAMP) AS first_ts,
                    CAST(w.last_record_ts AS TIMESTAMP) AS last_ts,
                    NULL AS max_chunk
                FROM featurestore:{well_index_key} w
                WHERE {" AND ".join(conditions)}
                ORDER BY w.first_record_ts ASC, w.name ASC
            """,
            workspace_id=workspace_id,
            params=params,
        )

    if targeted_wells:
        present_wells = set(progress_df["well_name"].drop_nulls().to_list()) if not progress_df.is_empty() else set()
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
        raw_params = params.copy()
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
                )
                SELECT
                    iw.well_name,
                    iw.first_ts,
                    iw.last_ts,
                    NULL AS max_chunk
                FROM indexed_wells iw
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
    for row in progress_df.to_pandas().to_dict("records"):
        first_ts = parse_dt(row.get("first_ts"))
        last_ts = parse_dt(row.get("last_ts"))
        if first_ts is None or last_ts is None:
            continue
        first_chunk = max(0, int(np.floor((first_ts - start_ts).total_seconds() / chunk_seconds)))
        last_chunk = min(total_chunks - 1, int(np.floor((last_ts - start_ts).total_seconds() / chunk_seconds)))
        if last_chunk < first_chunk:
            continue
        max_chunk = row.get("max_chunk")
        next_chunk = int(max_chunk) + 1 if skip_completed and pd.notna(max_chunk) else first_chunk
        logger.info(
            "Titanium historical planner well=%s first_ts=%s last_ts=%s first_chunk=%s last_chunk=%s max_chunk=%s next_chunk=%s",
            row.get("well_name"),
            format_dt(first_ts),
            format_dt(last_ts),
            first_chunk,
            last_chunk,
            None if pd.isna(max_chunk) else int(max_chunk),
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

    selected = sorted(candidates, key=lambda item: (int(item["next_chunk"]), item["first_ts"], str(item["well_name"])))[:well_limit]
    rows: list[dict[str, Any]] = []
    for item in selected:
        next_chunk = int(item["next_chunk"])
        last_chunk = int(item["last_chunk"])
        chunk_count = min(chunks_per_well, last_chunk - next_chunk + 1)
        selected_chunks = [next_chunk + offset for offset in range(chunk_count)]
        logger.info("Titanium historical planner selected well=%s chunks=%s", item["well_name"], selected_chunks)
        for offset in range(chunk_count):
            chunk_index = next_chunk + offset
            chunk_start_ts = start_ts + pd.Timedelta(seconds=chunk_index * chunk_seconds)
            chunk_end_ts = min(start_ts + pd.Timedelta(seconds=(chunk_index + 1) * chunk_seconds), end_ts)
            rows.append(
                {
                    "well_name": str(item["well_name"]),
                    "chunk_index": chunk_index,
                    "chunk_start_ts": chunk_start_ts,
                    "chunk_end_ts": chunk_end_ts,
                }
            )
    return rows


async def _process_well(
    *,
    workspace_id: int,
    run_id: str,
    mode: str,
    source_datastore_key: str,
    labels_key: str,
    stage_index_key: str,
    manifest_key: str,
    well_name: str,
    requested_start_ts: pd.Timestamp | None,
    requested_end_ts: pd.Timestamp | None,
    effective_start_ts: pd.Timestamp | None,
    effective_end_ts: pd.Timestamp | None,
    context_hours: float,
    chunk_hours: float | None,
    chunk_index: int | None,
    delete_existing: bool,
    dry_run: bool,
    algorithm_version: str,
    concentration_feature: str,
    stage_offset: int | None = None,
) -> dict[str, Any]:
    logger = get_run_logger()
    started_at = format_dt(now_utc())
    load_start_ts = effective_start_ts
    if load_start_ts is not None and context_hours > 0:
        load_start_ts = load_start_ts - pd.Timedelta(hours=context_hours)

    logger.info(
        "Titanium raw load start well=%s datastore=%s start=%s end=%s context_hours=%s",
        well_name,
        source_datastore_key,
        format_dt(load_start_ts),
        format_dt(effective_end_ts),
        context_hours,
    )
    raw_pl = await load_raw_window(workspace_id, source_datastore_key, well_name, load_start_ts, effective_end_ts)
    raw = raw_pl.to_pandas() if not raw_pl.is_empty() else pd.DataFrame()
    logger.info("Titanium raw load complete well=%s rows=%s", well_name, len(raw))
    if raw.empty:
        manifest = make_manifest_row(
            run_id=run_id, workflow_name=WORKFLOW_NAME, mode=mode, source_datastore_key=source_datastore_key,
            well_name=well_name, requested_start_ts=requested_start_ts, requested_end_ts=requested_end_ts,
            effective_start_ts=effective_start_ts, effective_end_ts=effective_end_ts, lookback_hours=context_hours,
            source_df=raw, chunk_hours=chunk_hours, chunk_index=chunk_index, bronze_rows=0, copper_rows=0,
            stage_rows=0, algorithm_version=algorithm_version, dry_run=dry_run, status="empty",
            started_at=started_at, completed_at=format_dt(now_utc()),
        )
        if not dry_run:
            await write_manifest(workspace_id, manifest, manifest_key)
        return {"well_name": well_name, "status": "empty", "rows": 0, "stages": 0}

    from nextier_core.titanium_layer import run_titanium_layer
    from nextier_utils.constants import DATETIME_COL

    compute = raw.rename(columns={"record_ts": DATETIME_COL}).copy()
    conc_col = CONC_COLUMNS.get(concentration_feature, "prop_conc_blend_auger")
    logger.info(
        "Titanium compute start well=%s rows=%s algorithm_version=%s conc_col=%s ds_callable=nextier_core.titanium_layer.run_titanium_layer",
        well_name,
        len(compute),
        algorithm_version,
        conc_col,
    )
    result = run_titanium_layer(
        compute,
        well=well_name,
        rate_col="rate_slurry",
        pressure_col="press_mainline",
        conc_col=conc_col,
    )
    labeled = raw.copy()
    labeled["titanium_first"] = np.asarray(result.titanium_first, dtype=float)
    labeled["titanium_final"] = np.asarray(result.titanium_final, dtype=float)

    if stage_offset is None:
        offset = await _previous_stage_offset(
            workspace_id=workspace_id,
            stage_index_featurestore_key=stage_index_key,
            well_name=well_name,
            before_ts=effective_start_ts,
            algorithm_version=algorithm_version,
        )
    else:
        offset = max(0, int(stage_offset))
    for col in ["titanium_first", "titanium_final"]:
        mask = labeled[col].fillna(0).astype(float) > 0
        labeled.loc[mask, col] = labeled.loc[mask, col].astype(float) + offset
    labeled["stage_num"] = labeled.apply(_stage_value, axis=1)
    labeled["stage_status"] = np.where(labeled["titanium_final"].fillna(0).astype(float) > 0, "final", np.where(labeled["titanium_first"].fillna(0).astype(float) > 0, "first", None))
    logger.info("Titanium compute complete well=%s offset=%s labeled_rows=%s", well_name, offset, len(labeled))

    # Bounded only on the right (effective_end_ts). Left-bounding here as well
    # would clip a stage's leading rows whenever its true start falls inside
    # the context lookback rather than the window we're actually responsible
    # for, truncating stage_start_ts to the window boundary instead of the
    # stage's real start. write_frame (below) applies the left bound only for
    # what actually gets written as label rows; stage_index is built from this
    # wider frame so a stage that started during context keeps its true start.
    enriched = labeled.copy()
    if effective_end_ts is not None:
        enriched = enriched[pd.to_datetime(enriched["record_ts"], errors="coerce") < effective_end_ts]
    enriched = enriched.rename(columns={"name": "well_name", "id": "well_id"})

    # Prefer the source id for upsert compatibility with raw telemetry. If an
    # id is missing, build a stable per-row key instead of collapsing rows into
    # a shared null/nan key.
    row_key_ts = pd.to_datetime(enriched["record_ts"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    source_ids = enriched["well_id"] if "well_id" in enriched else pd.Series([None] * len(enriched), index=enriched.index)
    enriched["telemetry_point_id"] = np.where(
        source_ids.notna(),
        source_ids.astype(str),
        enriched["well_name"].astype(str) + ":" + row_key_ts.fillna("") + ":" + enriched.index.astype(str),
    )
    enriched["algorithm_version"] = algorithm_version
    enriched["source_mode"] = mode
    enriched["processed_at"] = now_utc()
    label_columns = [
        "telemetry_point_id", "fleet_name", "pad_name", "well_name", "well_id", "api_num", "record_ts", "created_ts",
        "rate_slurry", "press_mainline", "prop_conc_blend_denso", "prop_conc_blend_auger", "prop_conc_inline", "prop_conc_target",
        "titanium_first", "titanium_final", "stage_num", "stage_status", "algorithm_version", "source_mode", "processed_at",
    ]
    enriched = enriched[[c for c in label_columns if c in enriched.columns]]

    write_frame = enriched
    if effective_start_ts is not None:
        write_frame = write_frame[pd.to_datetime(write_frame["record_ts"], errors="coerce") >= effective_start_ts]
    stage_index = _build_stage_index(enriched, run_id, algorithm_version, mode)

    if not dry_run:
        if delete_existing:
            logger.info("Titanium delete start well=%s start=%s end=%s", well_name, format_dt(effective_start_ts), format_dt(effective_end_ts))
            deleted = await _delete_outputs(workspace_id, well_name, effective_start_ts, effective_end_ts, labels_key, stage_index_key)
            logger.info("Titanium delete complete well=%s deleted=%s", well_name, deleted)
        logger.info("Titanium labels write start well=%s rows=%s featurestore=%s", well_name, len(write_frame), labels_key)
        if not write_frame.empty:
            await write_featurestore(
                featurestore_key=labels_key,
                workspace_id=workspace_id,
                df=to_polars_for_write(write_frame),
                upsert=True,
                bulk=True,
            )
        logger.info("Titanium labels write complete well=%s rows=%s", well_name, len(write_frame))
        logger.info("Titanium stage-index write start well=%s rows=%s featurestore=%s", well_name, len(stage_index), stage_index_key)
        if not stage_index.empty:
            await write_featurestore(
                featurestore_key=stage_index_key,
                workspace_id=workspace_id,
                df=to_polars_for_write(stage_index),
                upsert=True,
                bulk=True,
            )
        logger.info("Titanium stage-index write complete well=%s rows=%s", well_name, len(stage_index))
        manifest = make_manifest_row(
            run_id=run_id, workflow_name=WORKFLOW_NAME, mode=mode, source_datastore_key=source_datastore_key,
            well_name=well_name, requested_start_ts=requested_start_ts, requested_end_ts=requested_end_ts,
            effective_start_ts=effective_start_ts, effective_end_ts=effective_end_ts, lookback_hours=context_hours,
            source_df=raw, chunk_hours=chunk_hours, chunk_index=chunk_index, bronze_rows=0, copper_rows=len(write_frame),
            stage_rows=len(stage_index), algorithm_version=algorithm_version, dry_run=dry_run, status="written",
            started_at=started_at, completed_at=format_dt(now_utc()),
        )
        await write_manifest(workspace_id, manifest, manifest_key)

    return {"well_name": well_name, "status": "written", "rows": len(write_frame), "stages": len(stage_index)}


@flow(name="nextier-titanium-stage-orchestration-v1")
async def nextier_titanium_stage_orchestration_v1_flow(
    workspace_id: int,
    workflow_id: int | None = None,
    mode: str = "background",
    source_datastore_key: str = SOURCE_DATASTORE_KEY,
    titanium_labels_featurestore_key: str = TITANIUM_LABELS_FEATURESTORE_KEY,
    titanium_stage_index_featurestore_key: str = TITANIUM_STAGE_INDEX_FEATURESTORE_KEY,
    titanium_manifest_featurestore_key: str = TITANIUM_MANIFEST_FEATURESTORE_KEY,
    well_name: str | None = None,
    well_names: list[str] | None = None,
    fleet_name: str | None = None,
    include_fleet_names: list[str] | None = None,
    exclude_fleet_names: list[str] | None = None,
    pad_name: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    lookback_hours: float = 24,
    context_hours: float = 0,
    boundary_context_hours: float = 0.5,
    max_boundary_expansion_hours: float = 96,
    chunk_hours: float | None = None,
    max_chunks: int | None = None,
    max_wells: int = 25,
    concentration_feature: str = "auger",
    skip_completed: bool = False,
    delete_existing: bool = True,
    dry_run: bool = False,
    algorithm_version: str = DEFAULT_ALGORITHM_VERSION,
) -> dict[str, Any]:
    del workflow_id  # Platform flow contract; Titanium uses manifest for data lineage.
    logger = get_run_logger()
    mode = validate_mode(mode)
    run_id = str(uuid4())
    include_fleets = normalize_name_list(include_fleet_names)
    exclude_fleets = normalize_name_list(exclude_fleet_names)
    if include_fleets and exclude_fleets:
        raise ValueError("include_fleet_names and exclude_fleet_names are mutually exclusive")
    if concentration_feature not in CONC_COLUMNS:
        raise ValueError("concentration_feature must be one of: auger, denso, inline, target")
    boundary_context_hours = max(0.0, float(boundary_context_hours or 0.0))
    max_boundary_expansion_hours = max(0.0, float(max_boundary_expansion_hours or 0.0))

    start_ts, end_ts = resolve_window(mode, start_time, end_time, lookback_hours)
    if mode == "historical" and (start_ts is None or end_ts is None):
        scope_start, scope_end = await get_well_index_scope_bounds(
            workspace_id, source_datastore_key, well_name, well_names, fleet_name, pad_name, include_fleets, exclude_fleets
        )
        if scope_start is None or scope_end is None:
            scope_start, scope_end = await get_raw_scope_bounds(
                workspace_id, source_datastore_key, well_name, well_names, fleet_name, pad_name, include_fleets, exclude_fleets
            )
        start_ts = start_ts or scope_start
        end_ts = end_ts or scope_end

    effective_chunk_hours = chunk_hours if mode == "historical" else None
    chunks = [] if mode == "historical" else build_time_chunks(start_ts, end_ts, None, None)
    if mode != "historical" and not chunks:
        logger.info("Titanium no windows selected mode=%s range=(%s,%s)", mode, format_dt(start_ts), format_dt(end_ts))
        return {"run_id": run_id, "mode": mode, "results": []}

    logger.info(
        "Titanium workflow start mode=%s algorithm_version=%s ds_callable=nextier_core.titanium_layer.run_titanium_layer scope=(well=%s wells=%s fleet=%s include_fleets=%s exclude_fleets=%s pad=%s) range=(%s,%s) chunks=%s max_wells=%s context_hours=%s boundary_context_hours=%s max_boundary_expansion_hours=%s",
        mode, algorithm_version, well_name, well_names, fleet_name, include_fleets or None, exclude_fleets or None, pad_name,
        format_dt(start_ts), format_dt(end_ts), max_chunks if mode == "historical" else len(chunks), max_wells,
        context_hours, boundary_context_hours, max_boundary_expansion_hours,
    )

    results: list[dict[str, Any]] = []
    if mode == "historical":
        if start_ts is None or end_ts is None:
            logger.info("Titanium no historical bounds selected scope=(well=%s wells=%s fleet=%s pad=%s)", well_name, well_names, fleet_name, pad_name)
            return {"run_id": run_id, "mode": mode, "results": []}
        if effective_chunk_hours is None or float(effective_chunk_hours) <= 0:
            raise ValueError("Titanium historical mode requires chunk_hours > 0")

        pending_work = await _select_titanium_historical_pending_work(
            workspace_id=workspace_id,
            source_datastore_key=source_datastore_key,
            manifest_featurestore_key=titanium_manifest_featurestore_key,
            well_name=well_name,
            well_names=well_names,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleets,
            exclude_fleet_names=exclude_fleets,
            start_ts=start_ts,
            end_ts=end_ts,
            chunk_hours=float(effective_chunk_hours),
            max_chunks=max_chunks,
            max_wells=max_wells,
            skip_completed=skip_completed,
            algorithm_version=algorithm_version,
        )
        logger.info(
            "Titanium historical planner selected pending work items=%s max_chunks_per_well=%s max_wells=%s",
            len(pending_work),
            max_chunks,
            max_wells,
        )
        for work_item in pending_work:
            selected_well = str(work_item["well_name"])
            chunk_index = int(work_item["chunk_index"])
            chunk_start = work_item["chunk_start_ts"]
            chunk_end = work_item["chunk_end_ts"]
            logger.info(
                "Titanium historical work selected well=%s chunk_index=%s requested_start=%s requested_end=%s",
                selected_well,
                chunk_index,
                format_dt(chunk_start),
                format_dt(chunk_end),
            )
            context = await get_copper_stage_recompute_context(
                workspace_id=workspace_id,
                stage_index_featurestore_key=titanium_stage_index_featurestore_key,
                well_name=selected_well,
                before_ts=chunk_start,
                after_ts=chunk_end,
                algorithm_version=algorithm_version,
                boundary_context_hours=boundary_context_hours,
                max_boundary_expansion_hours=max_boundary_expansion_hours,
            )
            effective_start = context["recompute_start_ts"]
            effective_end = context["recompute_end_ts"]
            logger.info(
                "Titanium boundary-context resolved well=%s chunk_index=%s requested=(%s,%s) effective=(%s,%s) start_ordinal=%s context=%s left_expansions=%s right_expansions=%s",
                selected_well,
                chunk_index,
                format_dt(chunk_start),
                format_dt(chunk_end),
                format_dt(effective_start),
                format_dt(effective_end),
                context.get("start_ordinal"),
                context.get("context"),
                context.get("boundary_expansions"),
                context.get("right_boundary_expansions"),
            )
            results.append(await _process_well(
                workspace_id=workspace_id,
                run_id=run_id,
                mode=mode,
                source_datastore_key=source_datastore_key,
                labels_key=titanium_labels_featurestore_key,
                stage_index_key=titanium_stage_index_featurestore_key,
                manifest_key=titanium_manifest_featurestore_key,
                well_name=selected_well,
                requested_start_ts=chunk_start,
                requested_end_ts=chunk_end,
                effective_start_ts=effective_start,
                effective_end_ts=effective_end,
                context_hours=float(context_hours or 0),
                chunk_hours=float(effective_chunk_hours),
                chunk_index=chunk_index,
                delete_existing=delete_existing,
                dry_run=dry_run,
                algorithm_version=algorithm_version,
                concentration_feature=concentration_feature,
                stage_offset=int(context.get("start_ordinal") or 1) - 1,
            ))

        logger.info(
            "Titanium workflow complete mode=%s wells=%s rows=%s stages=%s",
            mode,
            len(results),
            sum(r.get('rows', 0) for r in results),
            sum(r.get('stages', 0) for r in results),
        )
        return {"run_id": run_id, "mode": mode, "results": results}

    for chunk_index, chunk_start, chunk_end in chunks:
        explicit_wells = normalize_name_list(well_names)
        if well_name:
            explicit_wells = [well_name, *[name for name in explicit_wells if name != well_name]]
        if explicit_wells:
            selected = explicit_wells[: max_wells or len(explicit_wells)]
        else:
            selected = await select_raw_wells(
                workspace_id=workspace_id,
                source_datastore_key=source_datastore_key,
                manifest_featurestore_key=titanium_manifest_featurestore_key,
                workflow_name=WORKFLOW_NAME,
                mode=mode,
                well_name=well_name,
                fleet_name=fleet_name,
                pad_name=pad_name,
                include_fleet_names=include_fleets,
                exclude_fleet_names=exclude_fleets,
                start_ts=chunk_start,
                end_ts=chunk_end,
                max_wells=max_wells,
                skip_completed=skip_completed,
                algorithm_version=algorithm_version,
            )
        logger.info(
            "Titanium chunk selected index=%s range=(%s,%s) wells=%s",
            chunk_index, format_dt(chunk_start), format_dt(chunk_end), selected,
        )
        for selected_well in selected:
            context = await get_copper_stage_recompute_context(
                workspace_id=workspace_id,
                stage_index_featurestore_key=titanium_stage_index_featurestore_key,
                well_name=selected_well,
                before_ts=chunk_start,
                after_ts=chunk_end,
                algorithm_version=algorithm_version,
                boundary_context_hours=boundary_context_hours,
                max_boundary_expansion_hours=max_boundary_expansion_hours,
            )
            effective_start = context["recompute_start_ts"]
            effective_end = context["recompute_end_ts"]
            logger.info(
                "Titanium boundary-context resolved well=%s chunk_index=%s requested=(%s,%s) effective=(%s,%s) start_ordinal=%s context=%s left_expansions=%s right_expansions=%s",
                selected_well,
                chunk_index,
                format_dt(chunk_start),
                format_dt(chunk_end),
                format_dt(effective_start),
                format_dt(effective_end),
                context.get("start_ordinal"),
                context.get("context"),
                context.get("boundary_expansions"),
                context.get("right_boundary_expansions"),
            )
            results.append(await _process_well(
                workspace_id=workspace_id,
                run_id=run_id,
                mode=mode,
                source_datastore_key=source_datastore_key,
                labels_key=titanium_labels_featurestore_key,
                stage_index_key=titanium_stage_index_featurestore_key,
                manifest_key=titanium_manifest_featurestore_key,
                well_name=selected_well,
                requested_start_ts=chunk_start,
                requested_end_ts=chunk_end,
                effective_start_ts=effective_start,
                effective_end_ts=effective_end,
                context_hours=float(context_hours or 0),
                chunk_hours=chunk_hours,
                chunk_index=chunk_index,
                delete_existing=delete_existing,
                dry_run=dry_run,
                algorithm_version=algorithm_version,
                concentration_feature=concentration_feature,
                stage_offset=int(context.get("start_ordinal") or 1) - 1,
            ))

    logger.info("Titanium workflow complete mode=%s wells=%s rows=%s stages=%s", mode, len(results), sum(r.get('rows', 0) for r in results), sum(r.get('stages', 0) for r in results))
    return {"run_id": run_id, "mode": mode, "results": results}
