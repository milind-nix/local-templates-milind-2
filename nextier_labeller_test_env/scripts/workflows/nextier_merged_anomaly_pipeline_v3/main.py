from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from uuid import uuid4

import pandas as pd
import numpy as np
import polars as pl
from prefect import flow, get_run_logger

from nixdlt.workflow_sdk.platform_tasks import query_store, write_featurestore


# Sapphire's per-stage landmarks are the gate. v2 gated on platinum `design`/`slurry`
# substage labels, which let a mid-stage rate dip close the window early -- so the real
# mid-stage shutdowns were outside the window and never labelled. See
# nextier-dash docs/sapphire_anomaly_gating_handoff.md.
SAPPHIRE_SUMMARY_KEY = "nextier_substage_sapphire_sept3_stage_summary_v2"
SOURCE_DATASTORE_KEY = "merged_fleet_stream_customer_full_v2"
CONC_CHANNEL_KEY = "nextier_well_conc_channel_v1"
LABELS_KEY = "merged_anomaly_labels_v3"
EVENTS_KEY = "merged_anomaly_events_v3"
STAGE_SUMMARY_KEY = "merged_anomaly_stage_summary_v3"
MANIFEST_KEY = "merged_anomaly_processing_manifest_v3"

DEFAULT_CONC_COL = "prop_conc_blend_denso"
TELEMETRY_COLS = (
    "rate_slurry",
    "press_mainline",
    "prop_conc_blend_auger",
    "prop_conc_target",
    "prop_conc_blend_denso",
    "prop_conc_inline",
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")


def _as_pandas(frame: Any) -> pd.DataFrame:
    if isinstance(frame, pl.DataFrame):
        return frame.to_pandas() if not frame.is_empty() else pd.DataFrame()
    return frame if isinstance(frame, pd.DataFrame) else pd.DataFrame(frame)


def _source_signature(row: dict[str, Any], algorithm_version: str) -> str:
    # label_row_count + processed_at come from the sapphire summary, so a sapphire
    # re-run of a stage invalidates that stage here and nothing else does.
    payload = "|".join(
        str(row.get(key) or "")
        for key in (
            "name",
            "stage_num",
            "source_rows",
            "first_source_ts",
            "last_source_ts",
            "latest_source_updated_at",
            "gate_start_ts",
            "gate_end_ts",
        )
    )
    return sha256(f"{algorithm_version}|{payload}".encode()).hexdigest()


async def _candidate_stages(
    workspace_id: int,
    algorithm_version: str,
    mode: str,
    max_stages: int,
    well_name: str | None,
    stage_num: float | None,
) -> list[dict[str, Any]]:
    force = mode == "rebuild"
    rows = await query_store(
        sql=f"""
            WITH source_stages AS (
              SELECT
                s.well_name AS name,
                CAST(s.stage_num AS DOUBLE PRECISION) AS stage_num,
                COALESCE(NULLIF(MAX(w.fleet_name), ''), NULLIF(MAX(s.fleet_name), ''), 'Unknown') AS fleet_name,
                COALESCE(NULLIF(MAX(w.pad_name), ''), NULLIF(MAX(s.pad_name), ''), 'Unknown') AS pad_name,
                COALESCE(MAX(CAST(s.label_row_count AS BIGINT)), 0) AS source_rows,
                MIN(CAST(s.stage_start_ts AS timestamp)) AS first_source_ts,
                MAX(CAST(s.stage_end_ts AS timestamp)) AS last_source_ts,
                MAX(CAST(s.processed_at AS timestamp)) AS latest_source_updated_at,
                MAX(CAST(s.ttr_ts AS timestamp)) AS gate_start_ts,
                MAX(CAST(s.rampdown_start_ts AS timestamp)) AS gate_end_ts
              FROM featurestore:{SAPPHIRE_SUMMARY_KEY} s
              LEFT JOIN featurestore:merged_well_index_v1 w
                ON w.name = s.well_name
              WHERE s.well_name IS NOT NULL
                AND s.stage_num IS NOT NULL
                AND s.stage_start_ts IS NOT NULL
                AND s.stage_end_ts IS NOT NULL
                AND (:well_name = '' OR s.well_name = :well_name)
                AND (:stage_num < 0 OR CAST(s.stage_num AS DOUBLE PRECISION) = :stage_num)
              GROUP BY s.well_name, CAST(s.stage_num AS DOUBLE PRECISION)
            )
            SELECT s.*
            FROM source_stages s
            LEFT JOIN featurestore:{MANIFEST_KEY} m
              ON m.name = s.name
             AND m.stage_num = s.stage_num
             AND m.algorithm_version = :algorithm_version
            WHERE :force_rebuild
               OR m.name IS NULL
               OR m.source_rows IS DISTINCT FROM s.source_rows
               OR CAST(m.latest_source_updated_at AS timestamp)
                    IS DISTINCT FROM s.latest_source_updated_at
            ORDER BY s.name, s.stage_num
            LIMIT {int(max_stages)}
        """,
        workspace_id=workspace_id,
        params={
            "algorithm_version": algorithm_version,
            "force_rebuild": force,
            "well_name": (well_name or "").strip(),
            "stage_num": float(stage_num) if stage_num is not None else -1.0,
        },
    )
    return rows.to_dicts() if not rows.is_empty() else []


async def _conc_col_for(workspace_id: int, name: str) -> str:
    """The channel the auto-selector chose for this well.

    Sweeps are read off the concentration trace, so reading a dead channel produces
    no sweep markers at all. v2 hardcoded denso, which on this fleet is the pick for
    exactly zero wells.
    """
    frame = await query_store(
        sql=f"""
            SELECT conc_col FROM featurestore:{CONC_CHANNEL_KEY}
            WHERE well_name = :name LIMIT 1
        """,
        workspace_id=workspace_id,
        params={"name": name},
    )
    df = _as_pandas(frame)
    if df.empty or not df["conc_col"].notna().any():
        return DEFAULT_CONC_COL
    return str(df["conc_col"].iloc[0]) or DEFAULT_CONC_COL


async def _load_well(
    workspace_id: int, name: str, start_ts: Any, end_ts: Any
) -> pd.DataFrame:
    """One read per WELL, not per stage.

    v2 issued a query per stage -- 19k round trips for a fleet pass. The well is also
    the unit the titanium merge needs, so loading it once serves both.
    """
    cols = ", ".join(TELEMETRY_COLS)
    frame = await query_store(
        sql=f"""
            SELECT record_ts AS datetime_fmt, {cols}
            FROM datastore:{SOURCE_DATASTORE_KEY}
            WHERE name = :name
              AND record_ts >= :start_ts
              AND record_ts <= :end_ts
            ORDER BY record_ts
        """,
        workspace_id=workspace_id,
        params={"name": name, "start_ts": start_ts, "end_ts": end_ts},
    )
    df = _as_pandas(frame)
    if df.empty:
        return df
    df["datetime_fmt"] = pd.to_datetime(df["datetime_fmt"], errors="coerce")
    return df.dropna(subset=["datetime_fmt"]).sort_values("datetime_fmt").reset_index(drop=True)


def _join_times(well: str, telem: pd.DataFrame, logger: Any) -> list[Any]:
    """Titanium sand-mass merge-join fire times, the PREFERRED mid-shutdown anchor.

    Optional by design: the rate sandwich alone still finds mid shutdowns, so a failure
    here degrades quality rather than the run. Imported inside the function because the
    package is installed into the job image -- a version mismatch should fail this well,
    not stop the flow importing.
    """
    try:
        from nextier_utils.labeling.sapphire.stages import resolve_coarse_stages

        merged = resolve_coarse_stages(well, telem)
        return list(merged.get("joins") or [])
    except Exception:  # noqa: BLE001 -- joins are an optimisation, not the answer
        logger.exception("Anomaly join resolve failed well=%s -- sandwich only", well)
        return []


def _gate_indices(
    slice_df: pd.DataFrame, gate_start: Any, gate_end: Any
) -> tuple[int | None, int | None]:
    if slice_df.empty:
        return None, None
    ts = slice_df["datetime_fmt"].to_numpy()

    def _idx(value: Any) -> int | None:
        if value is None or pd.isna(value):
            return None
        pos = int(np.searchsorted(ts, np.datetime64(pd.Timestamp(value))))
        return pos if 0 <= pos < len(ts) else None

    return _idx(gate_start), _idx(gate_end)


def _prepare_outputs(
    slice_df: pd.DataFrame,
    candidate: dict[str, Any],
    *,
    conc_col: str,
    joins: list[Any],
    algorithm_version: str,
    pressure_threshold_psi: float,
    rate_drop_threshold_bpm: float,
    rolling_points: int,
    max_gap_seconds: float,
) -> dict[str, Any]:
    from nextier_core.anomaly_detection import (
        combine_mid_stage_shutdown_masks,
        design_mask_between_ttr_and_rampdown,
        detect_design_delta_anomalies,
        detect_mid_stage_shutdowns,
        detect_mid_stage_shutdowns_from_joins,
        detect_sweep_markers_around_mid_stage_shutdowns,
        require_anomaly_min_length,
    )
    from nextier_utils.common.segment_utils import build_contiguous_segments

    name = str(candidate["name"])
    stage = float(candidate["stage_num"])
    fleet_name = str(candidate.get("fleet_name") or "Unknown")
    pad_name = str(candidate.get("pad_name") or "Unknown")
    run_id = str(uuid4())
    created_at = _now()
    signature = _source_signature(candidate, algorithm_version)

    gate_start = candidate.get("gate_start_ts")
    gate_end = candidate.get("gate_end_ts")

    labels = slice_df.reset_index(drop=True).copy()
    if labels.empty:
        gate_status = "no_telemetry"
    elif gate_start is None or pd.isna(gate_start):
        gate_status = "no_ttr"
    elif gate_end is None or pd.isna(gate_end):
        gate_status = "no_rampdown"
    else:
        gate_status = "ok"

    events = pd.DataFrame()
    flagged = pd.DataFrame(columns=["name", "stage_num", "datetime_fmt", "anomaly"])

    if gate_status == "ok":
        ttr_idx, rd_idx = _gate_indices(labels, gate_start, gate_end)
        design_mask = design_mask_between_ttr_and_rampdown(len(labels), ttr_idx, rd_idx)
        if not design_mask.any():
            gate_status = "no_rampdown" if rd_idx is None else "no_ttr"

    if gate_status == "ok":
        sandwich = require_anomaly_min_length(detect_mid_stage_shutdowns(labels))
        if joins:
            sandwich = combine_mid_stage_shutdown_masks(
                sandwich,
                require_anomaly_min_length(
                    detect_mid_stage_shutdowns_from_joins(labels, joins)
                ),
            )
        shutdown_mask = sandwich & design_mask
        sweep_pre_mask, sweep_post_mask = detect_sweep_markers_around_mid_stage_shutdowns(
            labels, shutdown_mask, conc_col=conc_col,
        )
        sweep_pre_mask &= design_mask
        sweep_post_mask &= design_mask
        pressure_mask, rate_mask, _, _ = detect_design_delta_anomalies(
            labels,
            design_mask=design_mask,
            pressure_threshold_psi=pressure_threshold_psi,
            rate_drop_threshold_bpm=rate_drop_threshold_bpm,
            rolling_points=rolling_points,
        )
        anomaly = np.full(len(labels), None, dtype=object)
        # Priority order is the handoff's: first match wins, one label per row.
        for mask, value in (
            (shutdown_mask, "mid_stage_shutdown"),
            (sweep_pre_mask, "sweep_pre"),
            (sweep_post_mask, "sweep_post"),
            (rate_mask, "pump_rate_drop"),
            (pressure_mask, "pressure_surge"),
        ):
            assign = np.asarray(mask, dtype=bool) & pd.isna(anomaly)
            anomaly[assign] = value
        labels["anomaly"] = anomaly
        labels["name"] = name
        labels["stage_num"] = stage
        flagged = labels.loc[
            labels["anomaly"].notna(), ["name", "stage_num", "datetime_fmt", "anomaly"]
        ].copy()

        if not flagged.empty:
            events = build_contiguous_segments(
                flagged,
                label_col="anomaly",
                datetime_col="datetime_fmt",
                well_col="name",
                stage_col="stage_num",
                max_gap_s=max_gap_seconds,
            ).rename(columns={"label": "anomaly"})

    if not events.empty:
        events["anomaly"] = events["anomaly"].str.lower()
        events["anomaly_display"] = (
            events["anomaly"].str.replace("_", " ", regex=False).str.title()
        )
        events = events[[
            "name", "stage_num", "anomaly", "anomaly_display",
            "start_ts", "end_ts", "duration_sec", "duration_min",
        ]]

    for frame in (flagged, events):
        if not frame.empty:
            frame["fleet_name"] = fleet_name
            frame["pad_name"] = pad_name
            frame["processing_run_id"] = run_id
            frame["algorithm_version"] = algorithm_version
            frame["source_signature"] = signature
            frame["created_at"] = created_at

    if not events.empty:
        events["event_id"] = events.apply(
            lambda row: sha256(
                f"{run_id}|{row['name']}|{row['stage_num']}|"
                f"{row['anomaly']}|{row['start_ts']}|{row['end_ts']}".encode()
            ).hexdigest(),
            axis=1,
        )

    counts = events["anomaly"].value_counts() if not events.empty else pd.Series(dtype=int)
    stage_summary = pd.DataFrame([{
        "fleet_name": fleet_name,
        "pad_name": pad_name,
        "name": name,
        "stage_num": stage,
        "stage_start_ts": candidate.get("first_source_ts"),
        "stage_end_ts": candidate.get("last_source_ts"),
        # The gate is recorded even when it is absent: "no anomalies" and "never
        # looked" are different answers and the dashboard has to tell them apart.
        "gate_start_ts": gate_start,
        "gate_end_ts": gate_end,
        "gate_status": gate_status,
        "source_rows": int(candidate.get("source_rows") or 0),
        "total_anomaly_lines": int(len(events)),
        "pressure_surge_lines": int(counts.get("pressure_surge", 0)),
        "pump_rate_drop_lines": int(counts.get("pump_rate_drop", 0)),
        "mid_stage_shutdown_lines": int(counts.get("mid_stage_shutdown", 0)),
        "sweep_lines": int(counts.get("sweep_pre", 0) + counts.get("sweep_post", 0)),
        "processing_run_id": run_id,
        "algorithm_version": algorithm_version,
        "source_signature": signature,
        "created_at": created_at,
    }])

    manifest = pd.DataFrame([{
        "fleet_name": fleet_name,
        "pad_name": pad_name,
        "name": name,
        "stage_num": stage,
        "algorithm_version": algorithm_version,
        "active_run_id": run_id,
        "source_signature": signature,
        "source_rows": int(candidate.get("source_rows") or 0),
        "first_source_ts": candidate.get("first_source_ts"),
        "last_source_ts": candidate.get("last_source_ts"),
        "latest_source_updated_at": candidate.get("latest_source_updated_at"),
        "processed_at": created_at,
        "status": "active",
    }])
    return {
        "gate_status": gate_status,
        LABELS_KEY: pl.from_pandas(flagged) if not flagged.empty else pl.DataFrame(),
        EVENTS_KEY: pl.from_pandas(events) if not events.empty else pl.DataFrame(),
        STAGE_SUMMARY_KEY: pl.from_pandas(stage_summary),
        MANIFEST_KEY: pl.from_pandas(manifest),
    }


@flow(name="nextier-merged-anomaly-pipeline-v3")
async def nextier_merged_anomaly_pipeline_v3_flow(
    workspace_id: int,
    workflow_id: int | None = None,
    mode: str = "incremental",
    max_stages: int = 25,
    well_name: str | None = None,
    stage_num: float | None = None,
    algorithm_version: str = "sapphire-gate-v1",
    pressure_threshold_psi: float = 200.0,
    rate_drop_threshold_bpm: float = 5.0,
    rolling_points: int = 1,
    max_gap_seconds: float = 120.0,
    use_joins: bool = True,
    dry_run: bool = False,
):
    del workflow_id  # Platform flow contract -- injected on every run, unused here.
    if mode not in {"incremental", "historical", "rebuild"}:
        raise ValueError("mode must be incremental, historical, or rebuild")
    logger = get_run_logger()
    candidates = await _candidate_stages(
        workspace_id, algorithm_version, mode, max_stages, well_name, stage_num,
    )
    logger.info(
        "Anomaly v3 start mode=%s gate=sapphire(ttr,rampdown] candidates=%s "
        "joins=%s algorithm_version=%s",
        mode, len(candidates), use_joins, algorithm_version,
    )

    by_well: dict[str, list[dict[str, Any]]] = {}
    for row in candidates:
        by_well.setdefault(str(row["name"]), []).append(row)

    results: list[dict[str, Any]] = []
    gate_tally: dict[str, int] = {}
    for name, stages in by_well.items():
        starts = [s["first_source_ts"] for s in stages if s.get("first_source_ts")]
        ends = [s["last_source_ts"] for s in stages if s.get("last_source_ts")]
        if not starts or not ends:
            continue
        telem = await _load_well(workspace_id, name, min(starts), max(ends))
        if telem.empty:
            logger.warning("Anomaly v3 no telemetry well=%s stages=%s", name, len(stages))
        conc_col = await _conc_col_for(workspace_id, name)
        if conc_col not in telem.columns:
            conc_col = DEFAULT_CONC_COL
        joins = (
            await asyncio.to_thread(_join_times, name, telem, logger)
            if use_joins and not telem.empty else []
        )
        logger.info(
            "Anomaly v3 well=%s stages=%s rows=%s conc_col=%s joins=%s",
            name, len(stages), len(telem), conc_col, len(joins),
        )

        for candidate in stages:
            t0 = candidate.get("first_source_ts")
            t1 = candidate.get("last_source_ts")
            if telem.empty:
                slice_df = telem
            else:
                m = (telem["datetime_fmt"] >= pd.Timestamp(t0)) & (
                    telem["datetime_fmt"] <= pd.Timestamp(t1))
                slice_df = telem.loc[m]
            outputs = await asyncio.to_thread(
                _prepare_outputs,
                slice_df, candidate,
                conc_col=conc_col, joins=joins,
                algorithm_version=algorithm_version,
                pressure_threshold_psi=pressure_threshold_psi,
                rate_drop_threshold_bpm=rate_drop_threshold_bpm,
                rolling_points=rolling_points,
                max_gap_seconds=max_gap_seconds,
            )
            status = outputs.pop("gate_status")
            gate_tally[status] = gate_tally.get(status, 0) + 1
            counts = {k: len(v) for k, v in outputs.items()}
            if not dry_run:
                # Manifest last: a run becomes visible only once every immutable
                # output for it has landed.
                for key in (LABELS_KEY, EVENTS_KEY, STAGE_SUMMARY_KEY, MANIFEST_KEY):
                    frame = outputs[key]
                    if frame.is_empty():
                        continue
                    await write_featurestore(
                        featurestore_key=key,
                        workspace_id=workspace_id,
                        df=frame,
                        upsert=(key == MANIFEST_KEY),
                    )
            results.append({
                "name": name, "stage_num": candidate["stage_num"],
                "gate_status": status, "counts": counts,
            })

    has_more = len(candidates) == int(max_stages)
    logger.info(
        "Anomaly v3 complete mode=%s wells=%s stages=%s gate=%s has_more=%s",
        mode, len(by_well), len(results), gate_tally, has_more,
    )
    return {
        "mode": mode,
        "algorithm_version": algorithm_version,
        "dry_run": dry_run,
        "wells_processed": len(by_well),
        "stages_processed": len(results),
        "gate_status_counts": gate_tally,
        "has_more": has_more,
    }
