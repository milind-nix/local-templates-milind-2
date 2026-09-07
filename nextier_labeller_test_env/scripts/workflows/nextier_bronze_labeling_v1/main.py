from __future__ import annotations

from pathlib import Path
import sys

TEMPLATE_ROOT = Path(__file__).resolve().parents[3]
if str(TEMPLATE_ROOT) not in sys.path:
    sys.path.insert(0, str(TEMPLATE_ROOT))

from uuid import uuid4

import pandas as pd
from prefect import flow, get_run_logger

from nixdlt.workflow_sdk.platform_tasks import write_featurestore
from scripts.workflows.nextier_labeling_common_v1.common import (
    BRONZE_LABELS_FEATURESTORE_KEY,
    COPPER_STAGE_INDEX_FEATURESTORE_KEY,
    DEFAULT_ALGORITHM_VERSION,
    MANIFEST_FEATURESTORE_KEY,
    SOURCE_DATASTORE_KEY,
    compute_bronze_labels,
    format_dt,
    get_copper_stage_recompute_context,
    load_raw_window,
    make_manifest_row,
    now_utc,
    parse_dt,
    prepare_raw_frame,
    resolve_window,
    select_raw_wells,
    to_polars_for_write,
    validate_mode,
    write_manifest,
)

WORKFLOW_NAME = "nextier_bronze_labeling_v1"


async def _process_one_well(
    *,
    workspace_id: int,
    run_id: str,
    mode: str,
    source_datastore_key: str,
    bronze_featurestore_key: str,
    manifest_featurestore_key: str,
    stage_index_featurestore_key: str | None,
    well_name: str,
    requested_start_ts,
    requested_end_ts,
    effective_start_ts,
    effective_end_ts,
    lookback_hours: float,
    boundary_context_hours: float,
    max_boundary_expansion_hours: float,
    delete_existing: bool,
    dry_run: bool,
    algorithm_version: str,
    chunk_hours: float | None = None,
    chunk_index: int | None = None,
    return_frame: bool = False,
) -> dict:
    logger = get_run_logger()
    started_at = format_dt(now_utc())
    bounded_start_ts = effective_start_ts
    if bounded_start_ts is not None and float(lookback_hours or 0) > 0:
        bounded_start_ts = bounded_start_ts - pd.Timedelta(hours=float(lookback_hours or 0))

    # Bronze is the direct input to copper. If the fixed chunk/context start
    # lands in the middle of an already served copper stage, recomputing bronze
    # only from that point would leave stale bronze rows at the stage boundary.
    # Resolve the same stage-safe start before reading raw telemetry so bronze
    # and copper are rebuilt over one identical effective window.
    if stage_index_featurestore_key and bounded_start_ts is not None:
        logger.info(
            "Bronze boundary-context resolve start well=%s stage_index_featurestore=%s before=%s algorithm_version=%s",
            well_name,
            stage_index_featurestore_key,
            format_dt(bounded_start_ts),
            algorithm_version,
        )
        boundary_context = await get_copper_stage_recompute_context(
            workspace_id=workspace_id,
            stage_index_featurestore_key=stage_index_featurestore_key,
            well_name=well_name,
            before_ts=bounded_start_ts,
            after_ts=effective_end_ts,
            algorithm_version=algorithm_version,
            boundary_context_hours=boundary_context_hours,
            max_boundary_expansion_hours=max_boundary_expansion_hours,
        )
        bounded_start_ts = boundary_context["recompute_start_ts"]
        effective_end_ts = boundary_context["recompute_end_ts"]
        logger.info(
            "Bronze boundary-context resolve complete well=%s requested_start=%s requested_end=%s safe_start=%s safe_end=%s context=%s left_expansions=%s left_stage_nums=%s right_expansions=%s right_stage_nums=%s",
            well_name,
            format_dt(boundary_context.get("requested_recompute_start_ts")),
            format_dt(boundary_context.get("requested_recompute_end_ts")),
            format_dt(bounded_start_ts),
            format_dt(effective_end_ts),
            boundary_context.get("context"),
            boundary_context.get("boundary_expansions", 0),
            boundary_context.get("boundary_stage_nums", []),
            boundary_context.get("right_boundary_expansions", 0),
            boundary_context.get("right_boundary_stage_nums", []),
        )

    logger.info(
        "Bronze raw load start well=%s datastore=%s start=%s end=%s",
        well_name,
        source_datastore_key,
        format_dt(bounded_start_ts),
        format_dt(effective_end_ts),
    )
    raw_pl = await load_raw_window(
        workspace_id=workspace_id,
        source_datastore_key=source_datastore_key,
        well_name=well_name,
        start_ts=bounded_start_ts,
        end_ts=effective_end_ts,
    )
    raw_pd = prepare_raw_frame(raw_pl)
    logger.info("Bronze raw load complete well=%s rows=%s", well_name, len(raw_pd))
    if raw_pd.empty:
        completed_at = format_dt(now_utc())
        manifest = make_manifest_row(
            run_id=run_id,
            workflow_name=WORKFLOW_NAME,
            mode=mode,
            source_datastore_key=source_datastore_key,
            well_name=well_name,
            requested_start_ts=requested_start_ts,
            requested_end_ts=requested_end_ts,
            effective_start_ts=bounded_start_ts,
            effective_end_ts=effective_end_ts,
            lookback_hours=lookback_hours,
            source_df=raw_pd,
            chunk_hours=chunk_hours,
            chunk_index=chunk_index,
            bronze_rows=0,
            copper_rows=0,
            stage_rows=0,
            algorithm_version=algorithm_version,
            dry_run=dry_run,
            status="skipped_no_raw_rows",
            started_at=started_at,
            completed_at=completed_at,
        )
        if not dry_run:
            await write_manifest(workspace_id, manifest, manifest_featurestore_key)
        return {"well_name": well_name, "status": "skipped_no_raw_rows", "manifest": manifest}

    processed_at = format_dt(now_utc())
    logger.info("Bronze compute start well=%s rows=%s algorithm_version=%s", well_name, len(raw_pd), algorithm_version)
    bronze_df = compute_bronze_labels(
        raw_pl,
        mode=mode,
        algorithm_version=algorithm_version,
        processed_at=processed_at,
    )
    logger.info("Bronze compute complete well=%s bronze_rows=%s", well_name, len(bronze_df))

    logger.info(
        "Bronze plan well=%s mode=%s dry_run=%s recompute_start=%s cutoff=%s raw_rows=%s bronze_rows=%s",
        well_name,
        mode,
        dry_run,
        format_dt(bounded_start_ts),
        format_dt(effective_end_ts),
        len(raw_pd),
        len(bronze_df),
    )

    deleted = None
    if not dry_run:
        if delete_existing:
            logger.info(
                "Bronze label delete skipped well=%s cutoff=%s reason=upsert_replaces_recomputed_rows",
                well_name,
                format_dt(effective_end_ts),
            )
        if not bronze_df.empty:
            logger.info("Bronze write start well=%s featurestore=%s rows=%s mode=upsert", well_name, bronze_featurestore_key, len(bronze_df))
            await write_featurestore(
                featurestore_key=bronze_featurestore_key,
                workspace_id=workspace_id,
                df=to_polars_for_write(bronze_df),
                upsert=True,
            )
            logger.info(
                "Bronze write complete well=%s featurestore=%s rows=%s mode=upsert",
                well_name,
                bronze_featurestore_key,
                len(bronze_df),
            )

    completed_at = format_dt(now_utc())
    manifest = make_manifest_row(
        run_id=run_id,
        workflow_name=WORKFLOW_NAME,
        mode=mode,
        source_datastore_key=source_datastore_key,
        well_name=well_name,
        requested_start_ts=requested_start_ts,
        requested_end_ts=requested_end_ts,
        effective_start_ts=bounded_start_ts,
        effective_end_ts=effective_end_ts,
        lookback_hours=lookback_hours,
        source_df=raw_pd,
        chunk_hours=chunk_hours,
        chunk_index=chunk_index,
        bronze_rows=len(bronze_df),
        copper_rows=0,
        stage_rows=0,
        algorithm_version=algorithm_version,
        dry_run=dry_run,
        status="dry_run" if dry_run else "written",
        started_at=started_at,
        completed_at=completed_at,
    )
    if not dry_run:
        logger.info("Bronze manifest write start well=%s featurestore=%s", well_name, manifest_featurestore_key)
        await write_manifest(workspace_id, manifest, manifest_featurestore_key)
        logger.info("Bronze manifest written well=%s status=%s requested=(%s,%s) effective=(%s,%s) rows=%s", well_name, manifest["status"], manifest["requested_start_ts"], manifest["requested_end_ts"], manifest["effective_start_ts"], manifest["effective_end_ts"], manifest["rows_bronze_written"])

    logger.info("Bronze complete well=%s status=%s raw_rows=%s bronze_rows=%s", well_name, "dry_run" if dry_run else "written", len(raw_pd), len(bronze_df))

    result = {
        "well_name": well_name,
        "status": "dry_run" if dry_run else "written",
        "raw_rows": len(raw_pd),
        "bronze_rows": len(bronze_df),
        "deleted": deleted,
        "manifest": manifest,
    }
    if return_frame:
        result["_bronze_frame"] = bronze_df
    return result


@flow(name="nextier-bronze-labeling-v1")
async def nextier_bronze_labeling_v1_flow(
    workspace_id: int,
    workflow_id: int,
    mode: str = "background",
    source_datastore_key: str = SOURCE_DATASTORE_KEY,
    bronze_featurestore_key: str = BRONZE_LABELS_FEATURESTORE_KEY,
    manifest_featurestore_key: str = MANIFEST_FEATURESTORE_KEY,
    stage_index_featurestore_key: str | None = COPPER_STAGE_INDEX_FEATURESTORE_KEY,
    well_name: str | None = None,
    fleet_name: str | None = None,
    pad_name: str | None = None,
    include_fleet_names: list[str] | None = None,
    exclude_fleet_names: list[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    lookback_hours: float = 72,
    boundary_context_hours: float = 0.5,
    max_boundary_expansion_hours: float = 96,
    max_wells: int = 5,
    skip_completed: bool = False,
    chunk_hours: float | None = None,
    chunk_index: int | None = None,
    delete_existing: bool = True,
    dry_run: bool = True,
    algorithm_version: str = DEFAULT_ALGORITHM_VERSION,
    return_frames: bool = False,
):
    return await run_nextier_bronze_labeling_v1(
        workspace_id=workspace_id,
        mode=mode,
        source_datastore_key=source_datastore_key,
        bronze_featurestore_key=bronze_featurestore_key,
        manifest_featurestore_key=manifest_featurestore_key,
        stage_index_featurestore_key=stage_index_featurestore_key,
        well_name=well_name,
        fleet_name=fleet_name,
        pad_name=pad_name,
        include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
        start_time=start_time,
        end_time=end_time,
        lookback_hours=lookback_hours,
        boundary_context_hours=boundary_context_hours,
        max_boundary_expansion_hours=max_boundary_expansion_hours,
        max_wells=max_wells,
        skip_completed=skip_completed,
        chunk_hours=chunk_hours,
        chunk_index=chunk_index,
        delete_existing=delete_existing,
        dry_run=dry_run,
        algorithm_version=algorithm_version,
        return_frames=return_frames,
    )


async def run_nextier_bronze_labeling_v1(
    *,
    workspace_id: int,
    mode: str = "background",
    source_datastore_key: str = SOURCE_DATASTORE_KEY,
    bronze_featurestore_key: str = BRONZE_LABELS_FEATURESTORE_KEY,
    manifest_featurestore_key: str = MANIFEST_FEATURESTORE_KEY,
    stage_index_featurestore_key: str | None = COPPER_STAGE_INDEX_FEATURESTORE_KEY,
    well_name: str | None = None,
    fleet_name: str | None = None,
    pad_name: str | None = None,
    include_fleet_names: list[str] | None = None,
    exclude_fleet_names: list[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    lookback_hours: float = 72,
    boundary_context_hours: float = 0.5,
    max_boundary_expansion_hours: float = 96,
    max_wells: int = 5,
    skip_completed: bool = False,
    chunk_hours: float | None = None,
    chunk_index: int | None = None,
    delete_existing: bool = True,
    dry_run: bool = True,
    algorithm_version: str = DEFAULT_ALGORITHM_VERSION,
    return_frames: bool = False,
):
    logger = get_run_logger()
    mode = validate_mode(mode)
    source_datastore_key = source_datastore_key or SOURCE_DATASTORE_KEY
    bronze_featurestore_key = bronze_featurestore_key or BRONZE_LABELS_FEATURESTORE_KEY
    manifest_featurestore_key = manifest_featurestore_key or MANIFEST_FEATURESTORE_KEY
    algorithm_version = algorithm_version or DEFAULT_ALGORITHM_VERSION
    stage_index_featurestore_key = stage_index_featurestore_key or COPPER_STAGE_INDEX_FEATURESTORE_KEY
    lookback_hours = float(lookback_hours or 0)
    boundary_context_hours = max(0.0, float(boundary_context_hours or 0.0))
    max_boundary_expansion_hours = max(0.0, float(max_boundary_expansion_hours or 0.0))
    max_wells = int(max_wells or 5)
    requested_start_ts = parse_dt(start_time)
    requested_end_ts = parse_dt(end_time)
    effective_start_ts, effective_end_ts = resolve_window(mode, start_time, end_time, float(lookback_hours or 0))
    explicit_planned_well = bool(well_name and str(well_name).strip() and chunk_index is not None)
    if explicit_planned_well:
        wells = [str(well_name).strip()]
        logger.info(
            "Using explicit planned bronze well=%s chunk_index=%s; skipping raw well-selection query",
            wells[0],
            chunk_index,
        )
    else:
        wells = await select_raw_wells(
            workspace_id=workspace_id,
            source_datastore_key=source_datastore_key,
            manifest_featurestore_key=manifest_featurestore_key,
            workflow_name=WORKFLOW_NAME,
            mode=mode,
            well_name=well_name,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
            start_ts=effective_start_ts,
            end_ts=effective_end_ts,
            max_wells=int(max_wells),
            skip_completed=bool(skip_completed),
            algorithm_version=algorithm_version,
        )
    if not wells:
        logger.info("No wells selected for bronze labeling mode=%s requested_range=(%s,%s) scope=(well=%s fleet=%s pad=%s)", mode, format_dt(effective_start_ts), format_dt(effective_end_ts), well_name, fleet_name, pad_name)
        return {
            "wells_processed": 0,
            "mode": mode,
            "dry_run": bool(dry_run),
            "effective_start_ts": format_dt(effective_start_ts),
            "effective_end_ts": format_dt(effective_end_ts),
            "results": [],
        }

    logger.info(
        "Selected %s wells for bronze labeling mode=%s requested_range=(%s,%s) cutoff=%s wells=%s",
        len(wells),
        mode,
        format_dt(effective_start_ts),
        format_dt(effective_end_ts),
        format_dt(effective_end_ts),
        wells,
    )

    run_id = str(uuid4())
    results = []
    bronze_frames: dict[str, pd.DataFrame] = {}
    for selected_well in wells:
        try:
            result = await _process_one_well(
                workspace_id=workspace_id,
                run_id=run_id,
                mode=mode,
                source_datastore_key=source_datastore_key,
                bronze_featurestore_key=bronze_featurestore_key,
                manifest_featurestore_key=manifest_featurestore_key,
                stage_index_featurestore_key=stage_index_featurestore_key,
                well_name=selected_well,
                requested_start_ts=requested_start_ts,
                requested_end_ts=requested_end_ts,
                effective_start_ts=effective_start_ts,
                effective_end_ts=effective_end_ts,
                lookback_hours=float(lookback_hours or 0),
                boundary_context_hours=boundary_context_hours,
                max_boundary_expansion_hours=max_boundary_expansion_hours,
                delete_existing=delete_existing,
                dry_run=dry_run,
                algorithm_version=algorithm_version,
                chunk_hours=chunk_hours,
                chunk_index=chunk_index,
                return_frame=return_frames,
            )
            if return_frames and result.get("status") in {"written", "dry_run"}:
                bronze_frame = result.pop("_bronze_frame", None)
                if isinstance(bronze_frame, pd.DataFrame) and not bronze_frame.empty:
                    bronze_frames[selected_well] = bronze_frame
            results.append(result)
        except Exception as exc:
            logger.exception("Bronze labeling failed for well=%s", selected_well)
            if not dry_run:
                manifest = make_manifest_row(
                    run_id=run_id,
                    workflow_name=WORKFLOW_NAME,
                    mode=mode,
                    source_datastore_key=source_datastore_key,
                    well_name=selected_well,
                    requested_start_ts=requested_start_ts,
                    requested_end_ts=requested_end_ts,
                    effective_start_ts=effective_start_ts,
                    effective_end_ts=effective_end_ts,
                    lookback_hours=float(lookback_hours or 0),
                    source_df=pd.DataFrame(),
                    chunk_hours=chunk_hours,
                    chunk_index=chunk_index,
                    bronze_rows=0,
                    copper_rows=0,
                    stage_rows=0,
                    algorithm_version=algorithm_version,
                    dry_run=dry_run,
                    status="failed",
                    started_at=format_dt(now_utc()),
                    completed_at=format_dt(now_utc()),
                    error_message=str(exc),
                )
                await write_manifest(workspace_id, manifest, manifest_featurestore_key)
            raise

    out = {
        "run_id": run_id,
        "wells_processed": len(results),
        "mode": mode,
        "dry_run": bool(dry_run),
        "effective_start_ts": format_dt(effective_start_ts),
        "effective_end_ts": format_dt(effective_end_ts),
        "results": results,
    }
    if return_frames:
        out["_bronze_frames_by_well"] = bronze_frames
    return out
