from __future__ import annotations

from pathlib import Path
import sys

TEMPLATE_ROOT = Path(__file__).resolve().parents[3]
if str(TEMPLATE_ROOT) not in sys.path:
    sys.path.insert(0, str(TEMPLATE_ROOT))

from uuid import uuid4

import pandas as pd
from nixdlt.workflow_sdk.platform_tasks import write_featurestore
from prefect import flow, get_run_logger
from scripts.workflows.nextier_labeling_common_v1.common import (
    BRONZE_LABELS_FEATURESTORE_KEY,
    COPPER_LABELS_FEATURESTORE_KEY,
    COPPER_STAGE_INDEX_FEATURESTORE_KEY,
    DEFAULT_ALGORITHM_VERSION,
    MANIFEST_FEATURESTORE_KEY,
    build_copper_stage_index,
    compute_copper_labels,
    delete_copper_stage_index_outputs,
    format_dt,
    get_copper_stage_recompute_context,
    load_bronze_window,
    make_manifest_row,
    normalize_copper_stage_index_windows,
    now_utc,
    parse_dt,
    resolve_window,
    select_bronze_wells,
    to_polars_for_write,
    validate_copper_stage_index_no_overlaps,
    validate_mode,
    write_manifest,
)

WORKFLOW_NAME = "nextier_copper_labeling_v1"
BRONZE_SOURCE_KEY = BRONZE_LABELS_FEATURESTORE_KEY


async def _process_one_well(
    *,
    workspace_id: int,
    run_id: str,
    mode: str,
    bronze_featurestore_key: str,
    copper_featurestore_key: str,
    stage_index_featurestore_key: str,
    manifest_featurestore_key: str,
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
    preloaded_bronze_by_well: dict[str, pd.DataFrame] | None = None,
) -> dict:
    logger = get_run_logger()
    started_at = format_dt(now_utc())
    bounded_start_ts = effective_start_ts
    if bounded_start_ts is not None and float(lookback_hours or 0) > 0:
        bounded_start_ts = bounded_start_ts - pd.Timedelta(hours=float(lookback_hours or 0))

    # Recompute context anchors the new run to prior served stages. The first
    # pass applies fixed context hours; the helper then walks farther left if
    # that timestamp is still inside an existing stage. Chunks therefore remain
    # small scheduling units, while actual recompute windows are stage-safe.
    logger.info(
        "Copper recompute-context load start well=%s stage_index_featurestore=%s before=%s algorithm_version=%s",
        well_name,
        stage_index_featurestore_key,
        format_dt(bounded_start_ts),
        algorithm_version,
    )
    recompute_context = await get_copper_stage_recompute_context(
        workspace_id=workspace_id,
        stage_index_featurestore_key=stage_index_featurestore_key,
        well_name=well_name,
        before_ts=bounded_start_ts,
        after_ts=effective_end_ts,
        algorithm_version=algorithm_version,
        boundary_context_hours=boundary_context_hours,
        max_boundary_expansion_hours=max_boundary_expansion_hours,
    )
    recompute_start_ts = recompute_context["recompute_start_ts"]
    recompute_end_ts = recompute_context["recompute_end_ts"]
    start_ordinal = int(recompute_context["start_ordinal"])
    logger.info(
        "Copper recompute-context load complete well=%s requested_start=%s requested_end=%s safe_start=%s safe_end=%s start_ordinal=%s context=%s left_expansions=%s left_stage_nums=%s right_expansions=%s right_stage_nums=%s",
        well_name,
        format_dt(recompute_context.get("requested_recompute_start_ts", bounded_start_ts)),
        format_dt(recompute_context.get("requested_recompute_end_ts", effective_end_ts)),
        format_dt(recompute_start_ts),
        format_dt(recompute_end_ts),
        start_ordinal,
        recompute_context["context"],
        recompute_context.get("boundary_expansions", 0),
        recompute_context.get("boundary_stage_nums", []),
        recompute_context.get("right_boundary_expansions", 0),
        recompute_context.get("right_boundary_stage_nums", []),
    )

    preloaded_bronze = (
        preloaded_bronze_by_well.get(well_name)
        if preloaded_bronze_by_well is not None
        else None
    )
    use_preloaded_bronze = False
    if isinstance(preloaded_bronze, pd.DataFrame) and not preloaded_bronze.empty:
        # Live orchestration can hand bronze rows directly to copper. Use them
        # only when they cover the recompute context; otherwise fall back to the
        # bronze featurestore to avoid missing the stage boundary rows.
        candidate = preloaded_bronze.copy()
        candidate["record_ts"] = pd.to_datetime(candidate["record_ts"], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
        candidate["created_ts"] = pd.to_datetime(candidate["created_ts"], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
        candidate = candidate.dropna(subset=["well_name", "record_ts"]).reset_index(drop=True)
        min_record_ts = candidate["record_ts"].min() if not candidate.empty else None
        use_preloaded_bronze = (
            not candidate.empty
            and (recompute_start_ts is None or min_record_ts is None or min_record_ts <= recompute_start_ts)
        )
        if use_preloaded_bronze:
            if recompute_start_ts is not None:
                candidate = candidate[candidate["record_ts"] >= recompute_start_ts]
            if recompute_end_ts is not None:
                candidate = candidate[candidate["record_ts"] < recompute_end_ts]
            bronze_df = candidate.reset_index(drop=True)
            logger.info(
                "Copper bronze-window load skipped via in-memory handoff well=%s rows=%s start=%s end=%s",
                well_name,
                len(bronze_df),
                format_dt(recompute_start_ts),
                format_dt(recompute_end_ts),
            )
        else:
            logger.info(
                "Copper bronze-window handoff unavailable well=%s handoff_min_record_ts=%s recompute_start=%s; falling back to featurestore",
                well_name,
                format_dt(min_record_ts),
                format_dt(recompute_start_ts),
            )

    if not use_preloaded_bronze:
        logger.info(
            "Copper bronze-window load start well=%s featurestore=%s start=%s end=%s",
            well_name,
            bronze_featurestore_key,
            format_dt(recompute_start_ts),
            format_dt(recompute_end_ts),
        )
        bronze_df = await load_bronze_window(
            workspace_id=workspace_id,
            bronze_featurestore_key=bronze_featurestore_key,
            well_name=well_name,
            start_ts=recompute_start_ts,
            end_ts=recompute_end_ts,
        )
    logger.info("Copper bronze-window load complete well=%s rows=%s", well_name, len(bronze_df))
    if bronze_df.empty:
        completed_at = format_dt(now_utc())
        manifest = make_manifest_row(
            run_id=run_id,
            workflow_name=WORKFLOW_NAME,
            mode=mode,
            source_datastore_key=bronze_featurestore_key,
            well_name=well_name,
            requested_start_ts=requested_start_ts,
            requested_end_ts=requested_end_ts,
            effective_start_ts=recompute_start_ts,
            effective_end_ts=recompute_end_ts,
            lookback_hours=lookback_hours,
            source_df=bronze_df,
            chunk_hours=chunk_hours,
            chunk_index=chunk_index,
            bronze_rows=0,
            copper_rows=0,
            stage_rows=0,
            algorithm_version=algorithm_version,
            dry_run=dry_run,
            status="skipped_no_bronze_rows",
            started_at=started_at,
            completed_at=completed_at,
        )
        if not dry_run:
            await write_manifest(workspace_id, manifest, manifest_featurestore_key)
        return {"well_name": well_name, "status": "skipped_no_bronze_rows", "manifest": manifest}

    processed_at = format_dt(now_utc())
    logger.info("Copper compute start well=%s bronze_rows=%s algorithm_version=%s", well_name, len(bronze_df), algorithm_version)
    copper_df = compute_copper_labels(
        bronze_df,
        mode=mode,
        algorithm_version=algorithm_version,
        processed_at=processed_at,
        start_ordinal_by_well={well_name: start_ordinal},
    )
    logger.info("Copper compute labels complete well=%s copper_rows=%s", well_name, len(copper_df))
    logger.info("Copper stage-index build start well=%s copper_rows=%s", well_name, len(copper_df))
    stage_index_df = build_copper_stage_index(
        copper_df,
        mode=mode,
        algorithm_version=algorithm_version,
        processed_at=processed_at,
    )
    logger.info("Copper stage-index build complete well=%s stage_windows=%s", well_name, len(stage_index_df))
    before_normalize_stage_windows = len(stage_index_df)
    # Normalization collapses accidental same-stage fragments before the overlap
    # guard runs. Any remaining overlap is treated as corrupted output.
    stage_index_df = normalize_copper_stage_index_windows(stage_index_df)
    logger.info("Copper stage-index normalize complete well=%s before=%s after=%s", well_name, before_normalize_stage_windows, len(stage_index_df))
    validate_copper_stage_index_no_overlaps(stage_index_df)

    logger.info(
        "Copper plan well=%s mode=%s dry_run=%s requested_recompute_start=%s actual_recompute_start=%s actual_recompute_end=%s start_ordinal=%s context=%s bronze_rows=%s copper_rows=%s stage_windows=%s",
        well_name,
        mode,
        dry_run,
        format_dt(bounded_start_ts),
        format_dt(recompute_start_ts),
        format_dt(recompute_end_ts),
        start_ordinal,
        recompute_context["context"],
        len(bronze_df),
        len(copper_df),
        len(stage_index_df),
    )

    deleted = {}
    replace_stage_index = bool(delete_existing)
    if not dry_run:
        if replace_stage_index:
            # Stage index is replaced over the recompute window because it is a
            # window table. Copper labels are upserted row-by-row below, which is
            # cheaper and avoids a large delete for every live/background run.
            logger.info("Copper stage-index delete start well=%s featurestore=%s start=%s end=%s", well_name, stage_index_featurestore_key, format_dt(recompute_start_ts), format_dt(recompute_end_ts))
            deleted[stage_index_featurestore_key] = await delete_copper_stage_index_outputs(
                workspace_id=workspace_id,
                well_name=well_name,
                start_ts=recompute_start_ts,
                end_ts=recompute_end_ts,
                stage_index_featurestore_key=stage_index_featurestore_key,
            )
            logger.info(
                "Copper stage-index deleted for replace well=%s start=%s end=%s stage_index_deleted=%s",
                well_name,
                format_dt(recompute_start_ts),
                format_dt(recompute_end_ts),
                deleted.get(stage_index_featurestore_key),
            )
        elif delete_existing:
            logger.info(
                "Copper label delete skipped well=%s cutoff=%s reason=upsert_replaces_recomputed_rows",
                well_name,
                format_dt(recompute_end_ts),
            )
        if not copper_df.empty:
            # Upsert by telemetry point ID overwrites rows in the recomputed
            # window, including canonicalized provisional/confirmed/continuous
            # state, without deleting untouched historical rows.
            logger.info("Copper labels write start well=%s featurestore=%s rows=%s mode=upsert", well_name, copper_featurestore_key, len(copper_df))
            await write_featurestore(
                featurestore_key=copper_featurestore_key,
                workspace_id=workspace_id,
                df=to_polars_for_write(copper_df),
                upsert=True,
            )
            logger.info(
                "Copper labels write complete well=%s featurestore=%s rows=%s mode=upsert",
                well_name,
                copper_featurestore_key,
                len(copper_df),
            )
        if not stage_index_df.empty:
            logger.info("Copper stage-index write start well=%s featurestore=%s stage_windows=%s mode=%s", well_name, stage_index_featurestore_key, len(stage_index_df), "replace" if replace_stage_index else "upsert")
            await write_featurestore(
                featurestore_key=stage_index_featurestore_key,
                workspace_id=workspace_id,
                df=to_polars_for_write(stage_index_df),
                upsert=not replace_stage_index,
            )
            logger.info(
                "Copper stage-index write complete well=%s featurestore=%s stage_windows=%s mode=%s",
                well_name,
                stage_index_featurestore_key,
                len(stage_index_df),
                "replace" if replace_stage_index else "upsert",
            )

    completed_at = format_dt(now_utc())
    manifest = make_manifest_row(
        run_id=run_id,
        workflow_name=WORKFLOW_NAME,
        mode=mode,
        source_datastore_key=bronze_featurestore_key,
        well_name=well_name,
        requested_start_ts=requested_start_ts,
        requested_end_ts=requested_end_ts,
        effective_start_ts=recompute_start_ts,
        effective_end_ts=recompute_end_ts,
        lookback_hours=lookback_hours,
        source_df=bronze_df,
        chunk_hours=chunk_hours,
        chunk_index=chunk_index,
        bronze_rows=len(bronze_df),
        copper_rows=len(copper_df),
        stage_rows=len(stage_index_df),
        algorithm_version=algorithm_version,
        dry_run=dry_run,
        status="dry_run" if dry_run else "written",
        started_at=started_at,
        completed_at=completed_at,
    )
    if not dry_run:
        logger.info("Copper manifest write start well=%s featurestore=%s", well_name, manifest_featurestore_key)
        await write_manifest(workspace_id, manifest, manifest_featurestore_key)
        logger.info("Copper manifest written well=%s status=%s requested=(%s,%s) effective=(%s,%s) copper_rows=%s stage_windows=%s", well_name, manifest["status"], manifest["requested_start_ts"], manifest["requested_end_ts"], manifest["effective_start_ts"], manifest["effective_end_ts"], manifest["rows_copper_written"], manifest["stage_count_written"])

    logger.info("Copper complete well=%s status=%s bronze_rows=%s copper_rows=%s stage_windows=%s", well_name, "dry_run" if dry_run else "written", len(bronze_df), len(copper_df), len(stage_index_df))

    return {
        "well_name": well_name,
        "status": "dry_run" if dry_run else "written",
        "bronze_rows": len(bronze_df),
        "copper_rows": len(copper_df),
        "stage_windows": len(stage_index_df),
        "deleted": deleted,
        "manifest": manifest,
    }


@flow(name="nextier-copper-labeling-v1")
async def nextier_copper_labeling_v1_flow(
    workspace_id: int,
    workflow_id: int,
    mode: str = "background",
    bronze_featurestore_key: str = BRONZE_LABELS_FEATURESTORE_KEY,
    copper_featurestore_key: str = COPPER_LABELS_FEATURESTORE_KEY,
    stage_index_featurestore_key: str = COPPER_STAGE_INDEX_FEATURESTORE_KEY,
    manifest_featurestore_key: str = MANIFEST_FEATURESTORE_KEY,
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
):
    return await run_nextier_copper_labeling_v1(
        workspace_id=workspace_id,
        mode=mode,
        bronze_featurestore_key=bronze_featurestore_key,
        copper_featurestore_key=copper_featurestore_key,
        stage_index_featurestore_key=stage_index_featurestore_key,
        manifest_featurestore_key=manifest_featurestore_key,
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
    )


async def run_nextier_copper_labeling_v1(
    *,
    workspace_id: int,
    mode: str = "background",
    bronze_featurestore_key: str = BRONZE_LABELS_FEATURESTORE_KEY,
    copper_featurestore_key: str = COPPER_LABELS_FEATURESTORE_KEY,
    stage_index_featurestore_key: str = COPPER_STAGE_INDEX_FEATURESTORE_KEY,
    manifest_featurestore_key: str = MANIFEST_FEATURESTORE_KEY,
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
    selected_wells: list[str] | None = None,
    preloaded_bronze_by_well: dict[str, pd.DataFrame] | None = None,
):
    logger = get_run_logger()
    mode = validate_mode(mode)
    bronze_featurestore_key = bronze_featurestore_key or BRONZE_LABELS_FEATURESTORE_KEY
    copper_featurestore_key = copper_featurestore_key or COPPER_LABELS_FEATURESTORE_KEY
    stage_index_featurestore_key = stage_index_featurestore_key or COPPER_STAGE_INDEX_FEATURESTORE_KEY
    manifest_featurestore_key = manifest_featurestore_key or MANIFEST_FEATURESTORE_KEY
    algorithm_version = algorithm_version or DEFAULT_ALGORITHM_VERSION
    lookback_hours = float(lookback_hours or 0)
    max_wells = int(max_wells or 5)
    boundary_context_hours = max(0.0, float(boundary_context_hours or 0.0))
    max_boundary_expansion_hours = max(0.0, float(max_boundary_expansion_hours or 0.0))
    requested_start_ts = parse_dt(start_time)
    requested_end_ts = parse_dt(end_time)
    effective_start_ts, effective_end_ts = resolve_window(mode, start_time, end_time, float(lookback_hours or 0))
    if selected_wells is not None:
        # Orchestration passes selected_wells for live/background so copper only
        # processes wells that bronze selected in the same target chunk.
        wells = [str(well).strip() for well in selected_wells if str(well).strip()]
        logger.info(
            "Using preselected copper wells mode=%s requested_range=(%s,%s) cutoff=%s wells=%s",
            mode,
            format_dt(effective_start_ts),
            format_dt(effective_end_ts),
            format_dt(effective_end_ts),
            wells,
        )
    else:
        wells = await select_bronze_wells(
            workspace_id=workspace_id,
            bronze_featurestore_key=bronze_featurestore_key,
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
        logger.info("No wells selected for copper labeling mode=%s requested_range=(%s,%s) scope=(well=%s fleet=%s pad=%s)", mode, format_dt(effective_start_ts), format_dt(effective_end_ts), well_name, fleet_name, pad_name)
        return {
            "wells_processed": 0,
            "mode": mode,
            "dry_run": bool(dry_run),
            "effective_start_ts": format_dt(effective_start_ts),
            "effective_end_ts": format_dt(effective_end_ts),
            "results": [],
        }

    logger.info(
        "Selected %s wells for copper labeling mode=%s requested_range=(%s,%s) cutoff=%s wells=%s",
        len(wells),
        mode,
        format_dt(effective_start_ts),
        format_dt(effective_end_ts),
        format_dt(effective_end_ts),
        wells,
    )

    run_id = str(uuid4())
    results = []
    for selected_well in wells:
        try:
            results.append(
                await _process_one_well(
                    workspace_id=workspace_id,
                    run_id=run_id,
                    mode=mode,
                    bronze_featurestore_key=bronze_featurestore_key,
                    copper_featurestore_key=copper_featurestore_key,
                    stage_index_featurestore_key=stage_index_featurestore_key,
                    manifest_featurestore_key=manifest_featurestore_key,
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
                    preloaded_bronze_by_well=preloaded_bronze_by_well,
                )
            )
        except Exception as exc:
            logger.exception("Copper labeling failed for well=%s", selected_well)
            if not dry_run:
                manifest = make_manifest_row(
                    run_id=run_id,
                    workflow_name=WORKFLOW_NAME,
                    mode=mode,
                    source_datastore_key=bronze_featurestore_key,
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

    return {
        "run_id": run_id,
        "wells_processed": len(results),
        "mode": mode,
        "dry_run": bool(dry_run),
        "effective_start_ts": format_dt(effective_start_ts),
        "effective_end_ts": format_dt(effective_end_ts),
        "results": results,
    }
