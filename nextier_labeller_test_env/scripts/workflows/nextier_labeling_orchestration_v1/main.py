from __future__ import annotations

from pathlib import Path
import sys
from uuid import uuid4

TEMPLATE_ROOT = Path(__file__).resolve().parents[3]
if str(TEMPLATE_ROOT) not in sys.path:
    sys.path.insert(0, str(TEMPLATE_ROOT))

from prefect import flow, get_run_logger
from scripts.workflows.nextier_auto_labeling_v1.main import run_nextier_auto_labeling_v1
from scripts.workflows.nextier_bronze_labeling_v1.main import (
    run_nextier_bronze_labeling_v1,
)
from scripts.workflows.nextier_copper_labeling_v1.main import (
    run_nextier_copper_labeling_v1,
)
from scripts.workflows.nextier_labeling_common_v1.common import (
    BRONZE_LABELS_FEATURESTORE_KEY,
    COPPER_LABELS_FEATURESTORE_KEY,
    COPPER_STAGE_INDEX_FEATURESTORE_KEY,
    DEFAULT_ALGORITHM_VERSION,
    MANIFEST_FEATURESTORE_KEY,
    SOURCE_DATASTORE_KEY,
    build_time_chunks,
    combine_well_names,
    format_dt,
    get_historical_chunk_manifest_status,
    get_raw_scope_bounds,
    get_well_index_scope_bounds,
    reset_historical_labeling_manifest_once,
    resolve_window,
    select_historical_pending_work,
    validate_mode,
    write_manifest,
)

BRONZE_WORKFLOW_NAME = "nextier_bronze_labeling_v1"
COPPER_WORKFLOW_NAME = "nextier_copper_labeling_v1"
AUTO_LABELS_FEATURESTORE_KEY = "nextier_auto_labels_v1"
AUTO_STAGE_SUMMARY_FEATURESTORE_KEY = "nextier_auto_stage_summary_v1"
AUTO_MANIFEST_FEATURESTORE_KEY = "nextier_auto_processing_manifest_v1"
DEFAULT_AUTO_ALGORITHM_VERSION = "nextier_auto_layer_v1"


def _default_chunk_hours(mode: str, chunk_hours: float | None) -> float | None:
    # Historical/background process bounded chunks; live uses a single rolling
    # window because it is expected to run frequently.
    if chunk_hours is not None and float(chunk_hours) > 0:
        return float(chunk_hours)
    if mode == "historical":
        return 24.0
    if mode == "background":
        return 24.0
    return None


def _default_context_hours(mode: str, lookback_hours: float, context_hours: float | None) -> float:
    # lookback_hours selects the target window. context_hours controls only the
    # extra data fed to the DS stage algorithm before each target chunk so that
    # stage boundaries can be evaluated without reprocessing the full lookback.
    if context_hours is not None:
        return max(0.0, float(context_hours or 0))
    if mode == "historical":
        return max(0.0, float(lookback_hours or 0))
    if mode == "background":
        return min(max(0.0, float(lookback_hours or 0)), 24.0)
    if mode == "live":
        return max(6.0, max(0.0, float(lookback_hours or 0)))
    return max(0.0, float(lookback_hours or 0))


async def _write_covered_manifest_rows(
    *,
    workspace_id: int,
    manifest_featurestore_key: str,
    base_manifest: dict | None,
    workflow_name: str,
    well_name: str,
    covered_work: list[dict],
    latest_chunk_index: int,
    dry_run: bool,
) -> int:
    if dry_run or not base_manifest or base_manifest.get("status") != "written":
        return 0

    written = 0
    for work_item in covered_work:
        chunk_index = int(work_item["chunk_index"])
        if chunk_index == latest_chunk_index:
            continue

        manifest = dict(base_manifest)
        manifest["manifest_id"] = f"{base_manifest.get('run_id')}:{well_name}:{uuid4()}"
        manifest["workflow_name"] = workflow_name
        manifest["well_name"] = well_name
        manifest["requested_start_ts"] = format_dt(work_item["chunk_start_ts"])
        manifest["requested_end_ts"] = format_dt(work_item["chunk_end_ts"])
        manifest["chunk_index"] = float(chunk_index)
        manifest["status"] = "written"
        await write_manifest(workspace_id, manifest, manifest_featurestore_key)
        written += 1

    return written


@flow(name="nextier-labeling-orchestration-v1")
async def nextier_labeling_orchestration_v1_flow(
    workspace_id: int,
    workflow_id: int,
    mode: str = "live",
    source_datastore_key: str = SOURCE_DATASTORE_KEY,
    bronze_featurestore_key: str = BRONZE_LABELS_FEATURESTORE_KEY,
    copper_featurestore_key: str = COPPER_LABELS_FEATURESTORE_KEY,
    stage_index_featurestore_key: str = COPPER_STAGE_INDEX_FEATURESTORE_KEY,
    manifest_featurestore_key: str = MANIFEST_FEATURESTORE_KEY,
    well_name: str | None = None,
    well_names: list[str] | None = None,
    fleet_name: str | None = None,
    pad_name: str | None = None,
    include_fleet_names: list[str] | None = None,
    exclude_fleet_names: list[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    lookback_hours: float = 6,
    context_hours: float | None = None,
    boundary_context_hours: float = 0.5,
    max_boundary_expansion_hours: float = 96,
    chunk_hours: float | None = None,
    max_chunks: int | None = None,
    max_wells: int = 25,
    skip_completed: bool = False,
    delete_existing: bool = True,
    recompute: bool = False,
    recompute_run_key: str | None = None,
    run_auto_labeling_after_stage: bool = False,
    auto_labels_featurestore_key: str = AUTO_LABELS_FEATURESTORE_KEY,
    auto_summary_featurestore_key: str = AUTO_STAGE_SUMMARY_FEATURESTORE_KEY,
    auto_manifest_featurestore_key: str = AUTO_MANIFEST_FEATURESTORE_KEY,
    auto_algorithm_version: str = DEFAULT_AUTO_ALGORITHM_VERSION,
    auto_concentration_feature: str = "all",
    auto_max_stages: int = 100,
    dry_run: bool = False,
    algorithm_version: str = DEFAULT_ALGORITHM_VERSION,
):
    """Run bronze, then copper, over bounded time chunks.

    Historical mode can be launched without explicit dates: the flow uses the
    well index to plan pending work when skip_completed is enabled, then runs
    bronze and copper over the selected well/chunk ranges.
    """
    logger = get_run_logger()
    mode = validate_mode(mode)
    source_datastore_key = source_datastore_key or SOURCE_DATASTORE_KEY
    bronze_featurestore_key = bronze_featurestore_key or BRONZE_LABELS_FEATURESTORE_KEY
    copper_featurestore_key = copper_featurestore_key or COPPER_LABELS_FEATURESTORE_KEY
    stage_index_featurestore_key = stage_index_featurestore_key or COPPER_STAGE_INDEX_FEATURESTORE_KEY
    manifest_featurestore_key = manifest_featurestore_key or MANIFEST_FEATURESTORE_KEY
    algorithm_version = algorithm_version or DEFAULT_ALGORITHM_VERSION
    lookback_hours = float(lookback_hours or 0)
    context_hours = _default_context_hours(mode, lookback_hours, context_hours)
    boundary_context_hours = max(0.0, float(boundary_context_hours or 0.0))
    max_boundary_expansion_hours = max(0.0, float(max_boundary_expansion_hours or 0.0))
    max_wells = int(max_wells or 25)
    max_chunks = int(max_chunks) if max_chunks is not None else None
    targeted_wells = combine_well_names(well_name, well_names)
    start_ts, end_ts = resolve_window(mode, start_time, end_time, lookback_hours)


    async def run_auto_for_changed_stages(
        *,
        stage_well_names: list[str],
        auto_start: str | None,
        auto_end: str | None,
    ) -> dict | None:
        # The stage workflow is the source of truth for which wells changed.
        # When enabled, auto/substage labeling is scoped to those wells and the
        # same target chunk, so substages are refreshed immediately after stage
        # windows are rewritten.
        selected = [str(name).strip() for name in stage_well_names if str(name).strip()]
        if not run_auto_labeling_after_stage or not selected:
            return None
        logger.info(
            "Auto handoff start mode=%s wells=%s range=(%s,%s) max_stages=%s concentration=%s",
            mode,
            selected,
            auto_start,
            auto_end,
            auto_max_stages,
            auto_concentration_feature,
        )
        result = await run_nextier_auto_labeling_v1(
            workspace_id=workspace_id,
            mode=mode,
            copper_featurestore_key=copper_featurestore_key,
            auto_labels_featurestore_key=auto_labels_featurestore_key or AUTO_LABELS_FEATURESTORE_KEY,
            stage_index_featurestore_key=stage_index_featurestore_key,
            auto_summary_featurestore_key=auto_summary_featurestore_key or AUTO_STAGE_SUMMARY_FEATURESTORE_KEY,
            auto_manifest_featurestore_key=auto_manifest_featurestore_key or AUTO_MANIFEST_FEATURESTORE_KEY,
            well_names=selected,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
            start_time=auto_start,
            end_time=auto_end,
            lookback_hours=context_hours if mode in {"live", "background", "debug"} else lookback_hours,
            max_wells=max(1, len(selected)),
            max_stages=int(auto_max_stages or 100),
            skip_completed=False,
            delete_existing=delete_existing,
            recompute=False,
            dry_run=dry_run,
            algorithm_version=auto_algorithm_version or DEFAULT_AUTO_ALGORITHM_VERSION,
            concentration_feature=auto_concentration_feature or "all",
        )
        logger.info(
            "Auto handoff complete mode=%s wells=%s stages_selected=%s results_processed=%s",
            mode,
            selected,
            result.get("stages_selected"),
            result.get("results_processed"),
        )
        return result

    if mode == "historical" and (start_ts is None or end_ts is None):
        # Historical can run without dates. Prefer the well index because it is
        # compact; fall back to raw data bounds if the index is missing/stale.
        if skip_completed:
            scope_start_ts, scope_end_ts = await get_well_index_scope_bounds(
                workspace_id=workspace_id,
                source_datastore_key=source_datastore_key,
                well_name=well_name,
                well_names=well_names,
                fleet_name=fleet_name,
                pad_name=pad_name,
                include_fleet_names=include_fleet_names,
                exclude_fleet_names=exclude_fleet_names,
            )
            if scope_start_ts is None or scope_end_ts is None:
                scope_start_ts, scope_end_ts = await get_raw_scope_bounds(
                    workspace_id=workspace_id,
                    source_datastore_key=source_datastore_key,
                    well_name=well_name,
                    well_names=well_names,
                    fleet_name=fleet_name,
                    pad_name=pad_name,
                    include_fleet_names=include_fleet_names,
                    exclude_fleet_names=exclude_fleet_names,
                )
        else:
            scope_start_ts, scope_end_ts = await get_raw_scope_bounds(
                workspace_id=workspace_id,
                source_datastore_key=source_datastore_key,
                well_name=well_name,
                well_names=well_names,
                fleet_name=fleet_name,
                pad_name=pad_name,
                include_fleet_names=include_fleet_names,
                exclude_fleet_names=exclude_fleet_names,
            )
        if start_ts is None:
            start_ts = scope_start_ts
        if end_ts is None:
            end_ts = scope_end_ts

    effective_chunk_hours = _default_chunk_hours(mode, chunk_hours)
    logger.info(
        "Labeling orchestration window semantics mode=%s target_range=(%s,%s) target_lookback_hours=%s context_hours=%s boundary_context_hours=%s max_boundary_expansion_hours=%s chunk_hours=%s",
        mode,
        format_dt(start_ts),
        format_dt(end_ts),
        lookback_hours,
        context_hours,
        boundary_context_hours,
        max_boundary_expansion_hours,
        effective_chunk_hours,
    )
    chunk_limit = int(max_chunks) if max_chunks is not None and int(max_chunks) > 0 else None
    recompute_reset = {"reset": False, "reason": "not_requested", "deleted": {}}
    auto_results = []
    if recompute:
        # Recompute is intentionally historical-only. The marker makes the reset
        # idempotent: first run clears manifest coverage for the requested range;
        # subsequent runs with the same key continue from the remaining chunks.
        if mode != "historical":
            raise ValueError("recompute is only supported for historical labeling orchestration")
        if start_ts is None or end_ts is None:
            raise ValueError("recompute requires a finite historical range after scope resolution")
        if effective_chunk_hours is None or float(effective_chunk_hours) <= 0:
            raise ValueError("recompute requires chunk_hours > 0")
        recompute_reset = await reset_historical_labeling_manifest_once(
            workspace_id=workspace_id,
            manifest_featurestore_key=manifest_featurestore_key,
            source_datastore_key=source_datastore_key,
            recompute_run_key=str(recompute_run_key or "").strip(),
            well_name=well_name,
            well_names=well_names,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
            start_ts=start_ts,
            end_ts=end_ts,
            chunk_hours=float(effective_chunk_hours),
            algorithm_version=algorithm_version,
            dry_run=dry_run,
        )
        logger.info("Historical recompute manifest reset result=%s", recompute_reset)
    if (
        mode == "historical"
        and skip_completed
        and start_ts is not None
        and end_ts is not None
        and effective_chunk_hours is not None
        and float(effective_chunk_hours) > 0
    ):
        # Incremental historical backfill path. The planner chooses pending
        # well/chunk work from the manifest, then we group adjacent chunks per
        # well to reduce repeated reads and writes while preserving chunk-level
        # manifest coverage.
        pending_work = await select_historical_pending_work(
            workspace_id=workspace_id,
            source_datastore_key=source_datastore_key,
            manifest_featurestore_key=manifest_featurestore_key,
            well_name=well_name,
            well_names=well_names,
            fleet_name=fleet_name,
            pad_name=pad_name,
            include_fleet_names=include_fleet_names,
            exclude_fleet_names=exclude_fleet_names,
            start_ts=start_ts,
            end_ts=end_ts,
            chunk_hours=effective_chunk_hours,
            max_chunks=chunk_limit,
            max_wells=max_wells,
            algorithm_version=algorithm_version,
            dry_run=dry_run,
        )
        logger.info(
            "Historical planner selected pending work items=%s max_chunks_per_well=%s max_wells=%s",
            len(pending_work),
            chunk_limit,
            max_wells,
        )
        if not pending_work:
            return {
                "mode": mode,
                "dry_run": bool(dry_run),
                "chunk_hours": effective_chunk_hours,
                "chunks_processed": 0,
                "chunks_attempted": 0,
                "work_items_processed": 0,
                "chunk_start_ts": format_dt(start_ts),
                "chunk_end_ts": format_dt(end_ts),
                "bronze": [],
                "copper": [],
                "auto": auto_results,
                "recompute_reset": recompute_reset,
            }

        planned_by_well: dict[str, list[dict]] = {}
        for work_item in pending_work:
            planned_by_well.setdefault(str(work_item["well_name"]), []).append(work_item)

        bronze_results = []
        copper_results = []
        processed_chunk_indexes: set[int] = set()
        covered_manifest_rows = 0
        for selected_well, well_work in planned_by_well.items():
            well_work = sorted(well_work, key=lambda item: int(item["chunk_index"]))
            earliest_work_item = well_work[0]
            latest_work_item = well_work[-1]
            chunk_index = int(latest_work_item["chunk_index"])
            chunk_start = format_dt(earliest_work_item["chunk_start_ts"])
            chunk_end = format_dt(latest_work_item["chunk_end_ts"])
            logger.info(
                "Processing grouped historical work well=%s planned_chunks=%s latest_chunk_index=%s requested_start=%s requested_end=%s cutoff=%s",
                selected_well,
                [int(item["chunk_index"]) for item in well_work],
                chunk_index,
                chunk_start,
                chunk_end,
                chunk_end,
            )

            bronze_result = await run_nextier_bronze_labeling_v1(
                workspace_id=workspace_id,
                mode=mode,
                source_datastore_key=source_datastore_key,
                bronze_featurestore_key=bronze_featurestore_key,
                manifest_featurestore_key=manifest_featurestore_key,
                stage_index_featurestore_key=stage_index_featurestore_key,
                well_name=selected_well,
                fleet_name=fleet_name,
                pad_name=pad_name,
                include_fleet_names=include_fleet_names,
                exclude_fleet_names=exclude_fleet_names,
                start_time=chunk_start,
                end_time=chunk_end,
                lookback_hours=lookback_hours,
                boundary_context_hours=boundary_context_hours,
                max_boundary_expansion_hours=max_boundary_expansion_hours,
                max_wells=1,
                skip_completed=skip_completed,
                chunk_hours=effective_chunk_hours,
                chunk_index=chunk_index,
                delete_existing=delete_existing,
                dry_run=dry_run,
                algorithm_version=algorithm_version,
            )
            bronze_results.append(bronze_result)

            copper_result = await run_nextier_copper_labeling_v1(
                workspace_id=workspace_id,
                mode=mode,
                bronze_featurestore_key=bronze_featurestore_key,
                copper_featurestore_key=copper_featurestore_key,
                stage_index_featurestore_key=stage_index_featurestore_key,
                manifest_featurestore_key=manifest_featurestore_key,
                well_name=selected_well,
                fleet_name=fleet_name,
                pad_name=pad_name,
                include_fleet_names=include_fleet_names,
                exclude_fleet_names=exclude_fleet_names,
                start_time=chunk_start,
                end_time=chunk_end,
                lookback_hours=lookback_hours,
                boundary_context_hours=boundary_context_hours,
                max_boundary_expansion_hours=max_boundary_expansion_hours,
                max_wells=1,
                skip_completed=skip_completed,
                selected_wells=[selected_well],
                chunk_hours=effective_chunk_hours,
                chunk_index=chunk_index,
                delete_existing=delete_existing,
                dry_run=dry_run,
                algorithm_version=algorithm_version,
            )
            copper_results.append(copper_result)
            auto_result = await run_auto_for_changed_stages(
                stage_well_names=[selected_well] if int(copper_result.get("wells_processed") or 0) > 0 else [],
                auto_start=chunk_start,
                auto_end=chunk_end,
            )
            if auto_result is not None:
                auto_results.append(auto_result)
            processed_chunk_indexes.update(int(item["chunk_index"]) for item in well_work)
            covered_manifest_rows += await _write_covered_manifest_rows(
                workspace_id=workspace_id,
                manifest_featurestore_key=manifest_featurestore_key,
                base_manifest=bronze_result.get("results", [{}])[0].get("manifest") if bronze_result.get("results") else None,
                workflow_name=BRONZE_WORKFLOW_NAME,
                well_name=selected_well,
                covered_work=well_work,
                latest_chunk_index=chunk_index,
                dry_run=dry_run,
            )
            covered_manifest_rows += await _write_covered_manifest_rows(
                workspace_id=workspace_id,
                manifest_featurestore_key=manifest_featurestore_key,
                base_manifest=copper_result.get("results", [{}])[0].get("manifest") if copper_result.get("results") else None,
                workflow_name=COPPER_WORKFLOW_NAME,
                well_name=selected_well,
                covered_work=well_work,
                latest_chunk_index=chunk_index,
                dry_run=dry_run,
            )

        logger.info(
            "Completed planned historical orchestration chunks_processed=%s work_items_processed=%s grouped_wells_processed=%s covered_manifest_rows=%s",
            len(processed_chunk_indexes),
            len(pending_work),
            len(planned_by_well),
            covered_manifest_rows,
        )
        return {
            "mode": mode,
            "dry_run": bool(dry_run),
            "chunk_hours": effective_chunk_hours,
            "chunks_processed": len(processed_chunk_indexes),
            "chunks_attempted": len(processed_chunk_indexes),
            "work_items_processed": len(pending_work),
            "grouped_wells_processed": len(planned_by_well),
            "covered_manifest_rows": covered_manifest_rows,
            "chunk_start_ts": format_dt(start_ts),
            "chunk_end_ts": format_dt(end_ts),
            "bronze": bronze_results,
            "copper": copper_results,
            "auto": auto_results,
            "recompute_reset": recompute_reset,
        }

    build_limit = None if mode == "historical" and skip_completed else chunk_limit
    chunks = build_time_chunks(start_ts, end_ts, effective_chunk_hours, build_limit)
    logger.info(
        "Starting labeling orchestration mode=%s algorithm_version=%s scope=(well=%s fleet=%s pad=%s) requested_range=(%s,%s) candidate_chunks=%s chunk_hours=%s max_chunks=%s max_wells=%s skip_completed=%s delete_existing=%s dry_run=%s",
        mode,
        algorithm_version,
        targeted_wells or well_name,
        fleet_name,
        pad_name,
        format_dt(start_ts),
        format_dt(end_ts),
        len(chunks),
        effective_chunk_hours,
        chunk_limit,
        max_wells,
        skip_completed,
        delete_existing,
        dry_run,
    )

    if not chunks:
        return {
            "mode": mode,
            "dry_run": bool(dry_run),
            "chunks_processed": 0,
            "chunks_attempted": 0,
            "bronze": [],
            "copper": [],
            "auto": auto_results,
            "recompute_reset": recompute_reset,
        }

    bronze_results = []
    copper_results = []
    chunks_attempted = 0
    chunks_processed = 0
    for chunk_index, chunk_start_ts, chunk_end_ts in chunks:
        # Normal chunk path used by live/background/debug and by targeted
        # historical runs that are not using the pending-work planner.
        if mode == "historical" and skip_completed and chunk_limit is not None and chunks_processed >= chunk_limit:
            break
        chunk_start = format_dt(chunk_start_ts)
        chunk_end = format_dt(chunk_end_ts)
        logger.info("Processing labeling chunk index=%s requested_start=%s requested_end=%s cutoff=%s", chunk_index, chunk_start, chunk_end, chunk_end)

        manifest_status = {"bronze": False, "copper": False}
        can_check_manifest_chunk = (
            mode == "historical"
            and skip_completed
            and len(targeted_wells) == 1
            and chunk_start_ts is not None
            and chunk_end_ts is not None
            and effective_chunk_hours is not None
        )
        if can_check_manifest_chunk:
            manifest_status = await get_historical_chunk_manifest_status(
                workspace_id=workspace_id,
                manifest_featurestore_key=manifest_featurestore_key,
                well_name=targeted_wells[0],
                chunk_start_ts=chunk_start_ts,
                chunk_end_ts=chunk_end_ts,
                chunk_hours=float(effective_chunk_hours),
                chunk_index=chunk_index,
                algorithm_version=algorithm_version,
            )
            if manifest_status["bronze"] and manifest_status["copper"]:
                logger.info(
                    "Skipping completed historical labeling chunk index=%s well=%s requested_start=%s requested_end=%s reason=manifest_written",
                    chunk_index,
                    targeted_wells[0],
                    chunk_start,
                    chunk_end,
                )
                continue

        preloaded_bronze_by_well = None
        if manifest_status["bronze"]:
            logger.info(
                "Skipping bronze for historical labeling chunk index=%s well=%s requested_start=%s requested_end=%s reason=manifest_written",
                chunk_index,
                targeted_wells[0],
                chunk_start,
                chunk_end,
            )
            bronze_result = {
                "wells_processed": 1,
                "mode": mode,
                "dry_run": bool(dry_run),
                "effective_start_ts": chunk_start,
                "effective_end_ts": chunk_end,
                "results": [
                    {
                        "well_name": targeted_wells[0],
                        "status": "skipped_manifest_written",
                    }
                ],
            }
        else:
            bronze_lookback_hours = context_hours if mode in {"live", "background", "debug"} else lookback_hours
            # Live returns bronze frames in-memory so copper does not immediately
            # re-query the same rows. Background still reads bronze from the
            # featurestore because its target window can span larger chunks.
            bronze_result = await run_nextier_bronze_labeling_v1(
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
                start_time=chunk_start,
                end_time=chunk_end,
                lookback_hours=bronze_lookback_hours,
                boundary_context_hours=boundary_context_hours,
                max_boundary_expansion_hours=max_boundary_expansion_hours,
                max_wells=max_wells,
                skip_completed=skip_completed,
                chunk_hours=effective_chunk_hours,
                chunk_index=chunk_index,
                delete_existing=delete_existing,
                dry_run=dry_run,
                algorithm_version=algorithm_version,
                return_frames=(mode == "live"),
            )
            preloaded_bronze_by_well = bronze_result.pop("_bronze_frames_by_well", None) if mode == "live" else None
        selected_bronze_wells = [
            str(result.get("well_name"))
            for result in bronze_result.get("results", [])
            if result.get("well_name")
        ]
        bronze_results.append(bronze_result)

        if manifest_status["copper"]:
            logger.info(
                "Skipping copper for historical labeling chunk index=%s well=%s requested_start=%s requested_end=%s reason=manifest_written",
                chunk_index,
                targeted_wells[0],
                chunk_start,
                chunk_end,
            )
            copper_result = {
                "wells_processed": 1,
                "mode": mode,
                "dry_run": bool(dry_run),
                "effective_start_ts": chunk_start,
                "effective_end_ts": chunk_end,
                "results": [
                    {
                        "well_name": targeted_wells[0],
                        "status": "skipped_manifest_written",
                    }
                ],
            }
        else:
            # For live/background, only run copper for wells bronze actually
            # touched in this chunk. This prevents repeated selection of
            # unrelated wells and keeps high-frequency live runs bounded.
            copper_result = await run_nextier_copper_labeling_v1(
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
                start_time=chunk_start,
                end_time=chunk_end,
                lookback_hours=context_hours if mode in {"live", "background", "debug"} else lookback_hours,
                boundary_context_hours=boundary_context_hours,
                max_boundary_expansion_hours=max_boundary_expansion_hours,
                max_wells=max_wells,
                skip_completed=skip_completed,
                selected_wells=selected_bronze_wells if mode in {"live", "background", "debug"} else None,
                preloaded_bronze_by_well=preloaded_bronze_by_well,
                chunk_hours=effective_chunk_hours,
                chunk_index=chunk_index,
                delete_existing=delete_existing,
                dry_run=dry_run,
                algorithm_version=algorithm_version,
            )
        copper_results.append(copper_result)
        auto_result = await run_auto_for_changed_stages(
            stage_well_names=selected_bronze_wells if int(copper_result.get("wells_processed") or 0) > 0 else [],
            auto_start=chunk_start,
            auto_end=chunk_end,
        )
        if auto_result is not None:
            auto_results.append(auto_result)

        chunk_wells_processed = int(bronze_result.get("wells_processed") or 0) + int(copper_result.get("wells_processed") or 0)
        logger.info(
            "Finished labeling chunk index=%s bronze_wells=%s copper_wells=%s chunk_wells_processed=%s",
            chunk_index,
            bronze_result.get("wells_processed"),
            copper_result.get("wells_processed"),
            chunk_wells_processed,
        )
        chunks_attempted += 1
        if chunk_wells_processed > 0 or not (mode == "historical" and skip_completed):
            chunks_processed += 1

    logger.info(
        "Completed labeling orchestration mode=%s chunks_processed=%s chunks_attempted=%s",
        mode,
        chunks_processed,
        chunks_attempted,
    )

    return {
        "mode": mode,
        "dry_run": bool(dry_run),
        "chunk_hours": effective_chunk_hours,
        "context_hours": context_hours,
        "boundary_context_hours": boundary_context_hours,
        "max_boundary_expansion_hours": max_boundary_expansion_hours,
        "chunks_processed": chunks_processed,
        "chunks_attempted": chunks_attempted,
        "chunk_start_ts": format_dt(start_ts),
        "chunk_end_ts": format_dt(end_ts),
        "bronze": bronze_results,
        "copper": copper_results,
        "auto": auto_results,
        "recompute_reset": recompute_reset,
    }
