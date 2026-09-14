"""Well-level auto-select of the concentration channel the dashboards default to.

Thin orchestration around nextier-dash's own selector -- the algorithm lives in
`nextier_utils.labeling.auto.conc_channel_select`, exactly as the sapphire and
titanium workflows call `run_well` and `titanium_merge`. Nothing here
reimplements it.

WHAT IT DOES
    For each well, hand `select_conc_channel_for_well` the first three
    substage-refined stage windows and persist the row it returns, so the
    concentration dropdown can default per well. The tiers, the ordering and
    the stairstep gate are upstream's; see
    `docs/conc_channel_auto_select_handoff.md` in nextier-dash.

WHAT IT DOES NOT DO
    It does not change which channel any labelling algorithm uses. Titanium and
    Sapphire keep resolving their own channel. This store is a UI default only,
    and the dropdown stays manually overridable.

WHY IT READS THE REFINED INDEX INSTEAD OF RE-RUNNING SAPPHIRE
    Upstream computes the windows from live sapphire placements. We already
    persisted exactly those windows per stage in the titanium substage index, so
    this flow needs one telemetry window per well rather than a full
    re-placement -- 396 wells of three stages each, not a nine-hour fleet pass.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

TEMPLATE_ROOT = Path(__file__).resolve().parents[3]
if str(TEMPLATE_ROOT) not in sys.path:
    sys.path.insert(0, str(TEMPLATE_ROOT))

import numpy as np
import pandas as pd
import polars as pl
from prefect import flow, get_run_logger

from nixdlt.workflow_sdk.platform_tasks import query_store, write_featurestore
from scripts.workflows.nextier_labeling_common_v1.common import (
    apply_fleet_filters,
    apply_well_name_filters,
    combine_well_names,
    format_dt,
    normalize_name_list,
    now_utc,
    to_polars_for_write,
)

SOURCE_DATASTORE_KEY = "merged_fleet_stream_customer_full_v2"
SUBSTAGE_INDEX_KEY = "nextier_titanium_substage_index_sept3_v2"
OUTPUT_KEY = "nextier_well_conc_channel_v1"
ALGORITHM_VERSION = "nextier_conc_channel_select_v1"

# Physical column per slot. `prop_conc` is a legacy alias for auger.
CONC_COLUMNS: dict[str, str] = {
    "auger": "prop_conc_blend_auger",
    "target": "prop_conc_target",
    "denso": "prop_conc_blend_denso",
    "inline": "prop_conc_inline",
}
AUGER_ALIASES = ("prop_conc",)
# Denso BEFORE inline -- this selector only. See the module docstring.
# Physical column -> the slot name the dashboard dropdown uses. Upstream returns
# a column; the UI selects by slot.
COL_TO_SLOT = {v: k for k, v in CONC_COLUMNS.items()} | {a: "auger" for a in AUGER_ALIASES}

# Upstream's own default; read from the module at run time so a change there
# does not silently disagree with the window planner here.
MAX_CONC_SELECT_STAGES = 3


async def _first_refined_windows(
    workspace_id: int, index_key: str, *, well_names, fleet_name,
    include_fleets, exclude_fleets, pad_name, limit: int = MAX_CONC_SELECT_STAGES,
) -> pd.DataFrame:
    """The first `limit` refined stages per well, ascending stage_num."""
    conditions = ["stage_start_ts IS NOT NULL", "stage_end_ts IS NOT NULL"]
    params: dict[str, Any] = {}
    apply_well_name_filters(conditions, params, "well_name", well_names=well_names)
    apply_fleet_filters(
        conditions, params, "fleet_name",
        fleet_name=fleet_name,
        include_fleet_names=include_fleets,
        exclude_fleet_names=exclude_fleets,
    )
    if pad_name:
        conditions.append("pad_name = :pad_name")
        params["pad_name"] = pad_name

    frame = await query_store(
        sql=f"""
            SELECT well_name, stage_num, stage_start_ts, stage_end_ts,
                   fleet_name, pad_name
            FROM (
              SELECT well_name, stage_num, stage_start_ts, stage_end_ts,
                     fleet_name, pad_name,
                     ROW_NUMBER() OVER (
                       PARTITION BY well_name
                       ORDER BY CAST(stage_num AS DOUBLE PRECISION) ASC
                     ) AS rn
              FROM featurestore:{index_key}
              WHERE {" AND ".join(conditions)}
            ) t
            WHERE rn <= {int(limit)}
            ORDER BY well_name, CAST(stage_num AS DOUBLE PRECISION)
        """,
        workspace_id=workspace_id,
        params=params,
    )
    df = frame.to_pandas() if isinstance(frame, pl.DataFrame) else pd.DataFrame(frame)
    return df


async def _already_selected(workspace_id: int, output_key: str) -> set[str]:
    frame = await query_store(
        sql=f"SELECT well_name FROM featurestore:{output_key}",
        workspace_id=workspace_id,
        params={},
    )
    df = frame.to_pandas() if isinstance(frame, pl.DataFrame) else pd.DataFrame(frame)
    if df.empty or "well_name" not in df.columns:
        return set()
    return {str(x) for x in df["well_name"].dropna().tolist()}


async def _conc_columns_present(workspace_id: int, source_key: str) -> list[str]:
    """Which concentration columns this datastore actually has.

    `prop_conc` is a legacy alias for auger that `columns_for_conc_slot` still
    checks, but it does not exist on every datastore -- naming it unconditionally
    makes the whole SELECT fail with UndefinedColumn. Probe once per run and take
    only what is there.
    """
    frame = await query_store(
        sql=f"SELECT * FROM datastore:{source_key} LIMIT 1",
        workspace_id=workspace_id,
        params={},
    )
    df = frame.to_pandas() if isinstance(frame, pl.DataFrame) else pd.DataFrame(frame)
    have = {str(c) for c in df.columns}
    cols = [c for c in (*CONC_COLUMNS.values(), *AUGER_ALIASES) if c in have]
    if not cols:
        raise RuntimeError(
            f"{source_key} has none of the concentration columns "
            f"{(*CONC_COLUMNS.values(), *AUGER_ALIASES)} -- nothing to score"
        )
    return cols


async def _load_window(
    workspace_id: int, source_key: str, well: str, t0: str, t1: str,
    conc_cols: list[str],
) -> pd.DataFrame:
    """Telemetry between t0 and t1. One query spans all scored stages; the
    per-stage slices are cut from it in-process rather than re-querying."""
    cols = ", ".join(conc_cols)
    frame = await query_store(
        sql=f"""
            SELECT record_ts, {cols}
            FROM datastore:{source_key}
            WHERE name = :well_name
              AND record_ts >= :t0 AND record_ts <= :t1
            ORDER BY record_ts ASC
        """,
        workspace_id=workspace_id,
        params={"well_name": well, "t0": t0, "t1": t1},
    )
    return frame.to_pandas() if isinstance(frame, pl.DataFrame) else pd.DataFrame(frame)


@flow(name="nextier_conc_channel_select_v1")
async def nextier_conc_channel_select_v1_flow(
    workspace_id: int,
    workflow_id: int | None = None,
    source_datastore_key: str = SOURCE_DATASTORE_KEY,
    substage_index_featurestore_key: str = SUBSTAGE_INDEX_KEY,
    output_featurestore_key: str = OUTPUT_KEY,
    well_name: str | None = None,
    well_names: list[str] | None = None,
    fleet_name: str | None = None,
    include_fleet_names: list[str] | None = None,
    exclude_fleet_names: list[str] | None = None,
    pad_name: str | None = None,
    max_wells: int = 500,
    skip_completed: bool = True,
    dry_run: bool = False,
    algorithm_version: str = ALGORITHM_VERSION,
) -> dict[str, Any]:
    del workflow_id  # Platform flow contract -- injected on every run, unused here.

    logger = get_run_logger()
    run_id = str(now_utc().value)

    wells = combine_well_names(well_name, normalize_name_list(well_names))
    # Log the upstream module that will do the work, so a run says which
    # nextier-dash the image carries -- the equivalent of sapphire logging
    # `ds_callable`.
    import nextier_utils.labeling.auto.conc_channel_select as _sel

    logger.info(
        "Conc channel select start index=%s output=%s selector=%s max_stages=%s "
        "max_wells=%s skip_completed=%s dry_run=%s",
        substage_index_featurestore_key, output_featurestore_key,
        f"{_sel.__name__}.select_conc_channel_for_well",
        _sel.MAX_CONC_SELECT_STAGES, max_wells, skip_completed, dry_run,
    )

    plan = await _first_refined_windows(
        workspace_id, substage_index_featurestore_key,
        well_names=wells, fleet_name=fleet_name,
        include_fleets=normalize_name_list(include_fleet_names),
        exclude_fleets=normalize_name_list(exclude_fleet_names),
        pad_name=pad_name,
    )
    if plan.empty:
        logger.warning("Conc channel select: no refined stages matched the scope")
        return {"wells": 0, "written": 0, "skipped": 0, "empty": 0}

    done = await _already_selected(workspace_id, output_featurestore_key) if skip_completed else set()
    conc_cols = await _conc_columns_present(workspace_id, source_datastore_key)
    logger.info("Conc channel select: datastore has channels %s", ",".join(conc_cols))

    # Imported here rather than at module scope, matching sapphire and titanium:
    # the package is installed into the job image from nextier-dash, so a version
    # mismatch fails this run rather than stopping the flow from importing at all.
    from nextier_core.stage_utils import DATETIME_COL
    from nextier_utils.labeling.auto.conc_channel_select import (
        MAX_CONC_SELECT_STAGES as UPSTREAM_MAX_STAGES,
        select_conc_channel_for_well,
    )

    processed_at = format_dt(now_utc())
    rows: list[dict[str, Any]] = []
    skipped = empty = 0

    plan["stage_num"] = pd.to_numeric(plan["stage_num"], errors="coerce")
    for well, grp in plan.groupby("well_name", sort=True):
        if len(rows) >= int(max_wells):
            break
        well = str(well)
        if well in done:
            skipped += 1
            continue
        grp = grp.sort_values("stage_num").head(int(UPSTREAM_MAX_STAGES))
        spans = [(int(r["stage_num"]), format_dt(r["stage_start_ts"]), format_dt(r["stage_end_ts"]))
                 for _n, r in grp.iterrows()]
        lo, hi = spans[0][1], spans[-1][2]
        frame = await _load_window(workspace_id, source_datastore_key, well, lo, hi, conc_cols)
        base = {
            "well_name": well, "stage_num": float(spans[0][0]), "t0": lo, "t1": hi,
            "algorithm_version": algorithm_version, "source_mode": "historical",
            "run_id": run_id, "processed_at": processed_at,
        }
        if frame.empty:
            logger.warning("Conc channel select: empty stage windows well=%s", well)
            empty += 1
            rows.append({**base, "conc_col": None, "concentration_feature": None,
                         "candidates": "[]", "reason": "empty first-stage window",
                         "channel_notes": "{}", "sample_count": 0.0,
                         "refine_n": 0.0, "stages_scored": "[]"})
            continue

        # Upstream indexes on DATETIME_COL and takes its windows as `placements`;
        # ours come from the persisted substage index, so build the same shape it
        # gets from a live sapphire run. `event_window_from_placement` prefers
        # open_well/close_well, which is exactly what those columns already hold.
        compute = frame.rename(columns={"record_ts": DATETIME_COL})
        placements = [
            {"stage_n": sn, "t0": pd.Timestamp(a), "t1": pd.Timestamp(b),
             "open_well__t": pd.Timestamp(a), "close_well__t": pd.Timestamp(b)}
            for sn, a, b in spans
        ]

        # Off the event loop -- numpy over four channels on up to three windows is
        # long enough for a Prefect lease renewal to land inside it and kill the run.
        result = await asyncio.to_thread(
            select_conc_channel_for_well, well, compute, placements=placements
        )

        slot = COL_TO_SLOT.get(result.conc_col or "", None)
        rows.append({
            **base,
            "stage_num": float(result.stage_n) if result.stage_n is not None else float(spans[0][0]),
            "conc_col": result.conc_col,
            "concentration_feature": slot,
            "candidates": json.dumps(list(result.candidates)),
            "reason": result.reason,
            # upstream already JSON-encodes this, including its by_stage map
            "channel_notes": result.to_row()["channel_notes"],
            "sample_count": float(len(frame)),
            "refine_n": float(result.refine_n),
            "stages_scored": json.dumps([int(x) for x in result.stages_scored]),
        })
        logger.info(
            "Conc channel select well=%s stages=%s rows=%s pick=%s slot=%s reason=%s",
            well, result.stages_scored, len(frame), result.conc_col, slot, result.reason,
        )

    if not rows:
        logger.info("Conc channel select complete: nothing to write (skipped=%s)", skipped)
        return {"wells": int(plan["well_name"].nunique()), "written": 0,
                "skipped": skipped, "empty": empty}

    out = pd.DataFrame(rows)
    if dry_run:
        logger.info("Conc channel select DRY RUN -- would write %s rows", len(out))
    else:
        await write_featurestore(
            featurestore_key=output_featurestore_key,
            workspace_id=workspace_id,
            df=to_polars_for_write(out),
            upsert=True,
            bulk=True,
        )

    picked = int(out["concentration_feature"].notna().sum())
    logger.info(
        "Conc channel select complete wells=%s written=%s skipped=%s empty=%s "
        "with_pick=%s no_pick=%s",
        int(plan["well_name"].nunique()), len(out), skipped, empty, picked, len(out) - picked,
    )
    return {
        "wells": int(plan["well_name"].nunique()), "written": int(len(out)),
        "skipped": int(skipped), "empty": int(empty), "with_pick": picked,
    }
