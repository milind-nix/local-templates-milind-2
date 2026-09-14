"""Well-level auto-select of the concentration channel the dashboards default to.

Port of the nextier-dash "conc channel auto-select" handoff
(`docs/conc_channel_auto_select_handoff.md`, commit 696a11a7) onto the platform.

WHAT IT DOES
    For each well, score the four concentration channels on that well's first
    THREE substage-refined stage windows (open_well -> close_well) and persist one
    row saying which channel the dropdown should default to. Each window is scored
    into a tier; the best tier across the windows wins, later stage breaking a tie:

        stairstep_starts_at_0  (3)  starts near zero AND passes the stairstep gate
        stairstep              (2)  passes the gate but does not start near zero
        fallback_present       (1)  first channel with finite data, in order
        no_candidate           (0)  nothing usable

    Ties within a tier break auger -> target -> denso -> inline.

    Scoring three stages rather than one matters: a thin first stage leaves every
    live channel `flat_no_signal` (AUSTIN 474-1004H, 695 rows) and decides the well
    on no evidence.

WHAT IT DOES NOT DO
    It does not change which channel any labelling algorithm uses. Titanium and
    Sapphire keep resolving their own channel (`CONC_CHANNEL_ORDER`, and
    Sapphire's zeroed-baseline `_usable` guard). This store is a UI default only,
    and the dropdown stays manually overridable. Note the tie-break here puts
    DENSO BEFORE INLINE, which is deliberately different from the global order --
    do not "align" them.

WHY IT READS THE REFINED INDEX INSTEAD OF RE-RUNNING SAPPHIRE
    The handoff computes the window from live sapphire placements. We already
    persisted exactly that window per stage in the titanium substage index, so
    this flow needs one telemetry window per well rather than a full re-placement
    -- 396 wells of one stage each, not a nine-hour fleet pass.
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
SELECT_ORDER = ("auger", "target", "denso", "inline")
COL_TO_SLOT = {v: k for k, v in CONC_COLUMNS.items()} | {a: "auger" for a in AUGER_ALIASES}

_EARLY_START_N = 12
_MIN_ZERO = 0.01
MAX_CONC_SELECT_STAGES = 3

# Higher is better. Mirrors upstream `_TIER_RANK`.
_TIER_RANK = {"stairstep_starts_at_0": 3, "stairstep": 2,
              "fallback_present": 1, "no_candidate": 0}


def _load_gate():
    """Prefer nextier-dash's own gate; fall back to the vendored copy.

    Upstream is the source of truth, but its selector module does
    `from src.analysis.stairstep_fit import ...` and the wheel ships only
    `src/nextier_core` and `src/nextier_utils`. Wherever only the wheel is
    installed that import raises, so we cannot depend on it being there.
    """
    try:
        from src.analysis.stairstep_fit import fit_stage, normalize_minmax  # type: ignore

        return fit_stage, normalize_minmax, "upstream"
    except Exception:  # noqa: BLE001
        from scripts.workflows.nextier_conc_channel_select_v1.stairstep_fit import (
            fit_stage,
            normalize_minmax,
        )

        return fit_stage, normalize_minmax, "vendored"


def _starts_near_zero(series: pd.Series) -> bool:
    """True when the series ALREADY starts near zero -- raw, uncorrected.

    Baseline correction is for display and shape analysis AFTER a channel is
    chosen, never inside the selection predicate. Correcting first subtracts the
    first-n mean, so those samples average exactly 0, `.clip(lower=0)` kills the
    negative half and the min is 0.0 -- every offset channel passes and only a
    channel whose idle floor sits between the correction band (3.0) and the zero
    test (0.01) fails. On our fleet that put 21 wells on a densometer reading
    10-15 ppa over an auger sitting at zero. Fixed upstream in nextier-dash
    7d21a352; this mirrors it.
    """
    vals = pd.to_numeric(series, errors="coerce").dropna()
    if vals.empty:
        return False
    return float(vals.iloc[: max(1, _EARLY_START_N)].min()) <= _MIN_ZERO


def _is_stairstep(series: pd.Series, fit_stage, normalize_minmax) -> tuple[bool, dict]:
    vals = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    finite = vals[np.isfinite(vals)]
    if finite.size < 10:
        return False, {"reject_reason": "too_few_samples", "n_steps": -1}
    shape = normalize_minmax(finite)
    if float(shape.max()) < 0.5:
        return False, {"reject_reason": "flat_no_signal", "n_steps": -1}
    fit = fit_stage(shape)
    return (not fit.get("reject_reason")), fit


def _resolve_col(window: pd.DataFrame, slot: str) -> str | None:
    names = (CONC_COLUMNS[slot], *AUGER_ALIASES) if slot == "auger" else (CONC_COLUMNS[slot],)
    for col in names:
        if col not in window.columns:
            continue
        vals = pd.to_numeric(window[col], errors="coerce").dropna()
        if not vals.empty and bool(np.isfinite(vals).any()):
            return col
    return None


def _score_window(window: pd.DataFrame, stage_n: int) -> dict[str, Any]:
    """Score one refined stage window into a tier. Pure CPU."""
    fit_stage, normalize_minmax, _src = _load_gate()
    notes: dict[str, Any] = {}
    stair_zero: list[str] = []
    stair_only: list[str] = []
    present: list[str] = []

    for slot in SELECT_ORDER:
        col = _resolve_col(window, slot)
        if col is None:
            notes[CONC_COLUMNS[slot]] = {
                "present": False, "starts_at_0": False, "is_stairstep": False,
                "reject_reason": "missing_or_all_nan", "n_steps": -1,
            }
            continue
        series = pd.to_numeric(window[col], errors="coerce")
        zero = _starts_near_zero(series)
        ok, fit = _is_stairstep(series, fit_stage, normalize_minmax)
        notes[col] = {
            "present": True, "starts_at_0": bool(zero), "is_stairstep": bool(ok),
            "reject_reason": "" if ok else str(fit.get("reject_reason") or "rejected"),
            "n_steps": int(fit.get("n_steps", -1) or -1),
        }
        present.append(col)
        if ok and zero:
            stair_zero.append(col)
        elif ok:
            stair_only.append(col)

    if stair_zero:
        pick, reason, cands = stair_zero[0], "stairstep_starts_at_0", stair_zero
    elif stair_only:
        pick, reason, cands = stair_only[0], "stairstep", []
    elif present:
        # First channel with data, in order -- NOT "first that starts at zero".
        # That older rule ran through the inverted zero test and is what elected
        # densometers over augers.
        pick, reason, cands = present[0], "fallback_present", []
    else:
        pick, reason, cands = None, "no_candidate", []
    return {"conc_col": pick, "candidates": cands, "reason": reason,
            "stage_n": int(stage_n), "channel_notes": notes}


def select_conc_channel(windows: list[tuple[int, pd.DataFrame]]) -> dict[str, Any]:
    """Score up to MAX_CONC_SELECT_STAGES windows; best tier wins.

    A tie on tier goes to the LATER stage -- stage 1 is often short or ragged,
    so a later window scoring the same tier is the better evidence.
    """
    scored = [_score_window(w, sn) for sn, w in windows if not w.empty]
    if not scored:
        return {"conc_col": None, "candidates": [], "reason": "empty first-stage window",
                "stage_n": None, "channel_notes": {}, "refine_n": 0, "stages_scored": []}

    best = scored[0]
    by_stage: dict[str, Any] = {}
    for r in scored:
        by_stage[str(r["stage_n"])] = {"conc_col": r["conc_col"], "reason": r["reason"],
                                       "channels": r["channel_notes"]}
        rank_b, rank_a = _TIER_RANK.get(r["reason"], -1), _TIER_RANK.get(best["reason"], -1)
        if rank_b > rank_a or (rank_b == rank_a and r["stage_n"] >= best["stage_n"]):
            best = r
    out = dict(best)
    out["channel_notes"] = dict(best["channel_notes"], by_stage=by_stage)
    out["refine_n"] = len(scored)
    out["stages_scored"] = sorted(r["stage_n"] for r in scored)
    return out


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


async def _load_window(
    workspace_id: int, source_key: str, well: str, t0: str, t1: str
) -> pd.DataFrame:
    """Telemetry between t0 and t1. One query spans all scored stages; the
    per-stage slices are cut from it in-process rather than re-querying."""
    cols = ", ".join(sorted({*CONC_COLUMNS.values(), *AUGER_ALIASES}))
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
    logger = get_run_logger()
    run_id = str(now_utc().value)

    wells = combine_well_names(well_name, normalize_name_list(well_names))
    _f, _n, gate_source = _load_gate()
    logger.info(
        "Conc channel select start index=%s output=%s gate=%s max_wells=%s "
        "skip_completed=%s dry_run=%s",
        substage_index_featurestore_key, output_featurestore_key, gate_source,
        max_wells, skip_completed, dry_run,
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
        grp = grp.sort_values("stage_num")
        spans = [(int(r["stage_num"]), format_dt(r["stage_start_ts"]), format_dt(r["stage_end_ts"]))
                 for _n, r in grp.iterrows()]
        lo, hi = spans[0][1], spans[-1][2]
        # One query covering every scored stage; slices are cut in-process.
        frame = await _load_window(workspace_id, source_datastore_key, well, lo, hi)
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

        ts = pd.to_datetime(frame["record_ts"], errors="coerce")
        windows = []
        for sn, a, b in spans:
            sub = frame.loc[(ts >= pd.Timestamp(a)) & (ts <= pd.Timestamp(b))]
            if not sub.empty:
                windows.append((sn, sub))

        # Off the event loop -- numpy over four channels on up to three windows is
        # long enough for a Prefect lease renewal to land inside it.
        result = await asyncio.to_thread(select_conc_channel, windows)

        slot = COL_TO_SLOT.get(result["conc_col"] or "", None)
        rows.append({
            **base,
            "stage_num": float(result["stage_n"]) if result.get("stage_n") is not None else float(spans[0][0]),
            "conc_col": result["conc_col"],
            "concentration_feature": slot,
            "candidates": json.dumps(result["candidates"]),
            "reason": result["reason"],
            "channel_notes": json.dumps(result["channel_notes"]),
            "sample_count": float(len(frame)),
            "refine_n": float(result.get("refine_n", 0)),
            "stages_scored": json.dumps(result.get("stages_scored", [])),
        })
        logger.info(
            "Conc channel select well=%s stages=%s rows=%s pick=%s slot=%s reason=%s",
            well, result.get("stages_scored"), len(frame), result["conc_col"], slot,
            result["reason"],
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
