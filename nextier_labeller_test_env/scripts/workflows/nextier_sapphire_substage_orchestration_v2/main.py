from __future__ import annotations

from pathlib import Path
import sys
from typing import Any
from uuid import uuid4

TEMPLATE_ROOT = Path(__file__).resolve().parents[3]
if str(TEMPLATE_ROOT) not in sys.path:
    sys.path.insert(0, str(TEMPLATE_ROOT))

import pandas as pd
import polars as pl
from prefect import flow, get_run_logger

from nixdlt.workflow_sdk.platform_tasks import delete_featurestore_records, query_store, write_featurestore
from scripts.workflows.nextier_labeling_common_v1.common import (
    SOURCE_DATASTORE_KEY,
    apply_fleet_filters,
    apply_well_name_filters,
    format_dt,
    load_raw_window,
    make_manifest_row,
    normalize_name_list,
    now_utc,
    parse_dt,
    prepare_raw_frame,
    resolve_window,
    to_polars_for_write,
    validate_mode,
    write_manifest,
)

WORKFLOW_NAME = "nextier_sapphire_substage_orchestration_v2"
DEFAULT_ALGORITHM_VERSION = "nextier_sapphire_substage_v2"

TITANIUM_STAGE_INDEX_FEATURESTORE_KEY = "nextier_titanium_stage_index_v1"
SAPPHIRE_STAGE_SUMMARY_FEATURESTORE_KEY = "nextier_substage_sapphire_sept3_stage_summary_v2"
SAPPHIRE_LABELS_FEATURESTORE_KEY = "nextier_substage_sapphire_sept3_labels_v2"
SAPPHIRE_MANIFEST_FEATURESTORE_KEY = "nextier_substage_sapphire_sept3_processing_manifest_v2"
TITANIUM_SUBSTAGE_INDEX_FEATURESTORE_KEY = "nextier_titanium_substage_index_sept3_v2"

CONC_COLUMNS = {
    "auger": "prop_conc_blend_auger",
    "denso": "prop_conc_blend_denso",
    "inline": "prop_conc_inline",
    "target": "prop_conc_target",
}

# The seven landmarks sapphire places, left to right. Order is not cosmetic --
# later landmarks read earlier ones, and the stage summary table is this wide.
LANDMARKS = (
    "open_well",
    "stage_start",
    "pad_end",
    "ttr",
    "slurry_end",
    "stage_end",
    "close_well",
)

# aug27 keeps the COARSE window on stage_start_ts / stage_end_ts and prefixes the
# DETECTED landmarks (auto_stage_start_ts). Same split here so a landmark can never
# be read as a stage boundary: two of the seven need the prefix, the rest do not.
LANDMARK_COL = {
    "stage_start": "sapphire_stage_start",
    "stage_end": "sapphire_stage_end",
}


def _landmark_prefix(landmark: str) -> str:
    return LANDMARK_COL.get(landmark, landmark)


def _stage_window_columns(stage_source: str) -> tuple[str, str]:
    """Which titanium split supplies the coarse window.

    `final` is the post-stage authoritative split and the default. `first` is the
    causal one -- useful for checking what sapphire would have placed live, not
    for anything anyone reports on.
    """
    if stage_source == "first":
        return "first_start_ts", "first_end_ts"
    return "final_start_ts", "final_end_ts"


async def _closed_stages(
    workspace_id: int,
    stage_index_key: str,
    well_name: str,
    stage_source: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    titanium_algorithm_version: str | None,
) -> pd.DataFrame:
    """Closed titanium stages for one well, in stage order.

    Only closed stages: sapphire is post-stage (`on_stage_closed`) and placing
    landmarks on a window that is still filling produces landmarks that move.
    A stage is closed when the chosen split has BOTH bounds.
    """
    start_col, end_col = _stage_window_columns(stage_source)
    conditions = [
        "well_name = :well_name",
        f"{start_col} IS NOT NULL",
        f"{end_col} IS NOT NULL",
        "stage_num IS NOT NULL",
    ]
    params: dict[str, Any] = {"well_name": well_name}
    if titanium_algorithm_version:
        conditions.append("algorithm_version = :titanium_algorithm_version")
        params["titanium_algorithm_version"] = titanium_algorithm_version
    if start_ts is not None:
        conditions.append(f"{end_col} >= :start_time")
        params["start_time"] = format_dt(start_ts)
    if end_ts is not None:
        conditions.append(f"{start_col} < :end_time")
        params["end_time"] = format_dt(end_ts)

    frame = await query_store(
        sql=f"""
            SELECT stage_num, stage_uid AS titanium_stage_uid,
                   {start_col} AS t0, {end_col} AS t1,
                   stage_mass_klb,
                   fleet_name, pad_name, well_name, well_id, api_num
            FROM featurestore:{stage_index_key}
            WHERE {" AND ".join(conditions)}
            ORDER BY stage_num ASC
        """,
        workspace_id=workspace_id,
        params=params,
    )
    df = frame.to_pandas() if isinstance(frame, pl.DataFrame) else pd.DataFrame(frame)
    if df.empty:
        return df
    df["stage_num"] = pd.to_numeric(df["stage_num"], errors="coerce")
    df = df.dropna(subset=["stage_num"]).sort_values("stage_num").reset_index(drop=True)
    # One row per stage. A duplicate here would feed the same stage twice and
    # corrupt look-left memory just as badly as a gap would.
    return df.drop_duplicates(subset=["stage_num"], keep="last").reset_index(drop=True)


async def _already_placed(
    workspace_id: int,
    stage_summary_key: str,
    well_name: str,
    algorithm_version: str,
) -> set[int]:
    """Stage ordinals this well already has placements for."""
    frame = await query_store(
        sql=f"""
            SELECT DISTINCT stage_num
            FROM featurestore:{stage_summary_key}
            WHERE well_name = :well_name AND algorithm_version = :algorithm_version
              AND stage_num IS NOT NULL
        """,
        workspace_id=workspace_id,
        params={"well_name": well_name, "algorithm_version": algorithm_version},
    )
    df = frame.to_pandas() if isinstance(frame, pl.DataFrame) else pd.DataFrame(frame)
    if df.empty:
        return set()
    return {int(v) for v in pd.to_numeric(df["stage_num"], errors="coerce").dropna()}


def _contiguous_pending(stages: pd.DataFrame, placed: set[int]) -> pd.DataFrame:
    """The unplaced tail, never a subset with holes.

    SapphireLayer carries causal look-left state -- ring period, prior stage end.
    Feeding stage 5 without having fed 4 does not merely lose context, it makes
    the placements for 5 wrong in a way nothing downstream can detect. So work
    resumes from the first unplaced stage and takes everything after it, even if
    some later stages were already placed; those get recomputed and upserted.
    """
    if stages.empty:
        return stages
    ordinals = [int(v) for v in stages["stage_num"]]
    first_pending = next((i for i, n in enumerate(ordinals) if n not in placed), None)
    if first_pending is None:
        return stages.iloc[0:0]
    return stages.iloc[first_pending:].reset_index(drop=True)


def _placement_rows(
    placements: list[dict],
    identity: dict[str, Any],
    stage_source: str,
    concentration_feature: str,
    algorithm_version: str,
    mode: str,
    run_id: str,
    processed_at: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in placements:
        stage_n = row.get("stage_n")
        if stage_n is None or int(stage_n) < 0:
            continue
        out: dict[str, Any] = {
            "sapphire_stage_uid": f"{identity['well_name']}:{int(stage_n)}:{algorithm_version}",
            **identity,
            "stage_num": float(stage_n),
            "stage_source": stage_source,
            # Coarse window, on the aug27 column names.
            "stage_start_ts": format_dt(parse_dt(row.get("t0"))),
            "stage_end_ts": format_dt(parse_dt(row.get("t1"))),
            "well_family": row.get("well_family"),
            "concentration_feature": concentration_feature,
            "conc_col": row.get("conc_col"),
            "n_stages": row.get("n_stages"),
            "mass_klb": row.get("mass_klb"),
            "algorithm_version": algorithm_version,
            "source_mode": mode,
            "run_id": run_id,
            "processed_at": processed_at,
        }
        for lm in LANDMARKS:
            col = _landmark_prefix(lm)
            out[f"{col}_ts"] = format_dt(parse_dt(row.get(f"{lm}__t")))
            out[f"{col}_src"] = row.get(f"{lm}__src")
            # Sapphire returns the REASON it declined ("no causal rate reference
            # yet"), or None when it placed the landmark -- not a boolean. Both are
            # written: the flag because the column shipped as a boolean and the
            # platform's schema migration cannot retype it, and the reason because
            # it is the only thing that explains a gap.
            reason = row.get(f"{lm}__abstain")
            out[f"{col}_abstain_reason"] = str(reason) if reason else None
        rows.append(out)
    return pd.DataFrame(rows)


def _label_spans(placements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Placement rows with `t0` pulled back to the earliest placed landmark.

    `placements_to_labels_df` builds each stage's label frame as
    `df.iloc[i_fire:i_hi+1]`, where `i_fire` is the coarse `t0`. It extends the
    RIGHT edge to reach close_well but never extends the left edge to reach
    open_well or stage_start -- so any landmark placed before the coarse start
    falls outside the frame and produces no labels at all.

    That is the normal case, not an edge case: open_well precedes pumping by
    definition. On HAWKEYE 33-1324H, 8 of 10 stages had BOTH stage_start and
    pad_end before `t0`, so the entire `pad` span vanished and the table came
    back with 20 rows (the stage_end and close_well points) instead of 7,119.

    It is also window-dependent, which is worse: feeding the same stages from the
    stored index instead gave 1,675 rows, because those windows happen to start
    earlier. A bug that appears and disappears with the input framing is the kind
    that gets mistaken for a data problem.

    Returns COPIES. `t0` is the coarse boundary everywhere else -- the stage
    summary's `stage_start_ts`, the refined window's fallback, `_usable_boundary`'s
    slack -- and mutating it in place would silently move all of them.
    """
    out: list[dict[str, Any]] = []
    for row in placements:
        widened = dict(row)
        t0 = parse_dt(widened.get("t0"))
        if t0 is not None:
            for landmark in ("open_well", "stage_start"):
                ts = parse_dt(widened.get(f"{landmark}__t"))
                if ts is not None and ts < t0:
                    t0 = ts
            widened["t0"] = t0
        out.append(widened)
    return out


def _usable_boundary(
    landmark: Any, coarse_start: Any, coarse_end: Any
) -> Any:
    """A landmark timestamp, or None when it cannot be a stage boundary.

    open_well legitimately precedes the coarse start and close_well legitimately
    follows the coarse end -- expanding the window is the point of this tier -- so
    the check is generous: one stage-duration of slack on each side. What it
    catches is a landmark nowhere near the stage at all.

    That is not hypothetical. Sapphire's `close_well__src="prior"` path returns a
    microsecond epoch value which `pd.Timestamp(int)` reads as nanoseconds, giving
    a 1970 timestamp for a 2026 stage. Used unchecked it produced a refined window
    ending fifty-six years before it started, and nothing downstream would have
    said so. Upstream fix belongs in nextier-dash; this keeps the corruption out
    of the featurestore in the meantime.
    """
    ts, t0, t1 = parse_dt(landmark), parse_dt(coarse_start), parse_dt(coarse_end)
    if ts is None or t0 is None or t1 is None:
        return None
    slack = max(t1 - t0, pd.Timedelta(minutes=30))
    if t0 - slack <= ts <= t1 + slack:
        return landmark
    return None


def _refined_window_rows(placement_df: pd.DataFrame) -> pd.DataFrame:
    """titanium_substage: coarse windows re-bound to open_well / close_well.

    Mirrors `event_window_from_placement` in sapphire utils rather than importing
    it, because the platform rows have already been flattened to strings by this
    point. The rule it mirrors is small and fixed: start prefers open_well and
    falls back to the coarse t0, end prefers close_well and falls back to t1.
    Pad, slurry, stage_start and stage_end deliberately do not move boundaries.
    """
    if placement_df.empty:
        return placement_df
    rows: list[dict[str, Any]] = []
    for _, p in placement_df.iterrows():
        coarse_start, coarse_end = p.get("stage_start_ts"), p.get("stage_end_ts")
        open_t = _usable_boundary(p.get("open_well_ts"), coarse_start, coarse_end)
        close_t = _usable_boundary(p.get("close_well_ts"), coarse_start, coarse_end)
        start = open_t or coarse_start
        end = close_t or coarse_end
        if not start or not end:
            continue
        rows.append({
            "stage_uid": p["sapphire_stage_uid"],
            "fleet_name": p.get("fleet_name"),
            "pad_name": p.get("pad_name"),
            "well_name": p["well_name"],
            "well_id": p.get("well_id"),
            "api_num": p.get("api_num"),
            "stage_num": p["stage_num"],
            "titanium_stage_uid": p.get("titanium_stage_uid"),
            "sapphire_stage_uid": p["sapphire_stage_uid"],
            "stage_start_ts": start,
            "stage_end_ts": end,
            "coarse_start_ts": coarse_start,
            "coarse_end_ts": coarse_end,
            # Three states, not two, so a rejection is never mistaken for an
            # abstention: the landmark was placed, and we declined to use it.
            "start_src": ("open_well" if open_t
                          else "coarse_t0_landmark_rejected" if p.get("open_well_ts")
                          else "coarse_t0"),
            "end_src": ("close_well" if close_t
                        else "coarse_t1_landmark_rejected" if p.get("close_well_ts")
                        else "coarse_t1"),
            "stage_source": p.get("stage_source"),
            "algorithm_version": p["algorithm_version"],
            "source_mode": p["source_mode"],
            "run_id": p["run_id"],
            "processed_at": p["processed_at"],
        })
    return pd.DataFrame(rows)


def _label_rows(
    labels: pd.DataFrame,
    raw: pd.DataFrame,
    identity: dict[str, Any],
    stage_num_by_position: dict[int, float],
    stage_source: str,
    concentration_feature: str,
    conc_col: str | None,
    algorithm_version: str,
    mode: str,
    run_id: str,
    processed_at: str,
) -> pd.DataFrame:
    """Row-level substage labels joined back onto telemetry identity."""
    if labels is None or labels.empty:
        return pd.DataFrame()
    out = labels.copy()
    ts_col = next((c for c in ("datetime_fmt", "record_ts", "ts") if c in out.columns), None)
    if ts_col is None:
        return pd.DataFrame()
    out["record_ts"] = out[ts_col].map(lambda v: format_dt(parse_dt(v)))

    if "record_ts" in raw.columns:
        keyed = raw.copy()
        keyed["record_ts"] = keyed["record_ts"].map(lambda v: format_dt(parse_dt(v)))
        keep = [c for c in ("record_ts", "telemetry_point_id", "created_ts") if c in keyed.columns]
        out = out.merge(keyed[keep].drop_duplicates("record_ts"), on="record_ts", how="left")

    # Fall back to a deterministic composite when the source row id did not survive
    # the join -- the surrogate below is built from it, so it has to be stable.
    if "telemetry_point_id" not in out.columns:
        out["telemetry_point_id"] = None
    out["telemetry_point_id"] = out["telemetry_point_id"].where(
        out["telemetry_point_id"].notna(),
        identity["well_name"] + ":" + out["record_ts"].fillna(""),
    )

    # Surrogate primary key, following aug27's auto_label_id rather than keying on
    # the telemetry row. One sample can carry a label from more than one algorithm
    # version; keyed on telemetry_point_id alone the newer run would upsert over the
    # older one and the comparison you wanted to make would be gone.
    out["sapphire_label_id"] = (
        out["telemetry_point_id"].astype(str) + ":" + algorithm_version
    )

    for key, value in identity.items():
        out[key] = value
    out["substage_label"] = out["substage"] if "substage" in out.columns else None

    # placements_to_labels_df is called with one_based_stage=True, so a label's
    # stage_n is the POSITION in the stages list, 1-based -- while the placement
    # rows use 0-based stage_n and our own ordinals come from titanium. Mapping
    # position -> titanium stage_num explicitly, because both off-by-ones are
    # silent: the labels would simply attach to the wrong stage.
    positions = pd.to_numeric(out.get("stage_n"), errors="coerce")
    out["stage_num"] = positions.map(
        lambda v: stage_num_by_position.get(int(v) - 1) if pd.notna(v) else None
    )
    out["sapphire_stage_uid"] = out["stage_num"].map(
        lambda v: f"{identity['well_name']}:{int(v)}:{algorithm_version}"
        if pd.notna(v) else None
    )
    # Sapphire only ever runs on closed stages, so this is true by construction --
    # carried so the column means the same thing on both substage chains.
    out["stage_closed"] = True
    out["stage_source"] = stage_source
    out["concentration_feature"] = concentration_feature
    out["conc_col"] = conc_col
    out["algorithm_version"] = algorithm_version
    out["source_mode"] = mode
    out["run_id"] = run_id
    out["processed_at"] = processed_at

    columns = [
        "sapphire_label_id", "telemetry_point_id", "fleet_name", "pad_name", "well_name",
        "well_id", "api_num", "record_ts", "created_ts", "stage_num", "sapphire_stage_uid",
        "substage_label", "stage_closed", "stage_source",
        "concentration_feature", "conc_col", "algorithm_version", "source_mode",
        "run_id", "processed_at",
    ]
    return out[[c for c in columns if c in out.columns]]


async def _delete_outputs(
    workspace_id: int,
    well_name: str,
    stage_nums: list[int],
    stage_summary_key: str,
    labels_key: str,
    substage_index_key: str,
    algorithm_version: str,
) -> None:
    """Clear this well's rows for the stages about to be rewritten.

    Scoped to the stage range actually recomputed, not the whole well: a run
    that resumes at stage 40 must not delete stages 1-39 it is not going to
    write back.
    """
    if not stage_nums:
        return
    lo, hi = min(stage_nums), max(stage_nums)
    filters = [
        {"field": "well_name", "op": "eq", "value": well_name},
        {"field": "algorithm_version", "op": "eq", "value": algorithm_version},
        {"field": "stage_num", "op": "gte", "value": lo},
        {"field": "stage_num", "op": "lte", "value": hi},
    ]
    for key in (stage_summary_key, substage_index_key, labels_key):
        await delete_featurestore_records(
            featurestore_key=key,
            workspace_id=workspace_id,
            filters=filters,
            require_primary_key_filter=False,
        )


async def _process_well(
    *,
    workspace_id: int,
    run_id: str,
    mode: str,
    source_datastore_key: str,
    stage_index_key: str,
    stage_summary_key: str,
    labels_key: str,
    substage_index_key: str,
    manifest_key: str,
    well_name: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    stage_source: str,
    concentration_feature: str,
    context_hours: float,
    max_stages: int | None,
    skip_completed: bool,
    delete_existing: bool,
    dry_run: bool,
    algorithm_version: str,
    titanium_algorithm_version: str | None,
) -> dict[str, Any]:
    logger = get_run_logger()
    started_at = format_dt(now_utc())

    stages = await _closed_stages(
        workspace_id, stage_index_key, well_name, stage_source,
        start_ts, end_ts, titanium_algorithm_version,
    )
    if stages.empty:
        logger.info("Sapphire no closed titanium stages well=%s stage_source=%s", well_name, stage_source)
        return {"well_name": well_name, "status": "empty", "stages": 0}

    if skip_completed:
        placed = await _already_placed(workspace_id, stage_summary_key, well_name, algorithm_version)
        pending = _contiguous_pending(stages, placed)
        if pending.empty:
            logger.info("Sapphire well already complete well=%s stages=%s", well_name, len(stages))
            return {"well_name": well_name, "status": "skipped", "stages": 0}
    else:
        pending = stages

    if max_stages is not None and max_stages > 0:
        pending = pending.iloc[:max_stages].reset_index(drop=True)

    window_start = parse_dt(pending["t0"].iloc[0])
    window_end = parse_dt(pending["t1"].iloc[-1])
    if window_start is None or window_end is None:
        return {"well_name": well_name, "status": "empty", "stages": 0}
    context = pd.Timedelta(hours=float(context_hours or 0))

    raw_pl = await load_raw_window(
        workspace_id, source_datastore_key, well_name, window_start - context, window_end + context,
    )
    if raw_pl is None or raw_pl.is_empty():
        logger.info("Sapphire no telemetry well=%s window=(%s,%s)", well_name, format_dt(window_start), format_dt(window_end))
        return {"well_name": well_name, "status": "empty", "stages": 0}

    raw = prepare_raw_frame(raw_pl)
    conc_col = CONC_COLUMNS[concentration_feature]
    if conc_col not in raw.columns or raw[conc_col].notna().sum() == 0:
        # Not fatal. Several landmarks need concentration and will abstain
        # without it -- an abstention is recorded, so this stays visible.
        logger.info("Sapphire concentration unavailable well=%s column=%s", well_name, conc_col)
        conc_col = None

    identity = {
        "fleet_name": pending["fleet_name"].iloc[0] if "fleet_name" in pending else None,
        "pad_name": pending["pad_name"].iloc[0] if "pad_name" in pending else None,
        "well_name": well_name,
        "well_id": pending["well_id"].iloc[0] if "well_id" in pending else None,
        "api_num": pending["api_num"].iloc[0] if "api_num" in pending else None,
    }

    # Stages go in as an ordered, gap-free list -- that is the contract
    # SapphireLayer.on_stage_closed depends on.
    stage_windows = [
        (parse_dt(row["t0"]), parse_dt(row["t1"]))
        for _, row in pending.iterrows()
    ]
    # Imported here rather than at module scope, matching titanium: the package is
    # installed into the job image from nextier-dash, so a version mismatch should
    # fail this well rather than stop the whole flow from importing. Import BEFORE
    # the "compute start" line, or a missing package logs a start that never happened.
    from nextier_utils.constants import DATETIME_COL
    from nextier_utils.labeling.sapphire.labels import placements_to_labels_df
    from nextier_utils.labeling.sapphire.pipeline import run_well as run_sapphire_well
    from nextier_utils.labeling.titanium.merge import titanium_merge

    # The platform's telemetry column is `record_ts`; the algorithm indexes on
    # DATETIME_COL ("datetime_fmt") and reads it by constant, not by parameter -- so
    # rate_col/pressure_col being configurable is no help here. Renamed exactly as
    # titanium does it. `raw` is kept unrenamed for the label join below, which
    # matches on record_ts.
    compute = raw.rename(columns={"record_ts": DATETIME_COL}).copy()

    # NANOSECONDS, explicitly -- and this line is load-bearing.
    #
    # `run_well` takes its integer time axis with `pd.DatetimeIndex(...).asi8`,
    # which returns the index's OWN resolution. query_store hands us Polars, and
    # `.to_pandas()` on a Postgres TIMESTAMP yields datetime64[us] -- so asi8
    # yields MICROseconds while every consumer downstream reads it as nanoseconds.
    # Every landmark then lands in January 1970 (a 2026 stage reads as
    # 1970-01-21), `_usable_boundary` rejects them all, and the refined tier
    # collapses to zero-length windows. The detectors also compute their sample
    # spacing off the same array, so a 1 s cadence reads as 0.001 s and the
    # duration gates stop firing: measured on HAWKEYE 33-1324H, open_well went
    # 0/10, pad_end 3/10, stage_end 0/10 and labels 0.
    #
    # nextier-dash normalises exactly this way in two other places that were bitten
    # by it -- `titanium/telemetry.py` and `titanium/history.py`, both with the same
    # comment -- which is why titanium's own output is correct and sapphire's is not.
    # `sapphire/pipeline.py` is the one place the fix was never applied. Doing it
    # here fixes it for this workflow without a nextier-dash change; the upstream
    # fix still belongs there, for every other caller.
    compute[DATETIME_COL] = compute[DATETIME_COL].astype("datetime64[ns]")

    # Per-stage sand mass, and why this is not optional.
    #
    # Sapphire will not place stage_start until it has a picture of a "normal" stage
    # on this well, built from three numbers per closed stage: peak rate, peak
    # pressure, and sand mass. Supply `stages` without `stage_masses` and run_well
    # sets mass to NaN for every stage; the picture never forms, stage_start abstains
    # with "no causal rate reference yet", and open_well/pad_end/ttr/slurry_end all
    # abstain after it because they are measured from stage_start. v1 did exactly
    # that and placed 44 landmarks across 23 stages where this places 109.
    #
    # titanium_merge accepts our STORED windows as `spans` and returns each one's
    # mass using titanium's own integral, so no titanium segmentation is recomputed
    # and the windows are unchanged -- verified 23-in/23-out on HAWKEYE 33-1324H.
    # The integral is window-local, so a partial well frame is fine.
    # PREFERRED PATH: the mass titanium already computed, carried on the stage
    # index row. One mass per window by construction, so it cannot misalign.
    #
    # The re-derivation below is what misaligns. `titanium_merge` is a MERGE: it
    # welds light adjacent spans, so feeding it N stored windows can return
    # fewer than N masses (AUSTIN 474-1004H: 56 windows in, 38 masses out). The
    # length check then blanks every mass and sapphire abstains on stage_start
    # and everything measured from it. Reading the stored column avoids the
    # round trip entirely.
    #
    # Only FINAL rows carry a stored mass -- titanium writes it for
    # `stages_final` only -- so `stage_source=first` still takes the fallback,
    # as does any index row written before this column existed.
    stage_masses: list[float] = []
    stored = pending["stage_mass_klb"] if "stage_mass_klb" in pending.columns else None
    if stored is not None:
        vals = pd.to_numeric(stored, errors="coerce")
        if vals.notna().all() and len(vals) == len(stage_windows):
            stage_masses = [float(v) for v in vals]
            logger.info(
                "Sapphire using STORED titanium masses well=%s stages=%s",
                well_name, len(stage_masses),
            )
        elif vals.notna().any():
            logger.info(
                "Sapphire stored masses incomplete well=%s have=%s of %s -- recomputing",
                well_name, int(vals.notna().sum()), len(stage_windows),
            )

    if not stage_masses:
        try:
            merged = titanium_merge(
                well_name, df=compute,
                rate_col="rate_slurry", pressure_col="press_mainline",
                spans=stage_windows,
            )
            stage_masses = list(merged.get("stage_mass_klb") or [])
            if merged.get("reason"):
                logger.info("Sapphire mass note well=%s %s", well_name, str(merged["reason"])[:200])
        except Exception:  # noqa: BLE001 -- mass is an input, not the answer
            logger.exception("Sapphire stage-mass computation failed well=%s", well_name)

    # Loud, because silence here is what made v1 look like it worked. A length
    # mismatch means every landmark downstream of stage_start will abstain.
    if len(stage_masses) != len(stage_windows):
        logger.warning(
            "Sapphire stage-mass MISALIGNED well=%s stages=%s masses=%s -- sapphire "
            "will abstain on stage_start and everything measured from it",
            well_name, len(stage_windows), len(stage_masses),
        )
        stage_masses = []

    logger.info(
        "Sapphire compute start well=%s rows=%s stages=%s masses=%s finite_masses=%s "
        "window=(%s,%s) conc_col=%s "
        "ds_callable=nextier_utils.labeling.sapphire.pipeline.run_well",
        well_name, len(compute), len(stage_windows), len(stage_masses),
        sum(1 for m in stage_masses if pd.notna(m) and m > 0),
        format_dt(window_start), format_dt(window_end), conc_col,
    )

    # run_well rather than nextier_core.sapphire_layer.run_sapphire_layer: the core
    # entry point has no stage_masses parameter and cannot forward one. This is the
    # same driver the handoff doc's own titanium_substage example calls.
    placements = run_sapphire_well(
        well_name,
        df=compute,
        rate_col="rate_slurry",
        press_col="press_mainline",
        stages=stage_windows,
        stage_masses=stage_masses or None,
        stage_source=stage_source,
    )
    labels = placements_to_labels_df(well_name, compute, _label_spans(placements))

    class _Result:  # keeps the row builders below unchanged
        pass

    result = _Result()
    result.placements = placements
    result.labels = labels

    processed_at = format_dt(now_utc())
    placement_df = _placement_rows(
        result.placements, identity, stage_source, concentration_feature,
        algorithm_version, mode, run_id, processed_at,
    )
    if placement_df.empty:
        logger.info("Sapphire produced no placements well=%s stages=%s", well_name, len(stage_windows))
        return {"well_name": well_name, "status": "empty", "stages": 0}

    # Stage ordinals come back positional; restore the titanium ordinals so the
    # placements join to the coarse index rather than to a run-local counter.
    label_df = _label_rows(
        result.labels, raw, identity,
        {i: float(v) for i, v in enumerate(pending["stage_num"])},
        stage_source, concentration_feature, conc_col,
        algorithm_version, mode, run_id, processed_at,
    )

    n = len(placement_df)
    placement_df["stage_num"] = list(pending["stage_num"].astype(float))[:n]
    placement_df["titanium_stage_uid"] = (
        list(pending["titanium_stage_uid"])[:n] if "titanium_stage_uid" in pending else None
    )
    placement_df["sapphire_stage_uid"] = (
        placement_df["well_name"].astype(str)
        + ":" + placement_df["stage_num"].astype(int).astype(str)
        + ":" + algorithm_version
    )
    # Row-level label counts belong on the stage row, same as aug27's stage summary.
    if not label_df.empty and "stage_num" in label_df.columns:
        counts = label_df.groupby("stage_num").size()
        placement_df["label_row_count"] = (
            placement_df["stage_num"].map(counts).fillna(0).astype(float)
        )
    else:
        placement_df["label_row_count"] = 0.0

    window_df = _refined_window_rows(placement_df)

    logger.info(
        "Sapphire compute complete well=%s placements=%s windows=%s labels=%s",
        well_name, len(placement_df), len(window_df), len(label_df),
    )

    if dry_run:
        return {
            "well_name": well_name, "status": "dry_run",
            "stages": len(placement_df), "windows": len(window_df), "labels": len(label_df),
        }

    if delete_existing:
        await _delete_outputs(
            workspace_id, well_name, [int(v) for v in placement_df["stage_num"]],
            stage_summary_key, labels_key, substage_index_key, algorithm_version,
        )

    for key, frame in (
        (stage_summary_key, placement_df),
        (substage_index_key, window_df),
        (labels_key, label_df),
    ):
        if frame is None or frame.empty:
            continue
        await write_featurestore(
            featurestore_key=key,
            workspace_id=workspace_id,
            df=to_polars_for_write(frame),
            upsert=True,
            bulk=True,
        )
        logger.info("Sapphire write complete well=%s featurestore=%s rows=%s", well_name, key, len(frame))

    manifest = make_manifest_row(
        run_id=run_id, workflow_name=WORKFLOW_NAME, mode=mode,
        source_datastore_key=source_datastore_key, well_name=well_name,
        requested_start_ts=window_start, requested_end_ts=window_end,
        effective_start_ts=window_start - context, effective_end_ts=window_end + context,
        lookback_hours=context_hours, source_df=raw, chunk_hours=0, chunk_index=0,
        bronze_rows=0, copper_rows=len(label_df), stage_rows=len(placement_df),
        algorithm_version=algorithm_version, dry_run=dry_run, status="written",
        started_at=started_at, completed_at=format_dt(now_utc()),
    )
    manifest["window_rows"] = len(window_df)
    manifest["label_rows"] = len(label_df)
    await write_manifest(workspace_id, manifest, manifest_key)

    return {
        "well_name": well_name, "status": "written",
        "stages": len(placement_df), "windows": len(window_df), "labels": len(label_df),
    }


async def _select_wells(
    workspace_id: int,
    stage_index_key: str,
    stage_source: str,
    well_name: str | None,
    well_names: list[str] | None,
    fleet_name: str | None,
    pad_name: str | None,
    include_fleet_names: list[str] | None,
    exclude_fleet_names: list[str] | None,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    max_wells: int,
) -> list[str]:
    """Wells that have closed titanium stages in scope, fewest stages first.

    Ordering by stage count is deliberate: it puts the cheap wells through
    first, so a run against a heavy fleet still produces outputs early instead
    of spending its whole budget on one well.
    """
    start_col, end_col = _stage_window_columns(stage_source)
    conditions = [f"{start_col} IS NOT NULL", f"{end_col} IS NOT NULL", "well_name IS NOT NULL"]
    params: dict[str, Any] = {}
    apply_well_name_filters(conditions, params, "well_name", well_name=well_name, well_names=well_names)
    apply_fleet_filters(
        conditions, params, "fleet_name",
        fleet_name=fleet_name, include_fleet_names=include_fleet_names,
        exclude_fleet_names=exclude_fleet_names,
    )
    if pad_name:
        conditions.append("pad_name = :pad_name")
        params["pad_name"] = pad_name
    if start_ts is not None:
        conditions.append(f"{end_col} >= :start_time")
        params["start_time"] = format_dt(start_ts)
    if end_ts is not None:
        conditions.append(f"{start_col} < :end_time")
        params["end_time"] = format_dt(end_ts)

    frame = await query_store(
        sql=f"""
            SELECT well_name, COUNT(*) AS stage_count
            FROM featurestore:{stage_index_key}
            WHERE {" AND ".join(conditions)}
            GROUP BY well_name
            ORDER BY stage_count ASC, well_name ASC
        """,
        workspace_id=workspace_id,
        params=params,
    )
    df = frame.to_pandas() if isinstance(frame, pl.DataFrame) else pd.DataFrame(frame)
    if df.empty:
        return []
    return [str(v) for v in df["well_name"].head(max(1, int(max_wells)))]


@flow(name="nextier-sapphire-substage-orchestration-v2")
async def nextier_sapphire_substage_orchestration_v2_flow(
    workspace_id: int,
    workflow_id: int | None = None,
    mode: str = "background",
    source_datastore_key: str = SOURCE_DATASTORE_KEY,
    titanium_stage_index_featurestore_key: str = TITANIUM_STAGE_INDEX_FEATURESTORE_KEY,
    sapphire_stage_summary_featurestore_key: str = SAPPHIRE_STAGE_SUMMARY_FEATURESTORE_KEY,
    sapphire_labels_featurestore_key: str = SAPPHIRE_LABELS_FEATURESTORE_KEY,
    sapphire_manifest_featurestore_key: str = SAPPHIRE_MANIFEST_FEATURESTORE_KEY,
    titanium_substage_index_featurestore_key: str = TITANIUM_SUBSTAGE_INDEX_FEATURESTORE_KEY,
    well_name: str | None = None,
    well_names: list[str] | None = None,
    fleet_name: str | None = None,
    include_fleet_names: list[str] | None = None,
    exclude_fleet_names: list[str] | None = None,
    pad_name: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    lookback_hours: float = 24,
    context_hours: float = 0.5,
    stage_source: str = "final",
    concentration_feature: str = "auger",
    max_wells: int = 25,
    max_stages: int | None = None,
    skip_completed: bool = True,
    delete_existing: bool = True,
    dry_run: bool = False,
    algorithm_version: str = DEFAULT_ALGORITHM_VERSION,
    titanium_algorithm_version: str | None = "nextier_titanium_stage_v1",
) -> dict[str, Any]:
    del workflow_id  # Platform flow contract; lineage lives in the manifest.
    logger = get_run_logger()
    mode = validate_mode(mode)
    run_id = str(uuid4())

    if stage_source not in ("final", "first"):
        raise ValueError("stage_source must be 'final' or 'first'")
    if concentration_feature not in CONC_COLUMNS:
        raise ValueError("concentration_feature must be one of: auger, denso, inline, target")
    include_fleets = normalize_name_list(include_fleet_names)
    exclude_fleets = normalize_name_list(exclude_fleet_names)
    if include_fleets and exclude_fleets:
        raise ValueError("include_fleet_names and exclude_fleet_names are mutually exclusive")

    start_ts, end_ts = resolve_window(mode, start_time, end_time, lookback_hours)

    wells = await _select_wells(
        workspace_id, titanium_stage_index_featurestore_key, stage_source,
        well_name, well_names, fleet_name, pad_name, include_fleets, exclude_fleets,
        start_ts, end_ts, max_wells,
    )
    logger.info(
        "Sapphire workflow start mode=%s algorithm_version=%s stage_source=%s "
        "ds_callable=nextier_utils.labeling.sapphire.pipeline.run_well "
        "scope=(well=%s wells=%s fleet=%s include_fleets=%s exclude_fleets=%s pad=%s) "
        "range=(%s,%s) selected_wells=%s max_wells=%s max_stages=%s",
        mode, algorithm_version, stage_source, well_name, well_names, fleet_name,
        include_fleets or None, exclude_fleets or None, pad_name,
        format_dt(start_ts), format_dt(end_ts), len(wells), max_wells, max_stages,
    )
    if not wells:
        return {"run_id": run_id, "mode": mode, "results": []}

    results: list[dict[str, Any]] = []
    for selected_well in wells:
        # Per well, not per fleet: one failure must not take the rest of the run
        # with it, and a heavy fleet must not starve the wells behind it.
        try:
            results.append(await _process_well(
                workspace_id=workspace_id, run_id=run_id, mode=mode,
                source_datastore_key=source_datastore_key,
                stage_index_key=titanium_stage_index_featurestore_key,
                stage_summary_key=sapphire_stage_summary_featurestore_key,
                labels_key=sapphire_labels_featurestore_key,
                substage_index_key=titanium_substage_index_featurestore_key,
                manifest_key=sapphire_manifest_featurestore_key,
                well_name=selected_well, start_ts=start_ts, end_ts=end_ts,
                stage_source=stage_source, concentration_feature=concentration_feature,
                context_hours=context_hours, max_stages=max_stages,
                skip_completed=skip_completed, delete_existing=delete_existing,
                dry_run=dry_run, algorithm_version=algorithm_version,
                titanium_algorithm_version=titanium_algorithm_version,
            ))
        except Exception as exc:  # noqa: BLE001 -- one bad well must not end the run
            logger.exception("Sapphire well failed well=%s", selected_well)
            results.append({"well_name": selected_well, "status": "failed", "error": str(exc)})

    logger.info(
        "Sapphire workflow complete mode=%s wells=%s written=%s skipped=%s empty=%s failed=%s stages=%s",
        mode, len(results),
        sum(1 for r in results if r.get("status") == "written"),
        sum(1 for r in results if r.get("status") == "skipped"),
        sum(1 for r in results if r.get("status") == "empty"),
        sum(1 for r in results if r.get("status") == "failed"),
        sum(int(r.get("stages") or 0) for r in results),
    )
    return {"run_id": run_id, "mode": mode, "results": results}
