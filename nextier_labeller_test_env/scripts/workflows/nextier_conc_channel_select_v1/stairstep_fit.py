"""Stairstep segmentation on min-max normalized concentration shapes.

VENDORED VERBATIM from nextier-dash `src/analysis/stairstep_fit.py` at commit
696a11a7cf73c661806bae47d6527dc277e88caa ("Conc channel auto-select -- handoff").

Why vendored rather than imported: nextier-dash's own selector does
`from src.analysis.stairstep_fit import ...`, but its pyproject ships only
`packages = ["src/nextier_core", "src/nextier_utils"]` -- `src/analysis` is not
in the wheel. Any environment that has the wheel and not the source tree cannot
import it. `main.py` still PREFERS the upstream module when it is importable and
only falls back to this copy, so upstream stays the source of truth.

Do not edit to "improve" the gate. It is deliberately strict: a synthetic
perfect staircase is rejected as `poor_fit` (hmargin is 6e-4) while a noisy real
one passes. That strictness is why the `fallback_starts_at_0` path carries most
real wells.
"""
from __future__ import annotations

import numpy as np

L = 200
N_MIN, N_MAX = 4, 14
HMARGIN = 0.0006
MARGIN = 0.025
GRID = 200
RISER_MIN, RISER_MAX = 2, 14
SPAN_FRAC = 0.25
INC_FRAC = 0.35
DROP_MIN = 0.30
DROP_END = 0.30
START_FRAC = 0.05
MONO_MIN = 0.70


def resample_shape(s: np.ndarray, n: int = L) -> np.ndarray:
    s = np.asarray(s, dtype=np.float64).ravel()
    if s.size == n:
        return s
    if s.size < 2:
        return np.zeros(n)
    return np.interp(np.linspace(0, 1, n), np.linspace(0, 1, s.size), s)


def normalize_minmax(seg: np.ndarray) -> np.ndarray:
    seg = np.asarray(seg, dtype=np.float64)
    seg = seg[np.isfinite(seg)]
    if seg.size < 2:
        return np.zeros(L)
    lo, hi = float(seg.min()), float(seg.max())
    if hi - lo < 1e-9:
        return np.zeros(L)
    return resample_shape((seg - lo) / (hi - lo))


def _register_ramp(r, pos, nr):
    bounds = [0] + [int(p) for p in pos[:nr]] + [len(r)]
    out = np.asarray(r, dtype=np.float64).copy()
    for i in range(len(bounds) - 1):
        a, b = bounds[i], bounds[i + 1]
        if b <= a:
            continue
        out[a:b] = out[a:b] + (i / nr - float(r[a:b].mean()))
    return out


def _registered_residual(r, pos, nr, grid=GRID):
    reg = _register_ramp(r, pos, nr)
    u = np.linspace(0.0, 1.0, grid)
    mem = np.interp(u, np.linspace(0.0, 1.0, len(r)), reg)
    p = np.asarray(pos, dtype=np.float64) / (len(r) - 1)
    q = p.copy()
    if nr >= 3:
        q[1:-1] = p[0] + (p[-1] - p[0]) * np.arange(1, nr - 1) / (nr - 1)
    ref = np.zeros(grid)
    for k in range(nr):
        ref += (u >= q[k]) * (1.0 / nr)
    return float(np.mean((mem - ref) ** 2))


def _strictly_increasing(idxs, cap):
    out, prev = [], -1
    for v in idxs:
        v = int(v)
        if v <= prev:
            v = prev + 1
        out.append(min(v, cap))
        prev = out[-1]
    return out


def fit_stage(
    x,
    n_min=N_MIN,
    n_max=N_MAX,
    hmargin=HMARGIN,
    margin=MARGIN,
    drop_min=DROP_MIN,
    drop_end=DROP_END,
    start_frac=START_FRAC,
    mono_min=MONO_MIN,
    riser_min=RISER_MIN,
    riser_max=RISER_MAX,
    span_frac=SPAN_FRAC,
    inc_frac=INC_FRAC,
):
    """Detect risers in one L=200 min-max conc shape."""
    x = np.asarray(x, dtype=np.float64)
    base = dict(
        n_steps=-1,
        residual=np.nan,
        height_resid=np.nan,
        drop_mag=np.nan,
        ramp_start=-1,
        t_d=-1,
        mono_frac=np.nan,
        ambiguous=False,
        landmarks=[],
        increments=[],
        reject_reason="",
    )
    peak = float(x.max())
    if peak < 0.5 or (peak - float(x.min())) < 1e-6:
        return {**base, "reject_reason": "flat_no_signal"}

    above = np.where(x >= start_frac * peak)[0]
    ramp_start = int(above[0]) if above.size else 0
    high = np.where(x >= 0.5 * peak)[0]
    t_d = min((int(high[-1]) + 1) if high.size else len(x), len(x) - 1)
    tail = x[int(0.75 * len(x)) :]
    end_drop = float(tail.max() - x[-1])
    base.update(ramp_start=ramp_start, t_d=t_d, drop_mag=round(end_drop, 4))
    if end_drop < drop_min or x[-1] > drop_end:
        return {**base, "reject_reason": "no_drop"}
    if t_d - ramp_start < n_min + 2:
        return {**base, "reject_reason": "too_short"}

    r = x[ramp_start:t_d].copy()
    rr = float(r.max() - r.min())
    if rr < 1e-6:
        return {**base, "reject_reason": "flat_no_signal"}
    r = (r - r.min()) / rr
    mono_frac = float((np.diff(r) >= -0.03).mean()) if r.size > 1 else 1.0
    base["mono_frac"] = round(mono_frac, 3)
    if mono_frac < mono_min:
        return {**base, "reject_reason": "not_monotone"}

    s_star, hres = None, np.nan
    for N in range(n_min, n_max + 1):
        s = np.maximum.accumulate(np.clip(np.rint(r * N).astype(int), 0, N))
        h = float(np.mean((r - s / N) ** 2))
        if h <= hmargin:
            s_star, hres = s, h
            break
    if s_star is None:
        return {**base, "reject_reason": "poor_fit"}

    levels = np.unique(s_star)
    base["height_resid"] = round(hres, 6)
    lv = np.array([float(r[s_star == Lv].mean()) for Lv in levels])
    wd = np.array([int(np.sum(s_star == Lv)) for Lv in levels])
    span_thr = max(2.0, span_frac * float(np.median(wd)))
    inc_thr = inc_frac * float(np.median(np.diff(lv))) if len(lv) > 1 else 0.0
    keep = [0]
    for k in range(1, len(levels)):
        if wd[k] < span_thr:
            continue
        if lv[k] - lv[keep[-1]] < inc_thr:
            continue
        keep.append(k)
    pos = [int(np.where(s_star >= levels[k])[0][0]) for k in keep[1:]]
    nr = len(pos)
    incs = [round(float(lv[keep[i]] - lv[keep[i - 1]]), 4) for i in range(1, len(keep))]
    if nr < riser_min or nr > riser_max:
        return {**base, "n_steps": nr, "increments": incs, "reject_reason": "n_out_of_range"}

    res = _registered_residual(r, pos, nr)
    risers = _strictly_increasing([pi + ramp_start for pi in pos], t_d - 1)
    out = {**base, "n_steps": nr, "residual": round(res, 6), "landmarks": risers, "increments": incs}
    if res > margin:
        out["reject_reason"] = "residual_high"
    return out
