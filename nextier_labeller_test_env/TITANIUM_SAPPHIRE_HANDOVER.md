# Titanium + Sapphire — implementation handover

**Status: incomplete. Sapphire runs end to end and writes, but most landmarks still
abstain and the third Titanium tier is currently a copy of the second.** Read
[Where it actually stands](#8-where-it-actually-stands) before trusting any of it.

Written for someone — or something — with no prior context on this system. It covers
the stage/substage detection work only.

---

## 1. The domain in one page

A **frac fleet** pumps fluid and sand into a well at high pressure to fracture rock.

- **Well** — one drilled hole. `HAWKEYE 33-1324H`.
- **Stage** — one pumping cycle on that well: open up, pump, shut down. Typically
  **40–60 minutes**. A well goes through 20–70 of them over weeks, with long idle
  gaps between.
- **Substage** — a moment *inside* one stage: when the well actually opened, when the
  pad ended, when slurry stopped, when the well closed.

The raw signal is a time series per well: **slurry rate** (bbl/min), **mainline
pressure** (psi), **proppant concentration** (several competing channels), sampled
every few seconds.

Everything below is about turning that signal into stages and substages.

---

## 2. Two independent algorithm families

This is the single most important thing to understand, and the most common source of
confusion.

| | **Bronze → Copper → Silver** | **Titanium → Sapphire** |
|---|---|---|
| Stage boundaries from | human/derived labels | telemetry only |
| Substages from | auto FSM on copper tips | post-stage placement on titanium |
| Entry point | `auto_layer.py` | `titanium_layer.py` + `sapphire_layer.py` |
| Stage index read | `nextier_copper_stage_index_v1` | `nextier_titanium_stage_index_v1` |

They are **parallel chains that never touch**. They are not stages of one pipeline
and the metals are not a quality ranking.

Within the Titanium/Sapphire chain the two *are* interdependent: Sapphire cannot run
until Titanium has closed a stage, and Titanium's finest tier is built back out of
Sapphire's output.

**The substage work labelled `aug27` (`nextier_substage_silver_aug27_*`) belongs to
the Copper chain, not to Sapphire.** It runs `nextier_auto_labeling_v1/main.py`
against `nextier_copper_stage_index_v1`. It is not an earlier version of Sapphire and
shares no code with it.

---

## 3. Titanium — coarse stage detection

Turns telemetry into numbered stages with no labels of any kind.

Pipeline: `detect → merge → continuity → assign_first / assign_final`.

It emits the **same stages three times**, at increasing confidence. The names were
changed recently and the old ones are still in circulation:

| Current | Former | Meaning | Priority to the business |
|---|---|---|---|
| `titanium_first` | provisional | Called live while pumping; may be withdrawn | 3rd |
| `titanium_final` | confirmed | Called post-stage, authoritative | 2nd |
| `titanium_substage` | continuous | `final`, re-cut to Sapphire's landmarks | **1st** |

**Priority order is not execution order.** `titanium_substage` is *derived from*
`final`, which is derived from raw telemetry. Execution is always
final → Sapphire → refined, regardless of which output matters most. Only
`titanium_first` is genuinely a live product.

**Status: complete and running.** Three Prefect deployments (live `*/5`, background
`*/30`, historical `*/5`) have processed the full well set. **Titanium must not be
re-run** — the runs were expensive over a large dataset. Anything Sapphire needs must
be obtained without re-running Titanium's segmentation.

---

## 4. Sapphire — substage landmark placement

Places **seven landmarks** on a **closed** Titanium stage window, in order:

```
open_well → stage_start → pad_end → ttr → slurry_end → stage_end → close_well
```

Three properties that drive every design decision downstream:

1. **Post-stage only.** It runs on `on_stage_closed`. Landmarks placed on a window
   still filling would move as more data arrives.
2. **Stages must be fed in order, with no gaps.** `SapphireLayer` keeps causal
   look-left memory. Feeding stage 5 without stage 4 does not fail — it produces
   confident, wrong landmarks that nothing downstream can detect.
3. **It abstains rather than guessing**, and returns *why* as a string
   (`"no causal rate reference yet"`). An abstention is a recorded decision, not an
   error. Preserving these strings is the single most useful diagnostic in the system.

### The causal rate reference — read this before debugging anything

Sapphire will not place `stage_start` until it has a picture of a "normal" stage on
this well. That picture is built from **three numbers per previously closed stage**:

- peak rate (p95)
- peak pressure (p95)
- **sand mass (klb)**

`CausalStageStats.record(p95_rate, p95_press, mass_klb)` accumulates them;
`hist_rate()` derives the reference. **If sand mass is NaN, the reference never
forms**, `stage_start` abstains with `"no causal rate reference yet"`, and
`open_well`, `pad_end`, `ttr` and `slurry_end` all abstain after it because they are
measured *from* `stage_start`. One missing input, six failures.

---

## 5. `titanium_substage` — the composite third tier

Not a third algorithm. There is no third core module.

```
[1] titanium_final windows          (coarse t0, t1)
[2] Sapphire places landmarks on them
[3] windows re-bound:  start = open_well or coarse t0
                       end   = close_well or coarse t1
```

`pad_end`, `slurry_end`, `stage_start` and `stage_end` deliberately **do not** move
these boundaries. Only `open_well` and `close_well` do.

It is a **stage** tier — its rows are stage windows, not substage labels — which is
why it is named in the `titanium_*` family and renders on the *stage* dashboard as a
third layer beside `titanium_first` and `titanium_final`.

---

## 6. Repositories and how code reaches production

| Repo | Contains |
|---|---|
| `neuralix-ai/nextier-dash` | The algorithms: `src/nextier_core/*.py`, `src/nextier_utils/labeling/**` |
| `neuralix-ai/nextier-test-templates` | The nixdlt template: workflows, featurestores, metrics, dashboards |
| `neuralix-ai/nix-dlt` | The platform itself (Prefect worker image, parser, API) |

### How an algorithm gets into a Prefect job

**Not a file copy.** `nix-dlt/Dockerfile.prefect-worker` pip-installs the whole
package from a pinned git SHA:

```dockerfile
uv pip install "nextier-dash @ git+ssh://git@github.com/neuralix-ai/nextier-dash.git@<SHA>"
```

Workflows then do a lazy import inside the compute function:

```python
from nextier_core.titanium_layer import run_titanium_layer      # titanium
from nextier_utils.labeling.sapphire.pipeline import run_well   # sapphire
```

**Changing an algorithm therefore means: merge into nextier-dash → push → bump the
SHA in the Dockerfile → rebuild and redeploy the worker image.** A stale image is the
first thing to suspect on `ModuleNotFoundError`.

### The `fix/package-safe-imports` branch

`main` in nextier-dash writes `from src.nextier_utils...`, which only resolves from
the repo root. Once pip-installed, the package root is `src/`, so the prefix must go:
`from nextier_utils...`. The branch `fix/package-safe-imports` strips it.

**Merging `main` into that branch is a recurring chore with a specific recipe**, used
five times now:

1. `git merge origin/main`
2. Resolve every conflict by taking **main's content**, then stripping the `src.`
   prefix from its imports.
3. Also strip it from files that merged **cleanly** — they arrive still carrying it.
   The invariant is: `git grep -E '^\s*(from|import)\s+src\.' -- 'src/*.py'` returns
   **zero** results.
4. Scripts **outside** `src/` (`deliverables/`, `experiments/`) are deliberately left
   alone — they run from the repo root with `PYTHONPATH=.`.
5. Comments and docstrings mentioning `src.nextier_*` get the same treatment.

Verify with `PYTHONPATH=src python -c "from nextier_core.sapphire_layer import run_sapphire_layer"`.

---

## 7. What was built

Titanium was already deployed. **Sapphire did not exist in the template at all** —
zero references. That, not the well list, was the work.

### Priority wells

`wells - wells.csv.csv` (nix-dlt repo root) lists 21 rows → **17 distinct wells**
(three are repeated) across fracs 4, 8, 76, 81, 84. Twelve of the seventeen sit on
fracs 08 and 76, the two heaviest — which is why runs are pinned by `well_names`
rather than partitioned by fleet.

### Template objects (v2 — the current set)

```
scripts/workflows/nextier_sapphire_substage_orchestration_v2/main.py

types/featurestores/
  nextier_substage_sapphire_sept3_stage_summary_v2         42 cols, pk sapphire_stage_uid
  nextier_substage_sapphire_sept3_labels_v2                21 cols, pk sapphire_label_id
  nextier_titanium_substage_index_sept3_v2                 20 cols, pk stage_uid
  nextier_substage_sapphire_sept3_processing_manifest_v2   20 cols, pk manifest_id

types/workflows/
  nextier_substage_sapphire_sept3_priority_wells_v2   manual, 17 wells pinned
  nextier_substage_sapphire_sept3_historical_v2       manual
  nextier_substage_sapphire_sept3_background_v2       */30 * * * *
  nextier_substage_sapphire_sept3_live_v2             */5  * * * *
```

Naming follows the existing `nextier_substage_silver_aug27_*` convention with `sept3`
as the vintage. `algorithm_version = nextier_sapphire_substage_v2`.

**A `_v1` set exists and is dormant.** It was left in place deliberately (see
[§9](#9-bugs-found-and-how-they-were-fixed)); its tables are never written to. Do not
delete it without checking nothing references it.

### How the workflow runs one well

```
1. read CLOSED titanium stages for the well from nextier_titanium_stage_index_v1
   (both bounds present on the chosen split, ordered by stage_num)
2. resume at the first stage with no placements; take everything after it
3. load telemetry spanning those stages ± context_hours
4. rename record_ts -> datetime_fmt
5. titanium_merge(spans=<stored windows>)  ->  per-stage sand mass
6. run_well(stages=..., stage_masses=...)  ->  placements
7. placements_to_labels_df(...)            ->  row-level labels
8. derive refined windows from the placements
9. write stage summary, refined windows, labels, manifest
```

### Design decisions worth preserving

**Stage-driven, not time-chunk-driven.** Titanium sweeps fixed time chunks looking
for data; Sapphire cannot, because it needs whole closed stages. It reads the stage
list first, then fetches only the matching telemetry. *Otherwise a chunk boundary
would routinely cut a stage in half and Sapphire would place landmarks on a fragment
believing it had the whole thing.*

**Never skip a stage in the middle** (`_contiguous_pending`). Work resumes from the
first unplaced stage and continues to the end, recomputing and upserting any later
stages already done. *Otherwise look-left memory is corrupted silently.*

**Delete only what is about to be rewritten** (`_delete_outputs`), scoped to the stage
range being recomputed. *Otherwise a run resuming at stage 40 wipes 1–39 and never
writes them back.*

**Wells cheapest-first, one at a time**, each in its own try/except. *Otherwise fracs
08/76/96 consume an entire run, and one bad well takes down every well behind it.*

**Masses via `titanium_merge(spans=...)`, not a Titanium re-run.** The sand-mass
integral is **window-local** — it only sums samples inside each span — so passing the
stored windows returns their masses without recomputing the segmentation. Verified
23-in/23-out with windows unchanged on `HAWKEYE 33-1324H`.

**`_usable_boundary` guard.** A landmark is accepted as a window boundary only if it
falls within the coarse window ± one stage-duration. When it is rejected, the source
column records `coarse_t1_landmark_rejected` rather than plain `coarse_t1`, so a
rejection is never mistaken for an abstention. *This is currently firing on every row
— see [§8](#8-where-it-actually-stands).*

---

## 8. Where it actually stands

Last real run: **1 well (`HAWKEYE 33-1324H`), 10 stages, historical mode.** Wrote 10
rows to the stage summary and 10 to the refined index.

```
placements=10  windows=10  labels=0
masses: 10/10 finite, 153.5 – 722.0 klb

open_well     0/10    "no qualifying pressure fall before SS"
stage_start   9/10    stage 1: "no causal rate reference yet"  (expected — no history)
pad_end       3/10
ttr           0/10    "design rate never sustained after"
slurry_end    0/10    "bad window"
stage_end     0/10    "no slope contrast in the 5 min before the anchor"
close_well   10/10    placed, but ALL rejected by the boundary guard

refined windows:  start=coarse_t0   end=coarse_t1_landmark_rejected   n=10
```

**Read that honestly:**

- The mass fix worked. `stage_start` went from 0/10 to 9/10, and the downstream
  reasons changed from `"no stage_start to ..."` to real detector reasons.
- **`titanium_substage` is currently identical to `titanium_final`** — every window
  fell back to coarse on both ends. It is not yet delivering anything.
- **`labels=0`.** Row-level labels are derived from placed landmarks; too few place.
- **`close_well` is placed on every stage and rejected on every stage**, because of
  the unit bug in §9. Fixing that alone would make the refined tier's *end* boundary
  real.

### A discrepancy not yet explained

Running the **same well** locally over **all 23 stages** gives very different results
from the cluster's 10-stage run:

| | local, 23 stages | cluster, 10 stages |
|---|---|---|
| open_well | 22/23 | **0/10** |
| stage_start | 22/23 | 9/10 |
| pad_end | 21/23 | 3/10 |
| stage_end | 22/23 | **0/10** |
| labels | 1,699 | **0** |

Both supply masses. Candidate causes, none confirmed: the number of stages fed
(more history ⇒ better references), the telemetry window loaded (the workflow adds
`context_hours` of lead-in; the local frame did not), or a frame difference
introduced by `prepare_raw_frame`. **This is the first thing to investigate.**

---

## 9. Bugs found and how they were fixed

Chronological. Several were only caught by running against real data.

**1. Sapphire absent from the template.** Assumed present; it was not. Built from
scratch.

**2. `KeyError: 'datetime_fmt'`.** The platform's telemetry column is `record_ts`;
Sapphire indexes on `DATETIME_COL` and reads it *by constant, not by parameter*, so
`rate_col`/`pressure_col` being configurable does not help. Fixed with
`raw.rename(columns={"record_ts": DATETIME_COL})`, exactly as Titanium does one line
before its own call.

**3. `abstain` is a reason string, not a boolean.** Sapphire returns
`"no causal rate reference yet"` or `None`. v1 declared BOOLEAN and did
`bool(x or False)`, destroying the reason.

**4. The platform schema is append-only.** `JSONSchemaTypeValidator` rejects **both**
a dropped key and a changed type. So the boolean could not be fixed in place, and
could not be renamed either — v1 had to carry both a useless boolean *and* the reason
column. **This is why the `_v2` set exists**: a fresh key has no publish history, so
it carries only `<landmark>_abstain_reason` (TEXT; NULL means placed).

> **Corollary for anyone changing these stores:** getting a column type wrong is
> permanent. Check the algorithm's real output shape locally before the first publish.
> Removing a featurestore from the template deletes the *record*, never the *table*,
> and re-adding runs `CREATE TABLE IF NOT EXISTS` — so a delete/re-add cycle silently
> leaves the old table in place with the old shape.

**5. Surrogate keys.** v1 keyed labels on `telemetry_point_id`, so a second algorithm
version would upsert over the first. Now `sapphire_label_id =
telemetry_point_id:algorithm_version`, matching the aug27 `auto_label_id` pattern,
with `telemetry_point_id` kept as a plain column.

**6. Off-by-one between three stage numbering schemes.** Placement rows use **0-based**
`stage_n`; `placements_to_labels_df(one_based_stage=True)` produces **1-based**
`stage_n`; neither is the Titanium ordinal. Now mapped explicitly through a
position→ordinal dict. Both errors are silent — labels simply attach to the wrong
stage.

**7. `label_kind` had no source.** The labels frame has only
`['name','stage_n','datetime_fmt','substage']`. Dropped in v2 rather than shipping a
permanently-null column.

**8. Trigger Run modal ignored deployment parameters.** The platform builds that modal
from **`paramsSchema` defaults**, not `deployment.parameters`. One shared schema meant
every manual trigger opened as a background run over all wells, silently ignoring the
17 pinned ones. Each workflow now carries its own schema with its own defaults.

**9. Missing `stage_masses` — the big one.** `run_well` sets `mass_klb = NaN` for
every stage when `stages` is supplied without `stage_masses`. That starves the causal
rate reference and cascades into six abstentions. `run_sapphire_layer` (the core entry
point) **has no `stage_masses` parameter and cannot forward one**, which is why v2
calls `run_well` directly — the same driver the handoff doc's own `titanium_substage`
example uses. Measured effect on 23 real stages: **44 → 109 landmarks placed.**

---

## 10. Open issues

**A. `close_well` timestamps are corrupt — bug in nextier-dash, not in this template.**

`sapphire/pipeline.py` does:

```python
row[f"{key}__t"] = pd.Timestamp(t) if t is not None else pd.NaT
```

On the `close_well__src = "prior"` / `"settled"` paths, `t` is a **microsecond** epoch
integer, and `pd.Timestamp(int)` reads a bare integer as **nanoseconds** — giving
`1970-01-21 14:57:43` for a 2026 stage. Read correctly, `1781863911000000 µs` is
`2026-06-19 10:11:51`, which is 4.6 minutes after that stage's coarse end: exactly
where a `close_well` belongs. **The number is right; the unit is wrong.** Fix belongs
in nextier-dash (`pd.to_datetime(t, unit="us")` or returning a Timestamp). Until then
the boundary guard rejects every one, and the refined tier gets no real end boundary.

**B. `ttr` / `slurry_end` / `stage_end` / `open_well` abstain on real detector
grounds.** `"design rate never sustained after"`, `"bad window"`, `"no slope contrast
in the 5 min before the anchor"`, `"no qualifying pressure fall before SS"`. These are
tuning or data questions for the data-science team, not template bugs.

**C. The local/cluster discrepancy in §8.** Unexplained.

**D. `concentration_feature` is decorative on the Sapphire side.**
`run_sapphire_layer` used it only to trigger a `.copy()`; Sapphire picks its channel
via `CONC_COL_ORDER`. `run_well` takes `conc_cols` (a tuple of candidates), not a
single column. The workflow parameter should either be wired to `conc_cols` or removed.

**E. No dashboard shows Sapphire.** Nothing in `types/dashboards/` references it.
Each substage model on the Substage Detection dashboard is a **pair of metrics** —
see `merged_detection_substage_silver_aug27_{windows,timeseries}_v1`, which read
`nextier_copper_stage_index_v1` + the aug27 labels store. Sapphire needs the
equivalent pair reading `nextier_titanium_stage_index_v1` + the sapphire labels store,
then wiring into `nix_substage_detection_live_v1`. Separately, `titanium_substage`
needs a layer on `nix_stage_segmentation_merged_v1`.

**F. The tier metrics are half-renamed.** `nextier_copper_provisional_stage_windows_v1`
reads copper **and** titanium and uses the literal `'first'`;
`..._continuous_...` reads both; `..._confirmed_...` still reads **copper only** and
was never repointed. Keys still say copper/provisional/confirmed/continuous while the
SQL has partly moved to titanium/first/final. **Any comparison of "last week vs this
week" will show differences that are naming, not algorithm.** Settle this before
adding a `titanium_substage` metric on top.

---

## 11. Running it

Prefect UI: `http://20.118.224.210:4200`. Datasink tables carry a `_wid2` suffix in
the deployed workspace.

**Order matters.** Deploy Priority Wells first; Background and Live have crons and
start firing the moment they are deployed, which interleaves them with any backfill.

1. **Publish the template.** Then rebuild the Prefect worker image if the nextier-dash
   SHA in `Dockerfile.prefect-worker` changed.
2. **Smoke test** — Priority Wells V2, `dry_run` **on**, `Max Wells` 1, `Max Stages` 3.
   Look for:
   ```
   Sapphire compute start ... masses=3 finite_masses=3 ...
       ds_callable=nextier_utils.labeling.sapphire.pipeline.run_well
   Sapphire compute complete ... placements=3 windows=3 labels=<n>
   ```
   `finite_masses=0` or a `stage-mass MISALIGNED` warning means bug 9 has returned.
   `ModuleNotFoundError` means a stale worker image.
3. **One well for real** — `dry_run` off, `Max Wells` 1, `Max Stages` clear.
4. **All 17** — clear `Max Wells` too.
5. **Background**, let one cycle land, then **Live**.

`skip_completed: true` makes re-runs cheap; `delete_existing: true` is scoped to the
recomputed stage range.

### Verification queries

```sql
-- what placed vs abstained
SELECT COUNT(*) AS stages,
       COUNT(open_well_ts) AS open_well, COUNT(sapphire_stage_start_ts) AS stage_start,
       COUNT(pad_end_ts) AS pad_end,     COUNT(ttr_ts) AS ttr,
       COUNT(slurry_end_ts) AS slurry_end,
       COUNT(sapphire_stage_end_ts) AS stage_end, COUNT(close_well_ts) AS close_well
FROM nextier_substage_sapphire_sept3_stage_summary_v2_wid2;

-- why anything abstained (NULL = placed)
SELECT stage_num, mass_klb, conc_col,
       sapphire_stage_start_abstain_reason, open_well_abstain_reason,
       ttr_abstain_reason, slurry_end_abstain_reason
FROM nextier_substage_sapphire_sept3_stage_summary_v2_wid2 ORDER BY stage_num;

-- is the third tier real, or a copy of titanium_final?
SELECT start_src, end_src, COUNT(*)
FROM nextier_titanium_substage_index_sept3_v2_wid2 GROUP BY 1,2;
--   open_well / close_well          -> real refinement
--   coarse_t0 / coarse_t1           -> landmark abstained
--   *_landmark_rejected             -> landmark placed but out of range (bug A)

-- coverage across the priority wells
SELECT well_name, COUNT(*) AS stages, COUNT(sapphire_stage_start_ts) AS placed
FROM nextier_substage_sapphire_sept3_stage_summary_v2_wid2
GROUP BY well_name ORDER BY well_name;
```

---

## 12. Reference

- `nextier-dash/docs/titanium_sapphire_handoff.md` — the algorithm-side handoff
- `nextier-dash/docs/PREFECT_COMPONENT_HANDOFF.md` — which modules may be shipped
- `nextier-test-templates` commit `da259f1` — the reference example of an algo change
  across workflow script, featurestores, metrics, workflow configs and dashboard
- `nextier_substage_silver_aug27_*` — the Copper-chain substage set these stores are
  modelled on
