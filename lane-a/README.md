# Lane A

Owner: streaming spine — synthetic generator, ingest pipeline, Lakebase, CDF, alerting.
Everything lands in the one shared schema `jack_freeman_catalog.tech_summit_scada_build`
(see the table-ownership map in `../contract/naming.md`).

This directory is a **Databricks Asset Bundle** (`databricks.yml`). Nothing here is
deployed yet — the files are authored assets only.

## Files

| File | Step | What it does |
|---|---|---|
| `src/01_generate_seed_data.py` | 1 | Populates `tag_reading_seed` (24 h) and `vibration_features_seed` (120 d) — the unblock for lanes B and C. **Already run.** |
| `src/create_lakebase_project.sh` | 2 | One-time: creates the `ironbark-ops` Lakebase project (DABs can't). |
| `src/lakebase_setup.py` | 2 | Applies `contract/ddl_lakebase.sql` to the `plant` schema (tables, replica identity, event trigger). |
| `src/replay_snapshot.py` | 2 | Looped snapshot micro-batch replaying the seed → upserts Lakebase `plant.tag_current` (continuous serverless job). |
| `resources/*.job.yml` | — | Job definitions for the three notebooks above. |
| `databricks.yml` | — | Bundle config: variables + `dev`/`prod` targets. |

## Step 2 — real-time replay to Lakebase (this bundle)

The whiteboard's pipeline 1 replays the seed in **wall-clock time** and upserts
`plant.tag_current`; Lakebase CDF then carries the change history back to UC.

**Design note — why a looped snapshot, not a streaming query.** Real-time mode
(`trigger(realTime=)`) can't read a Delta table (only Kafka/MSK/Kinesis/Event Hubs), and
this workspace is **serverless-only** where a blocking `awaitTermination()` trips the kernel
watchdog. So the replay is a *completing* job that loops: every `interval_seconds` it maps
wall-clock time onto the 24 h seed loop, takes the latest reading per tag at that replay
position, and upserts all 151 tags into `plant.tag_current`. `loop_seconds` defaults to
`86400`, so a single run replays the full 24 h loop and stays up all day; the **continuous**
job restarts it only if it exits or fails, keeping `tag_current` live.
Stateless (position is a pure function of wall time), so restarts need no shared state. Auth
is the driver's OAuth identity (SDK-minted token, refreshed). Verified end-to-end: 151 tags →
`tag_current` → CDF → `lb_tag_current_history` (~18 s flush).

### Bring it up
```bash
# 0. one-time: the Lakebase project already exists (lane B created it); the DDL is applied.
#    (src/create_lakebase_project.sh is idempotent if you ever need it.)

# 1. validate + deploy (the ingest job is created PAUSED)
databricks bundle validate -t dev --profile tech-summit
databricks bundle deploy   -t dev --profile tech-summit

# 2. start the live replay (continuous, unpaused)
databricks bundle deploy -t dev --var stream_pause_status=UNPAUSED --profile tech-summit
```

Knobs (bundle variables in `databricks.yml`): `replay_speed` (default `1.0` = true real
time), `interval_seconds` (`5`), `loop_seconds` (`86400` = a full 24 h replay loop per run;
the continuous job restarts a run only if it exits or fails), and the Lakebase/`plant` coordinates. A live-demo fault hook reads an optional
`scratch_a_fault_control` table `{tag_id, multiplier}` and scales that tag's value in real time.

## How the seed data is generated

A **physics simulation** — no dbldatagen. A single time-varying plant load factor `L(t)`
scales every flowsheet arc, so every unit node closes by construction. The physics (small,
stateful: the load factor, desands slurry back-calculated from the contract `Cw` formula,
integrated bin levels, the surge bin's derived outflow) runs in numpy on the driver; the
bulk stateless noise (independent tags, 10 Hz vibration) is generated distributed in Spark.
The mass balance actually closes — validation cells at the end prove per-node imbalance and
reconstruct the desands 2100/400 t/h split via `Cw`.

## Running `01_generate_seed_data.py`

It is a Databricks notebook (source format). Import it into the workspace and run on a
cluster **with Spark** (classic or serverless) — a SQL warehouse will not run it.

It's wired as the `a_generate_seed` job in the bundle. Or run it directly by importing
`src/01_generate_seed_data.py` into the workspace / opening it in the Databricks editor.

It is **re-runnable**: each section does `INSERT OVERWRITE`, so it refills the seed tables
without ever `CREATE OR REPLACE`-ing them (the tables are owned by the contract).

### Knobs (top config cell)
- `VIBRATION_HZ` (default `10.0`) — spec-compliant, but produces ~13M vibration rows. Set
  to `1.0` for a lighter seed; no impact on the mass balance (1-min) or the PdM model
  (trains off `vibration_features_seed`), only on the app's live 10 Hz vibration tiles.

## What this seed contains (new contract requirements)
- **Limit excursions:** 15–25 warning + 4–6 trip (incl. a crusher-pressure choke and a bin
  high-level), ramping in/out, ≥8 distinct tags, ≥1 sustained into the final 30 minutes.
- **Instrument quality:** ~99.5% GOOD; one tag STALE for minutes (fires R05); one node
  forced to `INSUFFICIENT_DATA`.
- **Current timestamps:** the 24 h window ends at generation time; the 120 d vibration
  history ends where the 24 h window begins (contiguous).
- **Vibration features:** contract column names, the four trend features
  (`rms_delta_1h/24h`, `rms_slope_24h`, `rms_pct_of_baseline`), `hours_since_maintenance`,
  and the four injected faults (crest/kurtosis leading RMS) with `label_failure_30d`.
