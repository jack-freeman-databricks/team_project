# Data contract: Ironbark iron ore crushing plant

Agreed in the phase 0 session. **Change this file by agreement, not unilaterally.**
Every Claude session working on this build should read it first.

Scenario: Ironbark Resources, Pilbara iron ore. Dry crushing and screening circuit
with a wet desands circuit, feeding a product stockpile via a stacker. Design basis
2,500 t/h ROM feed, 2,100 t/h product, 400 t/h fines rejected. The tertiary crusher
runs a 250 t/h recirculating load back to the secondary screen, which is what makes
the mass balance worth visualising.

## Unity Catalog naming

**Everything lives in one existing schema: `jack_freeman_catalog.tech_summit_scada_build`.**

Two workspace facts forced this, both verified in phase 0:

* Default Storage is enabled on the account and the metastore has no storage root,
  so `CREATE CATALOG ironbark` fails outright. A catalog created through the UI would
  sit on Default Storage, which Lakebase CDF does not support.
* `jack_freeman_catalog` has an external S3 storage root, so it **is** CDF-compatible,
  and `tech_summit_scada_build` already exists there and is empty.

| Object | Name |
|---|---|
| Catalog | `jack_freeman_catalog` |
| Schema (everything) | `tech_summit_scada_build` |
| Landing volume | `jack_freeman_catalog.tech_summit_scada_build.landing` |
| Lakebase project | `ironbark-ops` |
| Lakebase database / schema | `databricks_postgres` / `plant` |
| Registered model | `jack_freeman_catalog.tech_summit_scada_build.vibration_pdm` |
| Serving endpoint | `ironbark-pdm` |
| App | `ironbark-control-room` |

Conventions:

* `snake_case` throughout. Dimensions prefixed `dim_`, views `vw_`, Lakebase CDF
  destinations arrive as `lb_<source_table>_history` (do not rename).
* Aggregates carry their grain as a suffix: `_1m`, `_10m`.
* Pipelines, jobs, endpoints and apps are prefixed with the owner's lane letter until
  after seam 2, e.g. `a_ironbark_ingest`. Drop the prefix at integration.

## Table ownership: the only thing stopping us overwriting each other

There is **one schema**, so there is no schema-level boundary to protect us. This table
is the boundary instead. Do not create, replace or drop anything outside your lane.

| Object | Owner | Notes |
|---|---|---|
| `dim_tag`, `dim_flowsheet_node`, `dim_flowsheet_arc`, `dim_rule` | phase 0 | **Frozen.** Never `CREATE OR REPLACE` these. |
| `landing` volume | phase 0 | |
| `tag_reading_seed`, `vibration_features_seed` | lane A | Written once, then left alone. |
| `tag_reading`, `lb_*_history` | lane A | Bronze and the CDF destinations. |
| `vw_tag_reading` | lane A | The seam. Lane A repoints it; B and C only read it. |
| `vw_vibration_features` | lane A | Second seam, for training. |
| `tag_reading_1m`, `mass_flow_1m`, `mass_balance_node_1m`, `equipment_state_1m`, `desands_balance_1m` | lane B | Lakeflow owns these; do not hand-create. |
| `alert`, `stockpile_state` | lane B | |
| `vibration_features_10m`, `pdm_prediction`, `vibration_pdm` | lane B | |
| `stub_*` | lane C | App stubs. Kept as a demo fallback, never deleted. |
| `scratch_a_*`, `scratch_b_*` | A and B | Personal scratch. Nobody reads anyone else's. |

Lane C writes nothing except alarm acknowledgements to Lakebase.

## Tag ID convention

```
<ASSET_ID>-<INSTRUMENT_TYPE><SEQ>          e.g.  CRU-PCR01-VI01
<AREA>-<UNIT><NN>                          asset id, e.g. CRU-PCR01
```

Areas: `ROM` reclaim, `CRU` crushing, `SCR` screening, `DSD` desands,
`STK` stacking and stockpiles, `UTL` utilities, `LAB` laboratory.

Instrument types follow ISA-5.1 loosely:

| Code | Measure | Unit | Code | Measure | Unit |
|---|---|---|---|---|---|
| `WI` | mass flow | t/h | `TI` | temperature | degC |
| `SI` | belt speed | m/s | `VI` | vibration | mm/s |
| `RI` | rotational speed | rpm | `FI` | volume flow | m3/h |
| `LI` | level | pct | `DI` | density | t/m3 |
| `PI` | pressure | kPa | `QI` | assay | pct |
| `II` | motor current | A | `MI` | moisture | pct |
| `GI` | gap / CSS | mm | `XI` | position | deg |
| `ZI` | discrete state | enum | | | |

The register is generated, not hand-maintained: `gen_reference_data.py` emits
`dim_tag.csv` (121 tags, 25 assets), `dim_flowsheet_node.csv`,
`dim_flowsheet_arc.csv` and `dim_rule.csv`. It self-checks that every unit node in
the flowsheet closes and that the wet and dry sides of the balance reconcile, so it
fails loudly if someone edits a tonnage without editing its counterpart. Regenerate
rather than editing the CSVs by hand.

## Physical constants (both lanes must use the same values)

```
SOLIDS_SG = 4.9          # hematite solids specific gravity, t/m3
```

Dry solids tonnage from a slurry flow and density pair:

```
Cw    = SOLIDS_SG * (rho - 1) / (rho * (SOLIDS_SG - 1))     # solids mass fraction
t/h   = Q_m3h * rho * Cw
```

If lane B and the app use different values for `SOLIDS_SG` the desands node will
never close and it will look like a pipeline bug. It is in the contract for a reason.

## How the whiteboard maps onto what the platform actually does

Verified against current docs (Connect to Lakebase, Lakebase Change Data Feed,
Real-time mode concepts) rather than from memory. The short version: **the
whiteboard is buildable almost exactly as drawn.** One box has to change what it
is, not what it does.

### Pipeline 1 is a standalone Structured Streaming job, not a Lakeflow pipeline

There **is** a native Lakebase sink, `format("postgresql")`, in Public Preview, and it
**does** support `trigger(realTime=...)` for sub-second writes. It manages
credentials, batching, retries on transient JDBC errors, and backpressure for us. It
upserts with `INSERT ... ON CONFLICT`, inferring the key from the target table's
primary key, and auto-creates the table if it is missing.

The one hard constraint: *"Serverless compute and Lakeflow pipelines are not
supported."* So the whiteboard's `SDP-RTM -> Lakebase sink` cannot be a declarative
pipeline. It has to be a standalone Structured Streaming query on **classic compute**
(dedicated or standard access mode), **DBR 18 or later**.

That is a better outcome than it sounds, and it changes the shape of lane A's work:

| | Recommended | Fallback |
|---|---|---|
| Pipeline 1 | standalone Structured Streaming, `format("postgresql")`, `trigger(realTime=...)` | Lakeflow pipeline, continuous, `@dp.foreach_batch_sink` |
| Latency to Lakebase | sub-second | low seconds |
| Auth | managed, runs as the query's identity | hand-rolled, and the token expires hourly |
| Batching, retries, backpressure | built in | hand-rolled |
| Compute | classic only, DBR 18+ | serverless fine |
| Take it when | classic compute and the preview are available | they are not |

Two queries, matching the whiteboard's two arrows out of pipeline 1:

* **Q1**, native Lakebase sink, all readings upserting `plant.tag_current`.
* **Q2**, rule failures only, using a custom `foreach` sink so one writer can both
  POST to the alerting endpoint and insert into `plant.alert_outbox`. Note that an
  executor-side `foreach` sink needs native Postgres password auth from a secret
  scope, because OAuth refresh needs SDK context executors do not have.

Lakeflow keeps its place as **pipeline 2**, the analytical fan-out, where declarative
genuinely earns it: materialized views, expectations, incremental refresh, dependency
management. Both technologies end up in the demo doing what each is actually best at,
which is a better story than forcing one to do both.

### Lakebase Change Data Feed is scriptable, and it needs a preview enabled

It is called **Lakebase CDF** now, not Lakehouse Sync, and it is **not UI-only**: the
feed can be created, checked, disabled and deleted through the Postgres REST API and
the Databricks SDKs, so lane A can script it rather than clicking through it.

What it needs:

* a **workspace admin must enable the "Lakebase Change Data Feed" preview** on the
  workspace Previews page. Check this in phase 0, it gates the whole middle of the
  architecture and only an admin can do it
* Postgres 16, 17 or 18
* source tables in **any single database** in the project, not necessarily
  `databricks_postgres`. One database per feed
* `REPLICA IDENTITY FULL` on every source table
* `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` on the destination, and `CAN MANAGE` on
  the Lakebase project
* the destination catalog must **not** use default storage, and its managed storage
  must not be private-endpoint-only

How it behaves, all of which the analytics layer has to account for:

* configured at **schema level**: every current and future table in the source schema
  joins the feed
* lands as `lb_<table>_history`, batched and flushed **roughly every 15 seconds**
* an UPDATE produces two rows, `update_preimage` and `update_postimage`, so filter to
  `insert` + `update_postimage` and order by `_pg_lsn`
* **empty tables are skipped** until they hold at least one row, so
  `lb_alert_outbox_history` will not exist until the first alert fires. Do not treat
  its absence as a broken feed
* partitioned tables are unsupported
* never enable Delta CDF, a row filter or a column mask on a destination table. Any
  of the three stops the feed writing
* `TIMESTAMPTZ` maps to `TIMESTAMP`, `TIMESTAMP` maps to `TIMESTAMP_NTZ`, `JSONB`
  becomes `STRING`

## The seam view

`jack_freeman_catalog.tech_summit_scada_build.vw_tag_reading` is the only thing lanes B and C read. It starts
pointed at the static seed table so both lanes can work from minute one, and lane A
repoints it at the live path at seam 1. Nothing downstream changes when it moves.

## Fallback if lane A stalls

Two levels of fallback, take them early rather than late:

1. **No classic compute or DBR 18, or the native sink preview is unavailable.** Build
   pipeline 1 as a Lakeflow pipeline with `@dp.foreach_batch_sink` instead, per the
   table above. Latency drops to low seconds, which no control room screen notices.
   Remember the hourly token expiry on this path.
2. **Lakebase or CDF do not come together at all.** Pipeline 1 writes a bronze
   streaming table straight to `jack_freeman_catalog.tech_summit_scada_build.tag_reading` and `vw_tag_reading` points
   there. Lakebase then becomes a UC-to-Lakebase synced table purely for the app's hot
   reads, or drops out of the demo.

Lanes B and C are unaffected by either, which is the entire point of the seam view.

## Seed data: two horizons, not one

The mass balance and the app need high-rate data over a short window. The
predictive maintenance model needs low-rate data over a long window. A single
24 hour seed serves the first and starves the second, so lane A produces both:

| Seed table | Horizon | Grain | Serves |
|---|---|---|---|
| `jack_freeman_catalog.tech_summit_scada_build.tag_reading_seed` | 24 hours | full rate (1 Hz, 10 Hz vibration) | lanes B and C: mass balance, flowsheet, app |
| `jack_freeman_catalog.tech_summit_scada_build.vibration_features_seed` | 120 days | 10 minute | lane B: model training |

Both are static. Lane A writes them in phase 0 and then leaves them alone.

## Fault injection spec

The generator must inject these degradations, because they are the training
labels. Without them the PdM model has no positive class and lane B is stuck.

| Asset | Failure mode | Vibration RMS path | Duration | Events in 120d |
|---|---|---|---|---|
| `SCR-SCN01` | exciter bearing wear | 7.2 to 16.0 mm/s | 21 days | 2 |
| `CRU-SCC01` | mantle bearing wear | 3.8 to 12.0 mm/s | 30 days | 1 |
| `CV-CV009` | head pulley bearing | 2.8 to 7.0 mm/s | 14 days | 1 |
| `DSD-DWS01` | exciter bearing wear | 6.4 to 14.0 mm/s | 18 days | 1 |

Rules for every injected event:

* RMS follows a slow exponential ramp, not a step. Real bearings degrade.
* Bearing temperature rises with it, roughly 8 to 15 degC above nominal at end of life.
* Motor current rises slightly, 3 to 8 percent.
* Crest factor and kurtosis rise ahead of RMS. That early separation is what makes
  the model better than a plain RMS threshold, and it is the story worth telling.
* `label_failure_30d` is true for every 10 minute window falling inside the last
  30 days before the failure point.
* Every other asset stays healthy for the full 120 days, providing the negative class.

Five positive events is a thin and heavily imbalanced training set. That is fine for
a demo, but lane B should use class weighting and report precision and recall rather
than accuracy, and should say out loud in the demo that this is synthetic.

## Seed data must also contain faults, bad quality, and current timestamps

The first build of the seed data was physically excellent (all weightometer arcs within
0.2% of design, crest factor and kurtosis separating the failure classes cleanly) but
missed three things that the demo needs. They are requirements, not nice-to-haves.

**1. Limit excursions.** The first seed had **zero** breaches of `lo`/`hi`/`lo_lo`/`hi_hi`
across 8.7 million analog rows, which leaves the rule engine, the alert POST and the
app's alarm strip with nothing whatsoever to fire on. That is half the whiteboard
undemoable. Inject, over the 24 hour window:

* 15 to 25 **warning** excursions (between `lo`..`lo_lo` or `hi`..`hi_hi`), spread across
  at least 8 different tags and several areas
* 4 to 6 **trip** excursions (beyond `lo_lo`/`hi_hi`), including at least one crusher
  pressure choke and one bin high level, since those have dedicated rules
* each excursion lasting 30 seconds to 4 minutes, ramping in and out rather than
  stepping, so `rate_of_change` rules have something real to detect
* at least one sustained excursion in the final 30 minutes, so the app has a live alarm
  on screen when the demo starts

**2. Instrument quality.** The first seed was 100% `GOOD`, so rule R10 never fires,
`good_pct` is always 100, and lane B's `INSUFFICIENT_DATA` branch is untestable. Aim for
roughly 99.5% `GOOD`, with the remainder split across `BAD`, `UNCERTAIN` and `STALE`,
including one tag that goes `STALE` for several minutes so the staleness rule fires and
one node that drops to `INSUFFICIENT_DATA` in the mass balance.

**3. Timestamps must be current.** The first seed was dated June 2024, roughly two years
stale, which means anything using `current_timestamp()` (lane B's windowing, lane C's
live tiles) sees an empty recent window. The generator must anchor the 24 hour window so
it **ends at generation time**, and the 120 day vibration history must end where the 24
hour window begins, so the two are contiguous.

## Vibration feature columns: use the contract names

The first build shortened three names and omitted ten columns. The contract names in
section B of the DDL are authoritative: `rms_mm_s`, `bearing_temp_c`, `motor_current_a`,
not `rms`, `temperature`, `motor_current`. The four trend features
(`rms_delta_1h`, `rms_delta_24h`, `rms_slope_24h`, `rms_pct_of_baseline`) matter most:
a bearing failure is a trajectory, not an absolute value, and without them the model
will not beat a plain RMS threshold. `hours_since_maintenance` must be generated too,
since it cannot be derived after the fact.

## What breaks in B and C when lane A changes the data

One schema means lanes B and C read tables lane A owns. Whether that hurts depends
entirely on whether the reader is batch or streaming.

* **Lane C is nearly immune.** The app reads in batch on a refresh loop, so a data
  change just means different numbers on screen.
* **Lane B's `_1m` tables are streaming reads**, and a streaming read cannot survive a
  destructive change to its source.

| Lane A action | Lane B |
|---|---|
| Appending rows | safe |
| `CREATE OR REPLACE TABLE tag_reading_seed` | breaks the stream, needs a full refresh |
| `CREATE OR REPLACE VIEW vw_tag_reading` at seam 1 | breaks the stream, but this is planned |
| `CREATE OR REPLACE` on a `dim_*` table | may fail the batch side of a broadcast join mid-run; they are frozen for this reason |

Working rules:

1. **Lane A finalises both seed tables before lane B starts streaming.** This is why the
   seeds are lane A's step 1. Regenerating at 10am costs nothing; at 2pm it costs lane B
   a full refresh, and a full refresh destroys streaming state.
2. If lane A must regenerate later, lane A **says so out loud** and lane B full-refreshes.
   Silent regeneration looks exactly like a pipeline bug and will cost an hour of hunting.
3. Seam 1 is a planned full refresh for lane B. Budget it rather than being surprised.
4. `jack_freeman_catalog.tech_summit_scada_build.vw_tag_reading` must stay a plain filter with no
   window function, or lane B cannot stream from it at all. See the note in
   `contract/ddl_uc.sql` section A.3.
