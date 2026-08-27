# Data contract: Ironbark iron ore crushing plant

Agreed in the phase 0 session. **Change this file by agreement, not unilaterally.**
Every Claude session working on this build should read it first.

Scenario: Ironbark Resources, Pilbara iron ore. Dry crushing and screening circuit
with a wet desands circuit, feeding a product stockpile via a stacker. Design basis
2,500 t/h ROM feed, 2,100 t/h product, 400 t/h fines rejected. The tertiary crusher
runs a 250 t/h recirculating load back to the secondary screen, which is what makes
the mass balance worth visualising.

## Unity Catalog naming

| Object | Name | Owner |
|---|---|---|
| Catalog | `ironbark` | shared |
| Reference / dimensions | `ironbark.ref` | phase 0, then frozen |
| Bronze / landing | `ironbark.raw` | lane A |
| Silver / analytical | `ironbark.analytics` | lane B |
| ML features and scores | `ironbark.ml` | lane B |
| Personal scratch | `ironbark.dev_<initials>` | one each |
| Landing volume | `ironbark.raw.landing` | lane A |
| Lakebase project | `ironbark-ops` | lane A |
| Lakebase database / schema | `databricks_postgres` / `plant` | lane A |

Conventions:

* `snake_case` for everything in UC. Tables are singular nouns except aggregates.
* Dimension tables prefixed `dim_`. Views prefixed `vw_`.
* Aggregate tables carry their grain as a suffix: `_1m`, `_10m`.
* Tables created by Lakebase CDF arrive as `lb_<source_table>_history`. Do not rename.
* Pipeline, job, endpoint and app names are prefixed with the owner's initials until
  after seam 2, e.g. `cd_ironbark_ingest`. Drop the prefix at integration.
* Registered model: `ironbark.ml.vibration_pdm`. Serving endpoint: `ironbark-pdm`.
* App: `ironbark-control-room`.

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

`ironbark.analytics.vw_tag_reading` is the only thing lanes B and C read. It starts
pointed at the static seed table so both lanes can work from minute one, and lane A
repoints it at the live path at seam 1. Nothing downstream changes when it moves.

## Fallback if lane A stalls

Two levels of fallback, take them early rather than late:

1. **No classic compute or DBR 18, or the native sink preview is unavailable.** Build
   pipeline 1 as a Lakeflow pipeline with `@dp.foreach_batch_sink` instead, per the
   table above. Latency drops to low seconds, which no control room screen notices.
   Remember the hourly token expiry on this path.
2. **Lakebase or CDF do not come together at all.** Pipeline 1 writes a bronze
   streaming table straight to `ironbark.raw.tag_reading` and `vw_tag_reading` points
   there. Lakebase then becomes a UC-to-Lakebase synced table purely for the app's hot
   reads, or drops out of the demo.

Lanes B and C are unaffected by either, which is the entire point of the seam view.

## Seed data: two horizons, not one

The mass balance and the app need high-rate data over a short window. The
predictive maintenance model needs low-rate data over a long window. A single
24 hour seed serves the first and starves the second, so lane A produces both:

| Seed table | Horizon | Grain | Serves |
|---|---|---|---|
| `ironbark.raw.tag_reading_seed` | 24 hours | full rate (1 Hz, 10 Hz vibration) | lanes B and C: mass balance, flowsheet, app |
| `ironbark.ml.vibration_features_seed` | 120 days | 10 minute | lane B: model training |

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
