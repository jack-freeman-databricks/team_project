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
* Tables created by Lakehouse Sync arrive as `lb_<source_table>_history`. Do not rename.
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

## Two architecture decisions the whiteboard needs corrected

**1. There is no Lakebase sink in an SDP real-time-mode flow.** SDP RTM sinks are
Kafka only (Delta is explicitly not RTM-usable), and there is no HTTP or API sink
either. The whiteboard's `SDP-RTM -> Lakebase sink` and `SDP-RTM -> API sink` cannot
be built as drawn.

What we build instead: a normal continuous SDP pipeline using
`@dp.foreach_batch_sink` (Python only, Public Preview), whose handler does both jobs
per micro-batch, upserting `tag_current` into Lakebase and POSTing rule failures to
the alerting endpoint. Latency lands in the low seconds rather than milliseconds,
which is well inside what a control room screen needs.

Keep true RTM as a stretch goal only: a separate single-flow pipeline sinking to
Kafka purely to demo sub-second latency. Note RTM requires `continuous: true` plus
serverless on the `PREVIEW` channel with DBR 18.1.3, `spark.databricks.streaming.realTimeMode.enabled`,
and a per-flow `pipelines.trigger: "RealTime"`. Compute never scales to zero.

**2. Lakebase to Unity Catalog CDC (Lakehouse Sync) is UI-only.** There is no CLI
command and no REST API. It is configured once at the schema level via Catalog ->
project -> branch -> Lakehouse Sync -> Start Sync. Prerequisites that will bite:

* Postgres 17, tables in the `databricks_postgres` database
* `ALTER TABLE tag_current REPLICA IDENTITY FULL;` on every synced table
* the destination catalog must **not** use default storage
* partitioned tables are unsupported
* disabling and re-enabling sync does not re-snapshot, missed changes are lost

## The seam view

`ironbark.analytics.vw_tag_reading` is the only thing lanes B and C read. It starts
pointed at the static seed table so both lanes can work from minute one, and lane A
repoints it at the live path at seam 1. Nothing downstream changes when it moves.

## Fallback if lane A stalls

If Lakebase or Lakehouse Sync do not come together, the pipeline writes a bronze
streaming table directly to `ironbark.raw.tag_reading` and `vw_tag_reading` points
there. Lakebase then becomes a UC-to-Lakebase synced table purely for the app's hot
reads. Lanes B and C are unaffected either way, which is the whole point of the view.

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
