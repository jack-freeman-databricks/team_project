# Ironbark plant monitoring: real-time mass balance and predictive maintenance on Databricks

One day build, three people in parallel. Fictional scenario: Ironbark Resources, a
Pilbara iron ore crushing plant. We build a real-time mass balance monitoring solution
with control-room-style visualisation, predictive maintenance on vibration sensors,
and an AI layer over the top. All on Databricks.

`plan.html` is a standalone one-page version of this plan, with the architecture
diagram, the lane split, the day's two integration seams, and the platform
constraints. Open it in a browser, no build step and no network needed.

## Start here

1. Read `contract/naming.md`. It is the agreed data contract and it explains two
   places where the original whiteboard architecture cannot be built as drawn.
2. Run phase 0 together, using `prompts/00-phase-0-together.md`. Roughly 45 minutes,
   one laptop, all three of us. Nothing else starts until this is done.
3. Each take a lane and paste your prompt into your own Claude Code session.

## Repo layout

```
contract/
  naming.md                  the data contract: UC naming, tag convention,
                             physical constants, architecture corrections,
                             seed horizons, fault injection spec
  gen_reference_data.py      generates the reference CSVs and self-checks that
                             the design mass balance closes
  dim_tag.csv                121 tags across 25 assets
  dim_flowsheet_node.csv     13 nodes (unit operations)
  dim_flowsheet_arc.csv      15 arcs (material streams)
  dim_rule.csv               10 alarm rules
  ddl_uc.sql                 Unity Catalog DDL, split by who creates what
  ddl_lakebase.sql           Lakebase Postgres DDL
prompts/
  00-phase-0-together.md     the joint opening session
  lane-a-streaming-spine.md  generator, ingest pipeline, Lakebase, alerting
  lane-b-transform-ml.md     analytics tables, mass balance, PdM model
  lane-c-app-ai.md           control room app, AI layer
brief/
  transcript.md              the original scoping conversation
  whiteboard.jpg             the whiteboard photo the design came from
plan.html                    standalone one-page plan, open in a browser
```

## Architecture

```
synthetic generator (121 tags, ~450 events/sec)
        |
        v
Lakeflow Declarative Pipeline (continuous)
        |
        +--> foreach_batch_sink --> Lakebase plant.tag_current  (upsert, hot reads)
        |                      \
        |                       -> alert POST + plant.alert_outbox
        v
Lakehouse Sync (CDC, UI-only)
        |
        v
ironbark.raw.lb_tag_current_history
        |
        v
ironbark.analytics.vw_tag_reading   <-- THE SEAM. Lanes B and C read only this.
        |
        v
Lakeflow Declarative Pipeline #2
        |
        +--> tag_reading_1m, mass_flow_1m, mass_balance_node_1m,
        |    equipment_state_1m, desands_balance_1m, stockpile_state
        |
        +--> ml.vibration_features_10m --> vibration_pdm model --> ironbark-pdm endpoint
                                                     |
                                                     v
                                            ml.pdm_prediction
        |
        v
Databricks App (control room flowsheet)  +  AI layer
```

## The three lanes and two seams

| Lane | Owns | Writes to |
|---|---|---|
| A | generator, ingest pipeline, Lakebase, alerting | `ironbark.raw`, Lakebase `plant` |
| B | analytics tables, mass balance, PdM model | `ironbark.analytics`, `ironbark.ml` |
| C | control room app, AI layer | nothing shared, reads only |

The chain is linear, so the parallelism comes from two devices:

* **Seed data.** Lane A's first task is two static seed tables, so lanes B and C have
  real data from minute one instead of waiting for the streaming spine.
* **The seam view.** `ironbark.analytics.vw_tag_reading` starts pointed at the seed
  table and gets repointed at the live path. Nothing downstream changes when it moves.

Two integration checkpoints, booked rather than left to the end:

* **Seam 1**, late morning: lane A repoints `vw_tag_reading` at the live stream.
* **Seam 2**, mid afternoon: lane C swaps from stub tables to `ironbark.analytics`.

Every lane must be independently demoable by mid afternoon, so a failed integration
costs us one panel rather than the whole demo.

## Conventions while we work

* Own your schema. `ironbark.ref` is frozen after phase 0. Nobody writes outside
  their lane's schemas, and everyone has an `ironbark.dev_*` scratch schema.
* Prefix pipelines, jobs, endpoints and apps with your lane letter until after seam 2.
* One branch per lane, merge every 90 minutes. Long-lived branches cost more than
  small conflicts on a one day build.
* Tell your Claude session which schema it owns. Left unsupervised they will each
  cheerfully `CREATE OR REPLACE` the shared tables.
