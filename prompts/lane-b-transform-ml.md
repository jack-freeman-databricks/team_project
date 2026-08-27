# Lane B: transform and ML

You own the analytical layer and the predictive maintenance model. You read from one
view and write to `jack_freeman_catalog.tech_summit_scada_build` and `jack_freeman_catalog.tech_summit_scada_build`. You are not blocked on lane
A after phase 0, because the seed tables give you real data from minute one.

---

I am building the analytics and ML half of a real-time iron ore crushing plant
monitoring solution on Databricks. A colleague is building the ingest spine and
another is building the control room app and AI layer on top of my tables. Read
`contract/naming.md` and `contract/ddl_uc.sql` (all sections, B is my column
contract) first.

Use profile `<PROFILE>`. Prefix every pipeline, job and endpoint you create with
`b_`, and prefix any scratch tables `scratch_b_`. **Everything lives in the one schema
`jack_freeman_catalog.tech_summit_scada_build`, so read the table ownership map in
`contract/naming.md` and stay inside your rows of it.** Read tag data ONLY from
`jack_freeman_catalog.tech_summit_scada_build.vw_tag_reading`, never from the bronze
tables (`tag_reading`, `lb_*_history`) directly: that view
is the seam that lets me work before the live stream exists and swaps underneath me
without breaking anything. Same for training data, read
`jack_freeman_catalog.tech_summit_scada_build.vw_vibration_features`, not the seed table. Do not create or modify the four frozen `dim_*` tables, lane A's bronze or seed
tables, the two `vw_*` seam views, lane C's `stub_*` tables, or the Lakebase project.

**Step 1.** Build the analytics pipeline as a Lakeflow Declarative Pipeline reading
`vw_tag_reading`, producing the tables in section B of the DDL with exactly those
columns. The `_1m` tables are one minute tumbling window aggregates over a streaming
source: implement them as streaming tables with a watermark on `source_ts`, since a
window never changes once closed.

**Step 2, this is the centrepiece, get it right.** The mass balance must be driven
entirely by `jack_freeman_catalog.tech_summit_scada_build.dim_flowsheet_node` and `dim_flowsheet_arc`. Nothing about
the plant topology may be hardcoded, because my colleague's app reads the same two
tables to draw the diagram and the two must agree.

* For each arc, derive tonnage per minute according to its `measure_method`:
  `weightometer` is a direct average of the `WI` tag, `slurry_flow_density` needs
  `Q * rho * Cw` with `Cw = SOLIDS_SG * (rho - 1) / (rho * (SOLIDS_SG - 1))` and
  `SOLIDS_SG = 4.9`. Use exactly that constant, it is in the contract because if we
  each pick our own the desands node will never close and it will look like a bug.
  Arcs with a null `measure_tag_id` are inferred: mark them `is_estimated = true`.
* For each node, sum inbound and outbound arcs, and compute the accumulation term
  where `accumulation_tag_id` is set, as the rate of change of
  `level_pct/100 * capacity_t`. Bins genuinely accumulate and ignoring it will make
  them look permanently out of balance.
* `imbalance_tph = mass_in - mass_out - accumulation`. Only compute it for nodes
  where `balance_role = 'unit'`: sources and sinks never close by definition.
* Set `status` to `INSUFFICIENT_DATA` when measured arcs are missing or
  `data_quality_pct` is low, rather than reporting a misleading imbalance. A
  control room screen that cries wolf is worse than one that says "no data".

Sanity check against the design basis: 2,500 t/h feed, 900 t/h scalping oversize,
250 t/h tertiary recirculating load, 400 t/h fines reject, 2,100 t/h product. If your
nodes do not close on the seed data, the bug is yours, not the generator's:
`gen_reference_data.py` asserts the design closes before it will emit the CSVs.

**Step 3.** Build `vibration_features_10m` from the live vibration tags, matching the
seed table's columns exactly so the two can be unioned. Compute crest factor and
kurtosis, not just RMS, and classify `iso_10816_zone`.

**Step 4.** Train the predictive maintenance model on
`jack_freeman_catalog.tech_summit_scada_build.vw_vibration_features`. There are only about five injected failure events
across 120 days, so the classes are heavily imbalanced: use class weighting, report
precision and recall rather than accuracy, and tell me honestly whether the model
beats a plain RMS threshold. The interesting story is that crest factor and kurtosis
separate before RMS does, so check whether that actually shows up in the feature
importances. Track it in MLflow, register it as `jack_freeman_catalog.tech_summit_scada_build.vibration_pdm` in Unity
Catalog, and put it behind a serving endpoint named `ironbark-pdm`.

**Step 5.** Batch scoring job writing to `jack_freeman_catalog.tech_summit_scada_build.pdm_prediction` with
`scoring_mode = 'batch'`. My colleague's app calls the endpoint directly for
`scoring_mode = 'realtime'`, so leave that path alone but make sure the table accepts
both.

**Step 6, seam 2.** Tell my colleague when `mass_balance_node_1m`, `mass_flow_1m`,
`alert` and `pdm_prediction` hold real data and the endpoint is live, so they can
drop their stubs.

Guardrails. Do not full-refresh a streaming pipeline without telling me. Do not
change any column name in section B without telling both of the others first, because
the app is built against them. If the seed data looks physically wrong, tell me
rather than adding a correction factor to hide it.
