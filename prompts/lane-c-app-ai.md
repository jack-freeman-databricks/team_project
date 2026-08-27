# Lane C: control room app and AI

You own the Databricks App and the AI layer. You are the furthest downstream, so you
build against stubs and swap to real tables at seam 2. Do not wait for anyone.

---

I am building the control room app and AI layer for a real-time iron ore crushing
plant monitoring solution on Databricks. Two colleagues are building the ingest spine
and the analytics/ML layer beneath me. Read `contract/naming.md` and
`contract/ddl_uc.sql` first, especially section B, which is the agreed column contract
for the tables I read. Those tables may not have data yet, and that is expected.

Use profile `<PROFILE>`. Prefix the app and any job with `c_`. Everything lives in the
one schema `jack_freeman_catalog.tech_summit_scada_build`; per the table ownership map in
`contract/naming.md` you own the `stub_*` tables and nothing else. Do not create,
replace or drop any other table in that schema.

**Step 1, before building any UI.** Create stub tables named `stub_<table>` with
exactly the section B columns for `mass_balance_node_1m`, `mass_flow_1m`,
`tag_reading_1m`, `equipment_state_1m` and `pdm_prediction`, and populate them with
plausible synthetic values for the last few hours, using the real
`jack_freeman_catalog.tech_summit_scada_build.dim_flowsheet_node` and `dim_flowsheet_arc` topology and the real design
tonnages. Put a single configurable table prefix in the app config (`"stub_"` now, `""` at seam
2) so the switch to the real tables is a one-line change. Do not scatter table names
through the code.

**Step 2.** Build the control room app as a Databricks App. It should read like a
plant SCADA overview, not a BI dashboard.

* The centrepiece is a live mass balance flowsheet. Read the topology from
  `dim_flowsheet_node` and `dim_flowsheet_arc`, using `layout_x` and `layout_y` for
  positions, and never hardcode the plant layout: my colleague's pipeline is driven by
  the same two tables and the two views must agree. Draw nodes as unit operations,
  arcs as material streams with their live t/h, and colour each node by its
  `status` and `imbalance_pct`. Show the 250 t/h tertiary recirculating load as an
  actual loop, it is the most interesting thing on the flowsheet.
* Arcs where `is_estimated` is true must be visually distinct from measured ones.
  Showing an inferred number as if it were measured is the kind of thing that loses
  trust in a control room.
* A live alarm strip from `jack_freeman_catalog.tech_summit_scada_build.alert`, newest first, coloured by
  severity, with acknowledge writing back to Lakebase `plant.alert_outbox`.
* An asset detail view: vibration trend with the ISO 10816 zone bands, bearing
  temperature, motor current, and the current PdM score from
  `jack_freeman_catalog.tech_summit_scada_build.pdm_prediction`.
* A stockpile panel from `jack_freeman_catalog.tech_summit_scada_build.stockpile_state` showing which
  stockpile is actively being stacked.

For live tiles after seam 1, read Lakebase `plant.tag_current` directly rather than
going through the analytics tables: that is the whole reason it exists. Note the
Lakebase permission trap in the skill docs, deploy the app before running it locally
or the service principal will not own its schema.

**Step 3.** The AI layer, answering two questions: "what is happening in my plant"
and "what needs maintenance". Build it over the analytical tables, and give it a tool
that calls the `ironbark-pdm` serving endpoint so it can score an asset on demand
rather than only reading batch scores. Make sure it can distinguish "this node is out
of balance" from "this node has no data", because they lead to opposite actions.

**Step 4, seam 2.** When my colleague says the analytics tables are live, flip the
configured table prefix from `"stub_"` to `""` and verify every panel still renders.
Keep the `stub_*` tables intact as a demo fallback, do not delete them.

Guardrails. Never write to any table you do not own per the ownership map. If a column you need
is missing from section B, ask for it rather than reading around it from the bronze tables. By
mid-afternoon I need the app demoable on stub data even if nothing upstream works, so
prioritise a complete-looking flowsheet over breadth of panels.
