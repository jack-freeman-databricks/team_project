# Lane A: the streaming spine

You own the synthetic generator, the ingest pipeline, Lakebase, and alerting. You are
the only lane touching `ironbark.raw` and the Lakebase project. This is the lane with
the most platform risk, so the order below is deliberate: the seed data comes first
because two other people are blocked until it exists.

---

I am building the ingest half of a real-time iron ore crushing plant monitoring
solution on Databricks. Two colleagues are working in parallel on the analytics/ML
layer and the app/AI layer, and they depend on me. Read `contract/naming.md`,
`contract/ddl_uc.sql` (sections A and B) and `contract/ddl_lakebase.sql` first.

Use profile `<PROFILE>`. Prefix every pipeline and job you create with `a_`. Write
scratch work to `ironbark.dev_a`. Never create, replace or drop anything in
`ironbark.analytics` or `ironbark.ml` except the two seed objects named in step 1:
those schemas belong to someone else and you will break their session.

**Step 1, do this before anything else, my colleagues are blocked until it lands.**
Write a synthetic data generator driven entirely by `ironbark.ref.dim_tag`, so adding
a tag to the register adds it to the output with no code change. Use it to populate
both seed tables:

* `ironbark.raw.tag_reading_seed`: 24 hours, full rate, every one of the 121 tags,
  matching the bronze contract in section A.2 exactly. Each tag centred on `nominal`,
  staying inside `lo`..`hi` except for deliberate excursions. Correlate the tags that
  are physically linked: belt weightometers must be consistent with the design
  tonnages in `dim_flowsheet_arc`, motor current must track throughput, bin levels
  must integrate their in and out flows. If the tags are independent noise the mass
  balance will never close and the whole demo falls over.
* `ironbark.ml.vibration_features_seed`: 120 days at 10 minute grain, with the
  injected failure events exactly as specified in the fault injection section of
  `contract/naming.md`, including the `label_failure_30d` label.

Then tell me it is done so I can unblock the others.

**Step 2.** Turn the generator into a continuous stream. Prefer writing to a UC
volume that Auto Loader picks up, or a Kafka/Event Hubs topic if one is easy to get.
Around 121 tags at 1 Hz with vibration at 10 Hz is roughly 400 to 500 events/sec.
Include a controllable fault injection mode I can trigger live during the demo.

**Step 3.** Build the ingest pipeline as a Lakeflow Declarative Pipeline in Python.
Read `contract/naming.md` on why this is NOT a real-time-mode pipeline: SDP RTM sinks
are Kafka only, there is no Lakebase sink and no HTTP sink in an RTM flow, so the
whiteboard cannot be built as drawn. Use a normal continuous pipeline with
`@dp.foreach_batch_sink`, whose handler does two things per micro-batch:

* upsert `plant.tag_current` in Lakebase using the `ON CONFLICT` statement in
  `contract/ddl_lakebase.sql`, batched via `execute_values`, with the
  `WHERE EXCLUDED.seq > t.seq` guard so a late batch cannot overwrite a newer read
* evaluate the rules in `ironbark.ref.dim_rule` as a broadcast stream-static join,
  not hardcoded predicates, and for each failure insert into `plant.alert_outbox` and
  POST to the alerting endpoint, recording the HTTP status

Refresh the Lakebase OAuth credential inside the handler. Tokens expire after an
hour, and caching one at pipeline start is the most likely way this dies mid-demo.

**Step 4.** Enable Lakehouse Sync from the Lakebase `plant` schema to `ironbark.raw`.
This is UI-only, there is no CLI or API, so walk me through the clicks rather than
trying to automate it. Confirm `ironbark.raw.lb_tag_current_history` appears and is
populating.

**Step 5, seam 1.** Repoint `ironbark.analytics.vw_tag_reading` at the live path
using the commented definition in section A.3, filtering to `insert` and
`update_postimage` and de-duplicating on `_pg_lsn`. Verify row counts and freshness
look sane, then tell my colleagues the seam has moved.

Guardrails. Do not use `CREATE OR REPLACE` on anything in `ironbark.ref`, it is
frozen. Do not hand-create the pipeline's own streaming tables, Lakeflow owns those.
Never full-refresh a pipeline without telling me first, it destroys streaming state.
By mid-afternoon I need a standalone demoable state even if Lakebase is not working:
that fallback is a bronze streaming table at `ironbark.raw.tag_reading` with
`vw_tag_reading` pointed at it. Take the fallback early rather than late.
