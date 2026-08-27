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

**Step 3.** Build pipeline 1 as a **standalone Structured Streaming job**, not a
Lakeflow pipeline. Read the architecture section of `contract/naming.md` for why: the
native Lakebase sink supports real-time mode and gives us genuine sub-second writes
with managed credentials, batching, retries and backpressure, but it explicitly does
not run on serverless or inside a Lakeflow pipeline. So this hop needs classic compute
(dedicated or standard access mode) on DBR 18 or later.

Two queries, matching the two arrows out of pipeline 1 on the whiteboard:

* **Q1**, all readings into Lakebase:
  ```python
  (df.writeStream
     .format("postgresql")
     .outputMode("update")
     .option("checkpointLocation", "/Volumes/ironbark/raw/landing/checkpoints/tag_current")
     .option("batchinterval", "50 milliseconds")   # long-form units only
     .trigger(realTime="5 minutes")               # checkpoint cadence, not batch size
     .start())
  ```
  The upsert key is inferred from the `tag_current` primary key, so no `upsertkey`
  option is needed. Use the `endpoint` + `dbtable` form, or register the Lakebase
  database in Unity Catalog and use `.toTable()`, whichever is quicker. Give the query
  its own checkpoint location; it must be unique per query.

* **Q2**, rule failures only. Evaluate `ironbark.ref.dim_rule` as a broadcast
  stream-static join, not hardcoded predicates. Write with a custom `foreach` sink so
  one writer both POSTs to the alerting endpoint and inserts into
  `plant.alert_outbox`. An executor-side `foreach` sink needs native Postgres password
  auth from a secret scope, not OAuth, because executors have no SDK context to
  refresh a token with.

Size the cluster for real-time mode's slot math: RTM schedules every stage
concurrently, so free slots must cover the sum of partitions across all stages, not
just the source. Set `spark.sql.shuffle.partitions` low to match real parallelism.
Turn Photon and autoscaling off, they do nothing for RTM. Only one streaming shuffle
stage is allowed per query.

If classic compute or DBR 18 is not available, take the fallback in
`contract/naming.md`: a Lakeflow pipeline with `@dp.foreach_batch_sink` doing the
upsert and the POST. Latency drops to low seconds, which nobody watching a control
room screen notices. On that path only, refresh the Lakebase credential inside the
handler, because it expires hourly.

**Step 4.** Start Lakebase CDF from the `plant` schema to `ironbark.raw`. Confirm with
me first that a workspace admin has enabled the **Lakebase Change Data Feed** preview,
because nothing here works until they have. This IS scriptable, through the Postgres
REST API or the Databricks SDK, so prefer that over clicking so we can tear down and
rebuild. Then verify:

* `SELECT * FROM wal2delta.tables;` shows `tag_current` as `STREAMING`
* `ironbark.raw.lb_tag_current_history` is populating, allowing for the roughly 15
  second flush interval
* `lb_alert_outbox_history` is absent until the first alert fires. CDF skips empty
  tables, so that is expected rather than a fault

Never enable Delta change data feed, a row filter or a column mask on a destination
table. Any of the three permanently stops the feed.

**Step 5, seam 1.** Repoint `ironbark.analytics.vw_tag_reading` at the live path
using the commented definition in section A.3, filtering to `insert` and
`update_postimage` and de-duplicating on `_pg_lsn`. Verify row counts and freshness
look sane, then tell my colleagues the seam has moved.

Guardrails. Do not use `CREATE OR REPLACE` on anything in `ironbark.ref`, it is
frozen. If you end up on the Lakeflow fallback, do not hand-create that pipeline's own
streaming tables, Lakeflow owns those.
Never full-refresh a pipeline without telling me first, it destroys streaming state.
By mid-afternoon I need a standalone demoable state even if Lakebase is not working:
that fallback is a bronze streaming table at `ironbark.raw.tag_reading` with
`vw_tag_reading` pointed at it. Take the fallback early rather than late.
