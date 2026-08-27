# Databricks notebook source
# MAGIC %md
# MAGIC # Lane A — Pipeline 1 / Q1: real-time replay of the seed → Lakebase `plant.tag_current`
# MAGIC
# MAGIC Replays `tag_reading_seed` in **wall-clock time** and upserts the current value of every
# MAGIC tag into Lakebase, so the control-room app reads live tiles and Lakebase CDF carries the
# MAGIC change history into Unity Catalog.
# MAGIC
# MAGIC Real-time mode cannot use a Delta table as its source (only Kafka-family sources), so
# MAGIC this is the contract's documented fallback: a standalone Structured Streaming job doing
# MAGIC **low-latency micro-batch** upserts via `foreachBatch`. A `rate` stream is the clock;
# MAGIC each micro-batch maps wall time onto the seed's `source_ts` axis, reads the slice that
# MAGIC just "happened", keeps the latest reading per tag, and upserts. The 24 h seed loops
# MAGIC forever, so the stream never ends.
# MAGIC
# MAGIC Auth is driver-side (`foreachBatch` runs on the driver), so the Databricks SDK mints the
# MAGIC Lakebase OAuth token directly — no secret scope needed (that is only required for the
# MAGIC executor-side alert sink, Q2). Tokens expire hourly and are refreshed here.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "jack_freeman_catalog")
dbutils.widgets.text("schema", "tech_summit_scada_build")
dbutils.widgets.text("lakebase_project", "ironbark-ops")
dbutils.widgets.text("lakebase_branch", "production")
dbutils.widgets.text("lakebase_endpoint", "primary")
dbutils.widgets.text("lakebase_database", "databricks_postgres")
dbutils.widgets.text("pg_schema", "plant")
dbutils.widgets.text("replay_speed", "1.0")
dbutils.widgets.text("trigger_interval", "2 seconds")
dbutils.widgets.text("checkpoint_root", "/Volumes/jack_freeman_catalog/tech_summit_scada_build/landing/checkpoints")

CATALOG   = dbutils.widgets.get("catalog")
SCHEMA    = dbutils.widgets.get("schema")
PROJECT   = dbutils.widgets.get("lakebase_project")
BRANCH    = dbutils.widgets.get("lakebase_branch")
ENDPOINT  = dbutils.widgets.get("lakebase_endpoint")
DATABASE  = dbutils.widgets.get("lakebase_database")
PG_SCHEMA = dbutils.widgets.get("pg_schema")
SPEED     = float(dbutils.widgets.get("replay_speed"))
TRIGGER   = dbutils.widgets.get("trigger_interval")
CKPT      = dbutils.widgets.get("checkpoint_root").rstrip("/") + "/tag_current"

SEED_TABLE   = f"{CATALOG}.{SCHEMA}.tag_reading_seed"
FAULT_TABLE  = f"{CATALOG}.{SCHEMA}.scratch_a_fault_control"   # optional live-fault control
EP_NAME  = f"projects/{PROJECT}/branches/{BRANCH}/endpoints/{ENDPOINT}"
PRODUCER = "replay-lane-a"

# COMMAND ----------

import time, uuid, datetime as dt
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import functions as F, Window
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
endpoint = w.postgres.get_endpoint(name=EP_NAME)
HOST = endpoint.status.hosts.host
USER = w.current_user.me().user_name
print(f"Lakebase {HOST} as {USER} -> {PG_SCHEMA}.tag_current  (speed={SPEED}x, trigger='{TRIGGER}')")

# Seed replay window (epoch seconds). The stream loops over this span.
_row = spark.sql(
    f"SELECT unix_timestamp(min(source_ts)) mn, unix_timestamp(max(source_ts)) mx FROM {SEED_TABLE}"
).collect()[0]
SEED_MIN, SEED_MAX = float(_row["mn"]), float(_row["mx"])
SPAN = SEED_MAX - SEED_MIN
assert SPAN > 0, "seed table is empty — run the generator first"
print(f"seed span: {SPAN/3600:.1f} h")

# COMMAND ----------

# ---- Lakebase connection with hourly-token refresh (driver-side). -------------------
_state = {"conn": None, "token_ts": 0.0, "t0": None, "last_pos": None}

def _connect():
    token = w.postgres.generate_database_credential(endpoint=EP_NAME).token
    c = psycopg2.connect(host=HOST, dbname=DATABASE, user=USER, password=token, sslmode="require")
    c.autocommit = True
    _state["conn"], _state["token_ts"] = c, time.time()
    return c

def get_conn():
    c = _state["conn"]
    if c is None or c.closed or (time.time() - _state["token_ts"] > 40 * 60):   # refresh well before 1h expiry
        if c is not None and not c.closed:
            try: c.close()
            except Exception: pass
        c = _connect()
    return c

UPSERT = f"""
INSERT INTO {PG_SCHEMA}.tag_current AS t
  (tag_id, event_id, source_ts, value, value_text, quality, unit, seq, producer, ingest_ts)
VALUES %s
ON CONFLICT (tag_id) DO UPDATE SET
  event_id=EXCLUDED.event_id, source_ts=EXCLUDED.source_ts, value=EXCLUDED.value,
  value_text=EXCLUDED.value_text, quality=EXCLUDED.quality, unit=EXCLUDED.unit,
  seq=EXCLUDED.seq, producer=EXCLUDED.producer, ingest_ts=EXCLUDED.ingest_ts, updated_at=now()
WHERE EXCLUDED.seq > t.seq
"""

def load_fault_overrides():
    """Optional live demo hook: rows in scratch_a_fault_control {tag_id, multiplier}
    scale that tag's value in real time. Absent table = no overrides."""
    try:
        if spark.catalog.tableExists(FAULT_TABLE):
            return {r["tag_id"]: float(r["multiplier"]) for r in spark.table(FAULT_TABLE).collect()}
    except Exception:
        pass
    return {}

# COMMAND ----------

def upsert_batch(batch_df, batch_id):
    now = time.time()
    if _state["t0"] is None:
        _state["t0"], _state["last_pos"] = now, SEED_MIN

    # Map wall clock onto the seed-time axis; window = what elapsed since the last batch.
    pos  = SEED_MIN + ((now - _state["t0"]) * SPEED) % SPAN
    last = _state["last_pos"]
    windows = [(last, pos)] if pos >= last else [(last, SEED_MAX), (SEED_MIN, pos)]
    _state["last_pos"] = pos

    cond = None
    for a, b in windows:
        lo = F.lit(dt.datetime.fromtimestamp(a, tz=dt.timezone.utc))
        hi = F.lit(dt.datetime.fromtimestamp(b, tz=dt.timezone.utc))
        c = (F.col("source_ts") > lo) & (F.col("source_ts") <= hi)
        cond = c if cond is None else (cond | c)

    # Latest reading per tag in this replay window.
    rank = Window.partitionBy("tag_id").orderBy(F.col("source_ts").desc(), F.col("seq").desc())
    latest = (spark.table(SEED_TABLE).where(cond)
              .withColumn("_rn", F.row_number().over(rank)).where("_rn = 1")
              .select("tag_id", "value", "value_text", "quality", "unit"))
    rows = latest.collect()
    if not rows:
        return

    overrides = load_fault_overrides()
    live_ts = dt.datetime.now(dt.timezone.utc)
    seq = int(now * 1000)                      # monotonic across batches and loop wraps
    tuples = []
    for r in rows:
        v = r["value"]
        if v is not None and r["tag_id"] in overrides:
            v = v * overrides[r["tag_id"]]
        tuples.append((r["tag_id"], str(uuid.uuid4()), live_ts, v, r["value_text"],
                       r["quality"], r["unit"], seq, PRODUCER, live_ts))

    for attempt in range(2):                   # reconnect once on a dropped connection
        try:
            cur = get_conn().cursor()
            execute_values(cur, UPSERT, tuples)
            cur.close()
            return
        except psycopg2.OperationalError:
            _state["conn"] = None
            if attempt == 1:
                raise

# COMMAND ----------

# rate stream is only a clock; the replay window is derived from wall time in the batch.
clock = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

query = (clock.writeStream
         .foreachBatch(upsert_batch)
         .option("checkpointLocation", CKPT)
         .trigger(processingTime=TRIGGER)
         .queryName("a_replay_tag_current")
         .start())

print(f"streaming started; upserting {PG_SCHEMA}.tag_current every {TRIGGER}. Checkpoint: {CKPT}")
query.awaitTermination()
