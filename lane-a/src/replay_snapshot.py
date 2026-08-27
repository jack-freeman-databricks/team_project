# Databricks notebook source
# MAGIC %md
# MAGIC # Lane A — Pipeline 1 / Q1: looped snapshot replay → Lakebase `plant.tag_current`
# MAGIC
# MAGIC Serverless kills a blocking `awaitTermination()` (kernel watchdog), and this workspace
# MAGIC is serverless-only, so the replay is a **completing** job that loops: every few seconds
# MAGIC it maps wall-clock time onto the 24 h seed loop, takes the latest reading per tag at that
# MAGIC replay position, and upserts all tags into Lakebase `plant.tag_current`. Each run is a
# MAGIC bounded loop that exits cleanly (no infinite block); the **continuous** job restarts it,
# MAGIC so `tag_current` updates live. Lakebase CDF (already on) carries every upsert into
# MAGIC `jack_freeman_catalog.tech_summit_scada_build.lb_tag_current_history`.
# MAGIC
# MAGIC Stateless: the replay position is a pure function of wall-clock time, so restarts need
# MAGIC no shared state. Auth is the driver's OAuth identity (SDK-minted token, refreshed).

# COMMAND ----------

# MAGIC %pip install psycopg2-binary "databricks-sdk>=0.81.0"
# MAGIC %restart_python

# COMMAND ----------

import time, uuid, datetime as dt
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import functions as F, Window
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "jack_freeman_catalog")
dbutils.widgets.text("schema", "tech_summit_scada_build")
dbutils.widgets.text("lakebase_project", "ironbark-ops")
dbutils.widgets.text("lakebase_branch", "production")
dbutils.widgets.text("lakebase_endpoint", "primary")
dbutils.widgets.text("lakebase_database", "databricks_postgres")
dbutils.widgets.text("pg_schema", "plant")
dbutils.widgets.text("replay_speed", "1.0")
dbutils.widgets.text("interval_seconds", "5")
dbutils.widgets.text("loop_seconds", "86400")   # run length; 86400 = a full 24h replay loop per run

g = dbutils.widgets.get
CATALOG, SCHEMA, PG_SCHEMA = g("catalog"), g("schema"), g("pg_schema")
PROJECT, BRANCH, ENDPOINT, DATABASE = g("lakebase_project"), g("lakebase_branch"), g("lakebase_endpoint"), g("lakebase_database")
SPEED, INTERVAL, LOOP_SECONDS = float(g("replay_speed")), float(g("interval_seconds")), float(g("loop_seconds"))
SEED_TABLE  = f"{CATALOG}.{SCHEMA}.tag_reading_seed"
FAULT_TABLE = f"{CATALOG}.{SCHEMA}.scratch_a_fault_control"    # optional live-fault control
EP_NAME  = f"projects/{PROJECT}/branches/{BRANCH}/endpoints/{ENDPOINT}"
PRODUCER = "replay-lane-a"
LOOKBACK = 1800   # seconds; window wide enough that even the slowest tag (assay ~30 min) is present

# COMMAND ----------

w = WorkspaceClient()
endpoint = w.postgres.get_endpoint(name=EP_NAME)
HOST, USER = endpoint.status.hosts.host, w.current_user.me().user_name
print(f"Lakebase {HOST} as {USER} -> {PG_SCHEMA}.tag_current  (speed={SPEED}x, every {INTERVAL}s, run {LOOP_SECONDS}s)")

_r = spark.sql(f"SELECT unix_timestamp(min(source_ts)) mn, unix_timestamp(max(source_ts)) mx FROM {SEED_TABLE}").collect()[0]
SEED_MIN, SEED_MAX = float(_r["mn"]), float(_r["mx"])
SPAN = SEED_MAX - SEED_MIN
assert SPAN > 0, "seed table is empty — run the generator first"

_state = {"conn": None, "token_ts": 0.0}

def get_conn():
    c = _state["conn"]
    if c is None or c.closed or (time.time() - _state["token_ts"] > 40 * 60):
        if c is not None and not c.closed:
            try: c.close()
            except Exception: pass
        token = w.postgres.generate_database_credential(endpoint=EP_NAME).token
        c = psycopg2.connect(host=HOST, dbname=DATABASE, user=USER, password=token, sslmode="require")
        c.autocommit = True
        _state["conn"], _state["token_ts"] = c, time.time()
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
rank = Window.partitionBy("tag_id").orderBy(F.col("source_ts").desc(), F.col("seq").desc())

def snapshot_upsert():
    now = time.time()
    replay_pos = SEED_MIN + ((now - SEED_MIN) * SPEED) % SPAN
    hi = dt.datetime.fromtimestamp(replay_pos, tz=dt.timezone.utc)
    lo = dt.datetime.fromtimestamp(replay_pos - LOOKBACK, tz=dt.timezone.utc)
    snap = (spark.table(SEED_TABLE).where((F.col("source_ts") > F.lit(lo)) & (F.col("source_ts") <= F.lit(hi)))
            .withColumn("_rn", F.row_number().over(rank)).where("_rn = 1")
            .select("tag_id", "value", "value_text", "quality", "unit")).collect()
    if not snap:
        return 0
    overrides = {}
    try:
        if spark.catalog.tableExists(FAULT_TABLE):
            overrides = {r["tag_id"]: float(r["multiplier"]) for r in spark.table(FAULT_TABLE).collect()}
    except Exception:
        pass
    live_ts = dt.datetime.now(dt.timezone.utc)
    seq = int(now * 1000)
    tuples = []
    for r in snap:
        v = r["value"]
        if v is not None and r["tag_id"] in overrides:
            v = v * overrides[r["tag_id"]]
        tuples.append((r["tag_id"], str(uuid.uuid4()), live_ts, v, r["value_text"],
                       r["quality"], r["unit"], seq, PRODUCER, live_ts))
    for attempt in range(2):
        try:
            cur = get_conn().cursor(); execute_values(cur, UPSERT, tuples); cur.close()
            return len(tuples)
        except psycopg2.OperationalError:
            _state["conn"] = None
            if attempt == 1:
                raise

# COMMAND ----------

start = time.time()
iters = last = 0
while time.time() - start < LOOP_SECONDS:
    last = snapshot_upsert()
    iters += 1
    time.sleep(INTERVAL)
msg = f"done: {iters} snapshots over {int(time.time()-start)}s, last upsert {last} tags -> {PG_SCHEMA}.tag_current"
print(msg)
dbutils.notebook.exit(msg)
