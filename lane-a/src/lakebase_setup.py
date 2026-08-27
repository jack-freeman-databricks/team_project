# Databricks notebook source
# MAGIC %md
# MAGIC # Lane A — Lakebase setup (apply `contract/ddl_lakebase.sql`)
# MAGIC
# MAGIC Creates the `plant` schema, the `tag_current` and `alert_outbox` tables,
# MAGIC `REPLICA IDENTITY FULL` (required before Lakebase CDF will stream a table), the app's
# MAGIC live-query indexes, and the event trigger that applies full replica identity to any
# MAGIC future table in `plant`. Idempotent — safe to re-run.
# MAGIC
# MAGIC **Prerequisite:** the Lakebase project must already exist. DABs cannot create it; run
# MAGIC `src/create_lakebase_project.sh` (or the CLI one-liner in the README) once first.
# MAGIC
# MAGIC The event trigger needs superuser; the project creator has it. Run this job as the
# MAGIC identity that created the project (or a `databricks_superuser`).

# COMMAND ----------

# MAGIC %pip install psycopg2-binary
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("lakebase_project", "ironbark-ops")
dbutils.widgets.text("lakebase_branch", "production")
dbutils.widgets.text("lakebase_endpoint", "primary")
dbutils.widgets.text("lakebase_database", "databricks_postgres")
dbutils.widgets.text("pg_schema", "plant")

PROJECT  = dbutils.widgets.get("lakebase_project")
BRANCH   = dbutils.widgets.get("lakebase_branch")
ENDPOINT = dbutils.widgets.get("lakebase_endpoint")
DATABASE = dbutils.widgets.get("lakebase_database")
PG_SCHEMA = dbutils.widgets.get("pg_schema")

EP_NAME = f"projects/{PROJECT}/branches/{BRANCH}/endpoints/{ENDPOINT}"

# COMMAND ----------

import psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

try:
    endpoint = w.postgres.get_endpoint(name=EP_NAME)
except Exception as e:
    raise RuntimeError(
        f"Endpoint {EP_NAME} not found. Create the Lakebase project first "
        f"(src/create_lakebase_project.sh or the README one-liner). Underlying error: {e}")

host = endpoint.status.hosts.host
user = w.current_user.me().user_name
token = w.postgres.generate_database_credential(endpoint=endpoint.name).token
print(f"Connecting to {host} as {user}, database {DATABASE}")

conn = psycopg2.connect(host=host, dbname=DATABASE, user=user, password=token, sslmode="require")
conn.autocommit = True
cur = conn.cursor()

# COMMAND ----------

# MAGIC %md ## DDL (faithful to contract/ddl_lakebase.sql)

# COMMAND ----------

DDL = [
    f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA}",

    f"""CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.tag_current (
        tag_id     TEXT        PRIMARY KEY,
        event_id   TEXT        NOT NULL,
        source_ts  TIMESTAMPTZ NOT NULL,
        value      DOUBLE PRECISION,
        value_text TEXT,
        quality    TEXT        NOT NULL,
        unit       TEXT        NOT NULL,
        seq        BIGINT      NOT NULL,
        producer   TEXT        NOT NULL,
        ingest_ts  TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    # Required for Lakebase CDF: without it Postgres logs only the PK on update/delete
    # and the table is silently skipped by the feed.
    f"ALTER TABLE {PG_SCHEMA}.tag_current REPLICA IDENTITY FULL",
    f"CREATE INDEX IF NOT EXISTS ix_tag_current_source_ts ON {PG_SCHEMA}.tag_current (source_ts DESC)",
    f"CREATE INDEX IF NOT EXISTS ix_tag_current_quality   ON {PG_SCHEMA}.tag_current (quality) WHERE quality <> 'GOOD'",

    f"""CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.alert_outbox (
        alert_id        TEXT        PRIMARY KEY,
        raised_at       TIMESTAMPTZ NOT NULL,
        tag_id          TEXT,
        asset_id        TEXT,
        area_code       TEXT,
        node_id         TEXT,
        rule_id         TEXT        NOT NULL,
        rule_name       TEXT        NOT NULL,
        rule_type       TEXT        NOT NULL,
        severity        TEXT        NOT NULL,
        trigger_value   DOUBLE PRECISION,
        trigger_text    TEXT,
        limit_low       DOUBLE PRECISION,
        limit_high      DOUBLE PRECISION,
        message         TEXT        NOT NULL,
        delivery_status TEXT        NOT NULL DEFAULT 'PENDING',
        delivery_attempts INT       NOT NULL DEFAULT 0,
        delivered_at    TIMESTAMPTZ,
        http_status     INT,
        acknowledged_by TEXT,
        acknowledged_at TIMESTAMPTZ
    )""",
    f"ALTER TABLE {PG_SCHEMA}.alert_outbox REPLICA IDENTITY FULL",
    f"""CREATE INDEX IF NOT EXISTS ix_alert_open ON {PG_SCHEMA}.alert_outbox (raised_at DESC)
        WHERE acknowledged_at IS NULL""",

    # Event trigger: apply full replica identity to any future table added to the schema.
    f"""CREATE OR REPLACE FUNCTION {PG_SCHEMA}.set_full_replica_identity()
        RETURNS event_trigger LANGUAGE plpgsql AS $fn$
        DECLARE obj record;
        BEGIN
          FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() WHERE command_tag = 'CREATE TABLE'
          LOOP
            EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL;', obj.object_identity);
          END LOOP;
        END $fn$""",
    "DROP EVENT TRIGGER IF EXISTS set_full_replica_identity_on_create",
    f"""CREATE EVENT TRIGGER set_full_replica_identity_on_create
        ON ddl_command_end WHEN TAG IN ('CREATE TABLE')
        EXECUTE FUNCTION {PG_SCHEMA}.set_full_replica_identity()""",
]

for i, stmt in enumerate(DDL, 1):
    try:
        cur.execute(stmt)
        print(f"[{i:02d}/{len(DDL)}] OK  {stmt.split(chr(10))[0][:70]}")
    except Exception as e:
        print(f"[{i:02d}/{len(DDL)}] ERR {stmt.split(chr(10))[0][:70]}  -> {e}")
        raise

# COMMAND ----------

# MAGIC %md ## Verify replica identity (both tables must report 'full')

# COMMAND ----------

cur.execute(f"""
  SELECT c.relname,
         CASE c.relreplident WHEN 'd' THEN 'default' WHEN 'n' THEN 'nothing'
                             WHEN 'f' THEN 'full' WHEN 'i' THEN 'index' END
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind='r' AND n.nspname = %s ORDER BY c.relname
""", (PG_SCHEMA,))
for name, ident in cur.fetchall():
    flag = "OK" if ident == "full" else "!! NOT FULL"
    print(f"{name:16} replica_identity={ident}  {flag}")

cur.close()
conn.close()
print("\nLakebase setup complete. Next: enable the 'Lakebase Change Data Feed' preview "
      "(admin), then start Lakebase CDF from the plant schema (step 4).")
