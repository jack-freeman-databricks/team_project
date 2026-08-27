-- =====================================================================
-- Ironbark: Lakebase Postgres DDL
-- =====================================================================
-- Run against the Lakebase project's `databricks_postgres` database.
--
--   EP=projects/ironbark-ops/branches/production/endpoints/primary
--   HOST=$(databricks postgres get-endpoint $EP --profile <p> -o json \
--          | python3 -c "import json,sys; print(json.load(sys.stdin)['status']['hosts']['host'])")
--   TOKEN=$(databricks postgres generate-database-credential $EP --profile <p> -o json \
--          | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])")
--   PGPASSWORD="$TOKEN" psql "host=$HOST user=<user> dbname=databricks_postgres sslmode=require" \
--     -f contract/ddl_lakebase.sql
--
-- Note on credentials: the tokens minted above expire after 1 hour, which
-- matters ONLY for the fallback path (a Lakeflow @dp.foreach_batch_sink that
-- opens its own connection). It does NOT apply to the recommended path: the
-- native format("postgresql") sink manages credentials itself and runs as the
-- query's identity. Do not go hunting a token-refresh bug on the native path.
--
-- The one auth subtlety on the recommended path is Q2, the alert writer: an
-- executor-side custom `foreach` sink needs native Postgres password auth from
-- a Databricks secret scope, because OAuth refresh needs SDK context that
-- executors do not have. Create that role and password here, driver-side.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS plant;
SET search_path TO plant, public;

-- ---------------------------------------------------------------------
-- tag_current: one row per tag, upserted by the pipeline's
-- foreach_batch_sink handler. This is the hot operational store the
-- control room app reads for live tiles.
--
-- Because every value change is an UPDATE, the Lakebase CDF stream
-- carries the full history to Unity Catalog. That is deliberate: the
-- current state serves the app, and the change feed becomes the historian.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tag_current (
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
);

-- Every column type above maps cleanly through Lakebase CDF. Note how they
-- arrive in Unity Catalog, because lane B builds against the result:
--   text        -> STRING
--   timestamptz -> TIMESTAMP
--   timestamp   -> TIMESTAMP_NTZ   (so prefer timestamptz, as used here)
--   float8      -> DOUBLE
--   bigint      -> BIGINT
-- Types with no Delta equivalent (PostGIS, pgvector, composite types, hstore)
-- land as STRING. Do not add any here.
--
-- The native format("postgresql") sink would auto-create this table and infer
-- the upsert key from its PRIMARY KEY. We create it explicitly anyway so the
-- column types, the indexes and the replica identity are all deliberate.

-- REQUIRED for Lakebase CDF. Without it Postgres logs only the primary key on
-- update and delete, and the table is silently skipped by the feed.
ALTER TABLE tag_current REPLICA IDENTITY FULL;

-- The app's live queries: latest value for an asset, and stalest tags.
CREATE INDEX IF NOT EXISTS ix_tag_current_source_ts ON tag_current (source_ts DESC);
CREATE INDEX IF NOT EXISTS ix_tag_current_quality   ON tag_current (quality) WHERE quality <> 'GOOD';

-- The upsert the sink handler runs. One statement per micro-batch via
-- execute_values, not per row.
--
--   INSERT INTO plant.tag_current AS t (
--     tag_id, event_id, source_ts, value, value_text,
--     quality, unit, seq, producer, ingest_ts, updated_at)
--   VALUES %s
--   ON CONFLICT (tag_id) DO UPDATE SET
--     event_id   = EXCLUDED.event_id,
--     source_ts  = EXCLUDED.source_ts,
--     value      = EXCLUDED.value,
--     value_text = EXCLUDED.value_text,
--     quality    = EXCLUDED.quality,
--     unit       = EXCLUDED.unit,
--     seq        = EXCLUDED.seq,
--     producer   = EXCLUDED.producer,
--     ingest_ts  = EXCLUDED.ingest_ts,
--     updated_at = now()
--   WHERE EXCLUDED.seq > t.seq;      -- never let a late batch overwrite a newer read

-- ---------------------------------------------------------------------
-- alert_outbox: alerts the pipeline raised, so the app can show them
-- immediately and acknowledge them without waiting on the analytics path.
-- Mirrored into Unity Catalog through the same Lakebase CDF feed.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_outbox (
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
);

ALTER TABLE alert_outbox REPLICA IDENTITY FULL;

-- Note: Lakebase CDF skips empty tables until they hold at least one row, so
-- ironbark.raw.lb_alert_outbox_history will not exist until the first alert
-- fires. That is expected, not a broken feed.

CREATE INDEX IF NOT EXISTS ix_alert_open
  ON alert_outbox (raised_at DESC)
  WHERE acknowledged_at IS NULL;

-- ---------------------------------------------------------------------
-- Belt and braces: CDF is configured at schema level, so any table added to
-- `plant` later joins the feed automatically -- but only if it has replica
-- identity set. This event trigger applies it to every future table, so nobody
-- has to remember.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION plant.set_full_replica_identity()
RETURNS event_trigger
LANGUAGE plpgsql
AS $fn$
DECLARE obj record;
BEGIN
  FOR obj IN
    SELECT * FROM pg_event_trigger_ddl_commands()
    WHERE command_tag = 'CREATE TABLE'
  LOOP
    EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL;', obj.object_identity);
  END LOOP;
END $fn$;

DROP EVENT TRIGGER IF EXISTS set_full_replica_identity_on_create;
CREATE EVENT TRIGGER set_full_replica_identity_on_create
  ON ddl_command_end
  WHEN TAG IN ('CREATE TABLE')
  EXECUTE FUNCTION plant.set_full_replica_identity();

-- ---------------------------------------------------------------------
-- Grants for the app's service principal. Run AFTER the app is first
-- deployed, once its SP exists. Get the client id from:
--   databricks apps get ironbark-control-room --profile <p>  -> service_principal_client_id
--
-- Note the ownership trap: an app SP with CAN_CONNECT_AND_CREATE cannot use a
-- schema it does not own. Because `plant` is created here by a human, the SP
-- needs explicit grants (below) rather than relying on creating it itself.
-- ---------------------------------------------------------------------
-- GRANT USAGE ON SCHEMA plant TO "<sp_client_id>";
-- GRANT SELECT ON ALL TABLES IN SCHEMA plant TO "<sp_client_id>";
-- GRANT UPDATE (acknowledged_by, acknowledged_at) ON plant.alert_outbox TO "<sp_client_id>";
-- ALTER DEFAULT PRIVILEGES IN SCHEMA plant GRANT SELECT ON TABLES TO "<sp_client_id>";

-- ---------------------------------------------------------------------
-- Verify replica identity before starting Lakebase CDF. Both tables must
-- report 'full'. Feed state, once running, is visible via:
--   SELECT * FROM wal2delta.tables;
-- ---------------------------------------------------------------------
-- SELECT c.relname AS table_name,
--        CASE c.relreplident WHEN 'd' THEN 'default' WHEN 'n' THEN 'nothing'
--                            WHEN 'f' THEN 'full'    WHEN 'i' THEN 'index' END AS replica_identity
-- FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
-- WHERE c.relkind = 'r' AND n.nspname = 'plant';
