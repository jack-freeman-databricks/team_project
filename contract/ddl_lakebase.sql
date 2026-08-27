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
-- Note: tokens expire after 1 hour. A continuously running pipeline that
-- upserts from a foreach_batch_sink must refresh its credential, not cache one
-- at pipeline start. This is the single most likely cause of the pipeline
-- failing roughly an hour in.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS plant;
SET search_path TO plant, public;

-- ---------------------------------------------------------------------
-- tag_current: one row per tag, upserted by the pipeline's
-- foreach_batch_sink handler. This is the hot operational store the
-- control room app reads for live tiles.
--
-- Because every value change is an UPDATE, Lakehouse Sync's CDC stream
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

-- Every column type above is on the Lakehouse Sync supported list
-- (text, timestamptz, float8, int8, bool, numeric, jsonb). Do not add
-- arrays, structs or unsupported types: the sync will reject the table.

-- REQUIRED for Lakehouse Sync. Without this, CDC captures no before-image
-- and the sync fails to start.
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
-- Mirrored to ironbark.analytics.alert through the same Lakehouse Sync.
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

CREATE INDEX IF NOT EXISTS ix_alert_open
  ON alert_outbox (raised_at DESC)
  WHERE acknowledged_at IS NULL;

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
-- Verify replica identity before enabling Lakehouse Sync. Both tables
-- must report 'full'.
-- ---------------------------------------------------------------------
-- SELECT c.relname AS table_name,
--        CASE c.relreplident WHEN 'd' THEN 'default' WHEN 'n' THEN 'nothing'
--                            WHEN 'f' THEN 'full'    WHEN 'i' THEN 'index' END AS replica_identity
-- FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
-- WHERE c.relkind = 'r' AND n.nspname = 'plant';
