-- =====================================================================
-- Ironbark operational schema on Lakebase Postgres: main branch baseline
-- =====================================================================
-- Build 1 requirement 5: model the operational schema for the customer's
-- domain with related tables and keys, not a single flat dump, and include
-- at least one text field the assistant can search.
--
-- The domain write path a control room actually needs:
--   pipeline raises an alarm  ->  operator raises a work order against it
--   ->  operators append notes as they diagnose and fix it
--
-- Read/write split, per requirement 1:
--   plant.dim_tag       SYNCED from governed Unity Catalog. Read-only here.
--   plant.tag_current   writable, live values upserted by the ingest pipeline
--   plant.alert_outbox  writable, alarms raised by the rule engine
--   plant.work_order    writable, operator actions. Carries the free prose
--                       that Lakebase Search indexes.
-- =====================================================================

SET search_path TO plant, public;

-- ---------------------------------------------------------------------
-- Work orders. The application's write path: an operator turns an alarm
-- into a tracked action. This is deliberately NOT synced from Unity
-- Catalog, because a synced table is read-only in Postgres and an app
-- needs somewhere writable for its own state.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS work_order (
  work_order_id     TEXT        PRIMARY KEY,
  alert_id          TEXT        REFERENCES alert_outbox(alert_id),
  asset_id          TEXT        NOT NULL,
  tag_id            TEXT,
  area_code         TEXT        NOT NULL,
  status            TEXT        NOT NULL DEFAULT 'OPEN'
                                CHECK (status IN ('OPEN','IN_PROGRESS','ON_HOLD','CLOSED','CANCELLED')),
  priority          TEXT        NOT NULL DEFAULT 'MEDIUM'
                                CHECK (priority IN ('CRITICAL','HIGH','MEDIUM','LOW')),
  failure_mode      TEXT,
  -- Free prose. This is what Lakebase Search indexes, and the reason the
  -- assistant can answer questions without data leaving the account.
  title             TEXT        NOT NULL,
  description       TEXT        NOT NULL,
  resolution_notes  TEXT,
  raised_by         TEXT        NOT NULL,
  assigned_to       TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  closed_at         TIMESTAMPTZ,
  downtime_minutes  INTEGER,
  CONSTRAINT closed_has_timestamp
    CHECK ((status = 'CLOSED') = (closed_at IS NOT NULL))
);

-- Required for Lakebase CDF, so the work order audit trail reaches Unity
-- Catalog as SCD Type 2 history. Without it Postgres logs only the primary
-- key on update and the table is silently skipped by the feed.
ALTER TABLE work_order REPLICA IDENTITY FULL;

CREATE INDEX IF NOT EXISTS ix_wo_asset   ON work_order (asset_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_wo_open    ON work_order (priority, created_at DESC)
  WHERE status IN ('OPEN','IN_PROGRESS');
CREATE INDEX IF NOT EXISTS ix_wo_alert   ON work_order (alert_id);
