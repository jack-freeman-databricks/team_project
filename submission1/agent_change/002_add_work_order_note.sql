-- Migration 002: append-only operator notes on a work order
--
-- Authored by Claude (Anthropic) via Claude Code, on Lakebase development
-- branch `dev-work-order-notes`, off the root branch `production`.
--
-- Why: a work order carries a single description and a single resolution, but
-- diagnosis is iterative. Operators need to append findings as a job
-- progresses, and those notes are the richest free text in the whole
-- operational schema, which makes them the natural target for Lakebase
-- Search. One work order has many notes, so this is a genuine 1:N
-- relationship rather than more columns on the parent.

SET search_path TO plant, public;

CREATE TABLE IF NOT EXISTS work_order_note (
  note_id       BIGSERIAL   PRIMARY KEY,
  work_order_id TEXT        NOT NULL REFERENCES work_order(work_order_id) ON DELETE CASCADE,
  author        TEXT        NOT NULL,
  note_kind     TEXT        NOT NULL DEFAULT 'DIAGNOSIS'
                            CHECK (note_kind IN ('DIAGNOSIS','ACTION','PARTS','HANDOVER','SAFETY')),
  note_text     TEXT        NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Required for Lakebase CDF: without full replica identity Postgres logs only
-- the primary key on update and delete, and the table is skipped by the feed.
ALTER TABLE work_order_note REPLICA IDENTITY FULL;

CREATE INDEX IF NOT EXISTS ix_won_wo     ON work_order_note (work_order_id, created_at);
CREATE INDEX IF NOT EXISTS ix_won_author ON work_order_note (author, created_at DESC);
