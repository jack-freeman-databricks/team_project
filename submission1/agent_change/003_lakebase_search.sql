-- Migration 003: Lakebase Search (hybrid vector plus full text) over operator notes
--
-- Authored by Claude (Anthropic) via Claude Code.
--
-- Two halves, because operators search in two different ways:
--   full text (BM25) catches exact terms: a tag id, a part number, "outer race"
--   vector (ANN, cosine) catches meaning: "bearing going bad" finding a note
--                        that says "play in the outer race by hand"
-- Neither alone is enough, so the query combines them with reciprocal rank
-- fusion. Embeddings come from databricks-gte-large-en, an in-account serving
-- endpoint, so retrieval never leaves the customer's account.

SET search_path TO plant, public;

ALTER TABLE work_order_note
  ADD COLUMN IF NOT EXISTS note_tsv tsvector
    GENERATED ALWAYS AS (to_tsvector('english', note_text)) STORED;

-- gte-large-en emits 1024 dimensions.
ALTER TABLE work_order_note
  ADD COLUMN IF NOT EXISTS note_embedding vector(1024);

-- BM25 ranking, queried as: note_tsv <@> to_bm25query(query_tsv, 'ix_bm25_note')
CREATE INDEX IF NOT EXISTS ix_bm25_note
  ON work_order_note USING lakebase_bm25 (note_tsv tsvector_bm25_ops);

-- Approximate nearest neighbour on cosine distance.
CREATE INDEX IF NOT EXISTS ix_ann_note
  ON work_order_note USING lakebase_ann (note_embedding vector_cosine_ops);
