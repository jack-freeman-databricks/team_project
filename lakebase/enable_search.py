#!/usr/bin/env python3
"""Enable Lakebase Search (hybrid vector plus full text) on the plant schema.

Two prerequisites, in this order:

  1. pgvector must exist BEFORE lakebase_vector. It is NOT created
     automatically. lakebase_text can be created on its own.
  2. Both lakebase_* extensions must be present in shared_preload_libraries.
     That is a server-level setting, not something CREATE EXTENSION can do, and
     it is gated behind the Lakebase Search Beta on the workspace Previews page
     (workspace admin, no CLI or API path at time of writing).

This script reports precisely which prerequisite is missing rather than
half-failing, so the blocker is unambiguous.
"""
import os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lb import connect

SEARCHABLE = [
    ("plant.work_order_note", "note_text"),
    ("plant.work_order",      "description"),
    ("plant.work_order",      "resolution_notes"),
]

conn, host = connect(os.environ.get("LB_BRANCH", "production"))
with conn:
    preload = conn.execute("SHOW shared_preload_libraries").fetchone()[0]
    print(f"shared_preload_libraries: {preload}")

    conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    print("  vector: ok (pgvector, required before lakebase_vector)")

    missing = [e for e in ("lakebase_vector", "lakebase_text") if e not in preload]
    if missing:
        print(f"\nBLOCKED: {', '.join(missing)} not in shared_preload_libraries.")
        print("A workspace admin must enable the Lakebase Search preview:")
        print("  Settings -> Previews -> Lakebase Search")
        print("There is no CLI or REST path for this. Re-run once enabled.")
        sys.exit(2)

    for ext in ("lakebase_vector", "lakebase_text"):
        conn.execute(f"CREATE EXTENSION IF NOT EXISTS {ext}")
        print(f"  {ext}: ok")

    # Hybrid search over the operator's own words: vector for meaning,
    # full text for exact terms like a tag id or a part number.
    for tbl, col in SEARCHABLE:
        short = f"{tbl.split('.')[-1]}_{col}"
        conn.execute(f"""CREATE INDEX IF NOT EXISTS ix_fts_{short}
                         ON {tbl} USING GIN (to_tsvector('english', COALESCE({col}, '')))""")
        print(f"  full-text index on {tbl}.{col}")
    print("\nLakebase Search ready.")
