#!/usr/bin/env python3
"""Embed operator notes with an in-account Databricks embedding endpoint.

Uses databricks-gte-large-en (1024 dims), served inside the customer's own
workspace, so note text never leaves the account. That is the whole point of
doing retrieval in Lakebase rather than shipping text to an external service.
"""
import json, os, subprocess, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lb import connect

P = "ironbark"
ENDPOINT = "databricks-gte-large-en"

def embed(texts):
    # Serving invocations do not live under /api/2.0, so `databricks api post`
    # cannot reach them. The dedicated query command handles the routing.
    r = subprocess.run(["databricks","serving-endpoints","query", ENDPOINT,
                        "--json", json.dumps({"input": texts}),
                        "--profile", P, "-o", "json"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"embedding call failed:\n{(r.stderr or r.stdout)[:600]}")
    d = json.loads(r.stdout)
    return [item["embedding"] for item in d["data"]]

conn, _ = connect("production")
with conn:
    rows = conn.execute("""SELECT note_id, note_text FROM plant.work_order_note
                           WHERE note_embedding IS NULL ORDER BY note_id""").fetchall()
    print(f"{len(rows)} notes need embedding")
    B = 16
    done = 0
    for i in range(0, len(rows), B):
        chunk = rows[i:i+B]
        vecs = embed([t for _, t in chunk])
        for (nid, _), v in zip(chunk, vecs):
            conn.execute("UPDATE plant.work_order_note SET note_embedding = %s WHERE note_id = %s",
                         (str(v), nid))
        done += len(chunk)
        print(f"  embedded {done}/{len(rows)}")
    n, dim = conn.execute("""SELECT count(*), max(vector_dims(note_embedding))
                             FROM plant.work_order_note
                             WHERE note_embedding IS NOT NULL""").fetchone()
    print(f"  {n} notes embedded, {dim} dimensions")
