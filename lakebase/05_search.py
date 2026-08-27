#!/usr/bin/env python3
"""Hybrid Lakebase Search over operator notes: BM25 fused with vector ANN.

Reciprocal rank fusion: score = sum over each ranked list of 1/(k + rank).
k=60 is the conventional damping constant. RRF is used rather than adding raw
scores because BM25 magnitudes and cosine distances are not on a common scale,
so summing them would let whichever has the larger range dominate.
"""
import json, os, re, subprocess, sys, decimal, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lb import connect

P = "ironbark"
Q = "bearing starting to fail on a crusher, what did we find and what did we fit"

def or_tsquery(text):
    """Build an OR tsquery from the content words of a natural-language question.

    websearch_to_tsquery ANDs its terms, so a whole sentence matches nothing.
    Removing the filter entirely is worse: BM25 then ranks every row and returns
    spurious top hits that swamp the vector half through RRF. ORing the content
    words keeps only rows that share at least one meaningful term, and lets BM25
    rank within that candidate set.
    """
    stop = {"what","did","we","and","on","a","the","to","is","are","of","in","for",
            "with","how","why","it","that","this","was","were","has","have","been"}
    words = [w for w in re.findall(r"[a-z]{3,}", text.lower()) if w not in stop]
    return " | ".join(dict.fromkeys(words))


def embed(text):
    r = subprocess.run(["databricks","serving-endpoints","query","databricks-gte-large-en",
                        "--json", json.dumps({"input": [text]}), "--profile", P, "-o","json"],
                       capture_output=True, text=True)
    if r.returncode != 0: raise SystemExit((r.stderr or r.stdout)[:500])
    return json.loads(r.stdout)["data"][0]["embedding"]

SQL = """
-- to_bm25query takes a tsvector, but the @@ containment filter takes a tsquery,
-- so the query text is prepared both ways.
WITH q AS (SELECT to_tsvector('english', %(q)s)   AS qtsv,
                  to_tsquery('english', %(orq)s)   AS qquery,
                  %(vec)s::vector                  AS qvec),
bm AS (
  SELECT n.note_id,
         ROW_NUMBER() OVER (ORDER BY n.note_tsv <@> to_bm25query(q.qtsv, 'plant.ix_bm25_note') DESC) AS rnk,
         n.note_tsv <@> to_bm25query(q.qtsv, 'plant.ix_bm25_note') AS bm25
  FROM plant.work_order_note n, q
  WHERE n.note_tsv @@ q.qquery      -- OR over content words, see or_tsquery()
  ORDER BY bm25 DESC LIMIT 20
),
ann AS (
  SELECT n.note_id,
         ROW_NUMBER() OVER (ORDER BY n.note_embedding <=> q.qvec) AS rnk,
         1 - (n.note_embedding <=> q.qvec) AS cosine_sim
  FROM plant.work_order_note n, q
  WHERE n.note_embedding IS NOT NULL
  ORDER BY n.note_embedding <=> q.qvec LIMIT 20
),
fused AS (
  SELECT COALESCE(bm.note_id, ann.note_id) AS note_id,
         COALESCE(1.0/(60+bm.rnk), 0) + COALESCE(1.0/(60+ann.rnk), 0) AS rrf,
         bm.rnk AS bm25_rank, ann.rnk AS vector_rank,
         bm.bm25, ann.cosine_sim
  FROM bm FULL OUTER JOIN ann ON bm.note_id = ann.note_id
),
ranked AS (
  SELECT f.note_id, f.rrf, f.bm25_rank, f.vector_rank, f.bm25, f.cosine_sim,
         w.work_order_id, w.asset_id, w.failure_mode, w.title, w.status,
         n.note_kind, n.note_text,
         ROW_NUMBER() OVER (PARTITION BY w.work_order_id ORDER BY f.rrf DESC) AS per_wo
  FROM fused f
  JOIN plant.work_order_note n ON n.note_id = f.note_id
  JOIN plant.work_order w      ON w.work_order_id = n.work_order_id
)
SELECT note_id, ROUND(rrf::numeric, 5) AS rrf_score, bm25_rank, vector_rank,
       ROUND(bm25::numeric, 3) AS bm25_score, ROUND(cosine_sim::numeric, 4) AS cosine_sim,
       work_order_id, asset_id, failure_mode, title, status, note_kind, note_text
FROM ranked WHERE per_wo = 1
ORDER BY rrf DESC
LIMIT 5
"""

def enc(o):
    if isinstance(o, decimal.Decimal): return float(o)
    if isinstance(o, (datetime.datetime, datetime.date)): return o.isoformat()
    return str(o)

conn, _ = connect("production")
with conn:
    cur = conn.execute(SQL, {"q": Q, "orq": or_tsquery(Q), "vec": str(embed(Q))})
    cols = [d.name for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
json.dump({"query": Q, "method": "hybrid BM25 + vector ANN, reciprocal rank fusion (k=60)",
           "embedding_endpoint": "databricks-gte-large-en (1024d, in-account)",
           "results": rows}, open("submission1/search_result.json","w"), indent=2, default=enc)
print(f'query: "{Q}"\n')
for i, r in enumerate(rows, 1):
    print(f"{i}. {r['work_order_id']}  {r['asset_id']:12s} mode={str(r['failure_mode']):12s} "
          f"kind={r['note_kind']}")
    print(f"   rrf={float(r['rrf_score']):.5f}  bm25_rank={r['bm25_rank']}  vec_rank={r['vector_rank']}  "
          f"cos={r['cosine_sim']}")
    print(f"   {r['note_text'][:150]}...")
print(f"\n{len(rows)} results -> submission1/search_result.json")
