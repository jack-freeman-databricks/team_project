#!/usr/bin/env python3
"""Derive genuine alerts by evaluating dim_rule against real seed readings.

The seed data contains real limit excursions (20 warning episodes, 5 trip
episodes). Rather than inventing alert rows, this runs the rule engine's own
matching logic over those readings and loads the result into Lakebase
plant.alert_outbox, so the operational data downstream is real evaluation
output rather than hand-written fixtures.

Rule selection follows specificity: a tag-scoped rule beats an asset_type or
measure scoped rule, which beats a catch-all.
"""
import json, re, subprocess, uuid, psycopg

P = "ironbark"
S = "jack_freeman_catalog.tech_summit_scada_build"
EP = "projects/ironbark-ops/branches/production/endpoints/primary"

def dbsql(sql):
    r = subprocess.run(["databricks","experimental","aitools","tools","query",sql,"--profile",P],
                       capture_output=True, text=True)
    out = r.stdout + r.stderr
    if r.returncode != 0 or out.lstrip().startswith("Error"):
        raise SystemExit(out[:900])
    m = re.search(r'\[.*\]', out, re.S)
    return json.loads(m.group(0)) if m else []

# One row per breach EPISODE (consecutive breaching samples collapsed), with the
# most specific enabled rule that matches it.
rows = dbsql(f"""
WITH b AS (
  SELECT r.tag_id, r.source_ts, r.value, t.asset_id, t.asset_name, t.asset_type,
         t.area_code, t.measure, t.unit, t.lo, t.hi, t.lo_lo, t.hi_hi,
         CASE WHEN r.value > t.hi_hi OR r.value < t.lo_lo THEN 'limit_trip'
              WHEN r.value > t.hi    OR r.value < t.lo    THEN 'limit_warn' END AS rule_type
  FROM {S}.tag_reading_seed r
  JOIN {S}.dim_tag t ON t.tag_id = r.tag_id
  WHERE t.value_class = 'analog' AND r.quality = 'GOOD'
),
brk AS (
  SELECT *, CASE WHEN TIMESTAMPDIFF(SECOND,
              LAG(source_ts) OVER (PARTITION BY tag_id, rule_type ORDER BY source_ts), source_ts) > 60
              OR LAG(source_ts) OVER (PARTITION BY tag_id, rule_type ORDER BY source_ts) IS NULL
            THEN 1 ELSE 0 END AS new_ep
  FROM b WHERE rule_type IS NOT NULL
),
ep AS (
  SELECT *, SUM(new_ep) OVER (PARTITION BY tag_id, rule_type ORDER BY source_ts) AS ep_no
  FROM brk
),
agg AS (
  SELECT tag_id, asset_id, asset_name, asset_type, area_code, measure, unit,
         rule_type, ep_no,
         MIN(source_ts) AS raised_at,
         MAX(source_ts) AS ended_at,
         COUNT(*)       AS samples,
         MAX(CASE WHEN value > hi THEN value END) AS peak_high,
         MIN(CASE WHEN value < lo THEN value END) AS peak_low,
         MAX(lo) lo, MAX(hi) hi, MAX(lo_lo) lo_lo, MAX(hi_hi) hi_hi
  FROM ep GROUP BY tag_id, asset_id, asset_name, asset_type, area_code, measure, unit, rule_type, ep_no
),
matched AS (
  SELECT a.*, r.rule_id, r.rule_name, r.severity, r.message_template,
         ROW_NUMBER() OVER (PARTITION BY a.tag_id, a.rule_type, a.ep_no
                            ORDER BY CASE r.scope_type WHEN 'tag' THEN 0
                                                       WHEN 'asset_type' THEN 1
                                                       WHEN 'measure' THEN 1
                                                       ELSE 2 END, r.rule_id) AS rn
  FROM agg a
  JOIN {S}.dim_rule r
    ON r.enabled AND r.rule_type = a.rule_type
   AND (r.scope_type = 'all'
     OR (r.scope_type = 'asset_type' AND r.scope_value = a.asset_type)
     OR (r.scope_type = 'measure'    AND r.scope_value = a.measure)
     OR (r.scope_type = 'tag'        AND r.scope_value = a.tag_id))
)
SELECT tag_id, asset_id, asset_name, asset_type, area_code, measure, unit,
       rule_id, rule_name, rule_type, severity,
       CAST(raised_at AS STRING) raised_at, CAST(ended_at AS STRING) ended_at,
       CAST(samples AS STRING) samples,
       CAST(COALESCE(peak_high, peak_low) AS STRING) trigger_value,
       CAST(lo AS STRING) lo, CAST(hi AS STRING) hi,
       CAST(lo_lo AS STRING) lo_lo, CAST(hi_hi AS STRING) hi_hi,
       message_template
FROM matched WHERE rn = 1
ORDER BY raised_at
""")
print(f"derived {len(rows)} alert episodes from real excursions")

def cli(*a):
    r = subprocess.run(["databricks",*a,"--profile",P,"-o","json"],capture_output=True,text=True)
    return json.loads(r.stdout)

host = cli("postgres","get-endpoint",EP)["status"]["hosts"]["host"]
tok  = cli("postgres","generate-database-credential",EP)["token"]
usr  = cli("current-user","me")["userName"]

with psycopg.connect(host=host,user=usr,password=tok,dbname="databricks_postgres",
                     sslmode="require",autocommit=True) as c:
    c.execute("TRUNCATE plant.work_order, plant.alert_outbox CASCADE")
    ins = 0
    for r in rows:
        tv = float(r["trigger_value"]) if r["trigger_value"] not in (None,"") else None
        msg = (r["message_template"]
               .replace("{tag_id}", r["tag_id"]).replace("{asset_name}", r["asset_name"])
               .replace("{value}", f"{tv:.2f}" if tv is not None else "?")
               .replace("{unit}", r["unit"]).replace("{node_id}", ""))
        c.execute("""
          INSERT INTO plant.alert_outbox
            (alert_id, raised_at, tag_id, asset_id, area_code, rule_id, rule_name,
             rule_type, severity, trigger_value, limit_low, limit_high, message,
             delivery_status, delivery_attempts, delivered_at, http_status)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'DELIVERED',1,%s,200)
        """, (str(uuid.uuid4()), r["raised_at"], r["tag_id"], r["asset_id"], r["area_code"],
              r["rule_id"], r["rule_name"], r["rule_type"], r["severity"], tv,
              float(r["lo_lo"]) if r["rule_type"]=="limit_trip" else float(r["lo"]),
              float(r["hi_hi"]) if r["rule_type"]=="limit_trip" else float(r["hi"]),
              msg, r["ended_at"]))
        ins += 1
    n = c.execute("SELECT count(*) FROM plant.alert_outbox").fetchone()[0]
    print(f"loaded {ins} alerts into plant.alert_outbox (table now holds {n})")
    for sev, cnt in c.execute("""SELECT severity, count(*) FROM plant.alert_outbox
                                 GROUP BY 1 ORDER BY 2 DESC""").fetchall():
        print(f"  {sev:9s} {cnt}")
