#!/usr/bin/env python3
"""Validate the mass balance logic against a design-value fixture.

The design provably closes: gen_reference_data.py refuses to emit the reference
CSVs unless every unit node balances and the wet and dry sides of the desands
circuit reconcile. So if we feed every tag its `nominal` value, a correct
implementation must report imbalance ~= 0 on every node with
balance_role='unit'. Any non-zero node is a bug in the SQL, not the data.

This runs before lane A's seed data exists, so the centrepiece is proven early.

Usage: python3 lane_b/test_mass_balance.py
"""
import json, re, subprocess, sys

P   = "ironbark"
S   = "jack_freeman_catalog.tech_summit_scada_build"
FIX = (sys.argv[1] if len(sys.argv) > 1 else f"{S}.scratch_b_fixture_reading")
USE_FIXTURE = len(sys.argv) <= 1
TOL = 5.0 if len(sys.argv) <= 1 else 60.0   # real data has instrument noise;
                                            # 60 t/h is 2.4% of the 2500 t/h feed

def q(sql, label=""):
    r = subprocess.run(["databricks","experimental","aitools","tools","query",sql,
                        "--profile",P], capture_output=True, text=True)
    out = (r.stdout + r.stderr).strip()
    if r.returncode != 0 or out.lstrip().startswith("Error"):
        det = re.search(r'\[([A-Z_0-9.]+)\]', out)
        print(f"  FAIL {label or sql[:50]}: {det.group(1) if det else out.splitlines()[0][:160]}")
        print(f"       {out[:500]}")
        sys.exit(1)
    m = re.search(r'\[.*\]', out, re.S)
    return json.loads(m.group(0)) if m else []

# ---------------------------------------------------------------- fixture
# Every analog tag held at its design operating point for 5 minutes at 10 s.
# Level tags stay constant, so accumulation is genuinely zero and any
# accumulation term the SQL produces is a bug.
if USE_FIXTURE:
    print("building fixture at design values...")
if USE_FIXTURE: q(f"""
CREATE OR REPLACE TABLE {FIX} AS
SELECT uuid() AS event_id, t.tag_id, ts.source_ts, t.nominal AS value,
       CAST(NULL AS STRING) AS value_text, 'GOOD' AS quality, t.unit,
       CAST(unix_timestamp(ts.source_ts) AS BIGINT) AS seq,
       'fixture' AS producer, ts.source_ts AS ingest_ts
FROM {S}.dim_tag t
CROSS JOIN (SELECT explode(sequence(
              TIMESTAMP'2026-08-27 00:00:00', TIMESTAMP'2026-08-27 00:04:50',
              INTERVAL 10 SECONDS)) AS source_ts) ts
WHERE t.value_class = 'analog' AND t.nominal IS NOT NULL
""", "fixture")
else:
    print(f"running against real data: {FIX}")
n = q(f"SELECT COUNT(*) c, COUNT(DISTINCT tag_id) t FROM {FIX}")[0]
print(f"  {int(n['c']):,} rows across {n['t']} tags\n")

# ------------------------------------------- run the real logic as tables
# One source of truth: mass_balance.sql. Rewritten here from temp views into
# scratch tables, because each CLI query is a separate session so temp views
# would not survive between statements.
sql = open("lane_b/mass_balance.sql").read()
sql = re.sub(r'^USE (CATALOG|SCHEMA).*$', '', sql, flags=re.M)
sql = sql.replace("${src_readings}", FIX)
# Rewrite the b_* dataset names FIRST, so both the CREATE and every FROM
# reference are caught by the same substitution. Doing the CREATE clause first
# leaves the FROM references dangling.
DATASETS = ["reading_1m","windows","slurry_pair","mass_flow_1m_final","mass_flow_1m",
            "arc_infer","node_flow","node_accum","mass_balance_node_1m"]
for name in DATASETS:
    sql = re.sub(rf'(?<![\w.])b_{name}(?![\w])', f"{S}.scratch_b_{name}", sql)
sql = sql.replace("CREATE OR REPLACE TEMPORARY VIEW ", "CREATE OR REPLACE TABLE ")
sql = re.sub(r'(?<![\w.])(dim_tag|dim_flowsheet_arc|dim_flowsheet_node)(?![\w])', rf'{S}.\1', sql)
sql = sql.replace(f"{S}.{S}.", f"{S}.")

def split_sql(t):
    t = "\n".join(l for l in t.splitlines() if not l.lstrip().startswith("--"))
    out, cur, instr = [], [], False
    for ch in t:
        if ch == "'": instr = not instr; cur.append(ch)
        elif ch == ";" and not instr:
            s = "".join(cur).strip()
            if s: out.append(s)
            cur = []
        else: cur.append(ch)
    s = "".join(cur).strip()
    if s: out.append(s)
    return out

for st in split_sql(sql):
    lbl = re.search(r'TABLE\s+[\w.]*?\.(scratch_b_\w+)', st)
    print(f"  step {lbl.group(1) if lbl else st[:40]}")
    q(st, lbl.group(1) if lbl else "")

# ------------------------------------------------------------ assertions
print("\nslurry arcs resolved a density partner:")
sl = q(f"""SELECT a.arc_id, a.measure_tag_id, p.density_tag_id
           FROM {S}.dim_flowsheet_arc a
           LEFT JOIN {S}.scratch_b_slurry_pair p ON p.flow_tag_id = a.measure_tag_id
           WHERE a.measure_method='slurry_flow_density' ORDER BY a.arc_id""")
unresolved = [r for r in sl if not r.get("density_tag_id")]
for r in sl:
    print(f"  {r['arc_id']}  {r['measure_tag_id']} -> {r.get('density_tag_id') or 'UNRESOLVED'}")
if unresolved:
    print("\n  FAIL: a slurry arc has no density partner. dim_flowsheet_arc needs an")
    print("        explicit density_tag_id column; the naming convention has broken.")
    sys.exit(1)

print("\nper-arc measured vs design:")
arcs = q(f"""SELECT arc_id, design_tph, ROUND(AVG(measured_tph),1) measured, measure_method
             FROM {S}.scratch_b_mass_flow_1m_final GROUP BY arc_id, design_tph, measure_method
             ORDER BY arc_id""")
bad_arcs = 0
for r in arcs:
    d = float(r["design_tph"]); mv = r["measured"]
    if mv in (None, ""):
        flag = "  (no solids figure, expected for inferred/water)"
        print(f"  {r['arc_id']:4s} design={d:7.0f} measured=      -{flag}")
        continue
    m = float(mv); dev = m - d
    ok = abs(dev) <= TOL
    bad_arcs += 0 if ok else 1
    print(f"  {r['arc_id']:4s} design={d:7.0f} measured={m:8.1f}  dev={dev:+7.1f} {'' if ok else '  <-- OFF'}")

print("\nnode closure (only balance_role='unit' must close):")
nodes = q(f"""SELECT n.node_id, n.balance_role, b.status,
                     ROUND(AVG(b.mass_in_tph),1) mi, ROUND(AVG(b.mass_out_tph),1) mo,
                     ROUND(AVG(b.accumulation_tph),3) acc, ROUND(AVG(b.imbalance_tph),2) imb
              FROM {S}.scratch_b_mass_balance_node_1m b
              JOIN {S}.dim_flowsheet_node n ON n.node_id = b.node_id
              GROUP BY n.node_id, n.balance_role, b.status ORDER BY n.node_id""")
bad = 0
for r in nodes:
    role = r["balance_role"]; imb = r["imb"]
    f = lambda v: "     -" if v in (None,"") else f"{float(v):7.1f}"
    if role == "unit" and r["status"] != "ESTIMATED":
        if imb in (None,""):
            print(f"  {r['node_id']} {role:6s} in={f(r['mi'])} out={f(r['mo'])} imb=  NULL  {r['status']}  <-- NO IMBALANCE COMPUTED")
            bad += 1; continue
        v = float(imb); ok = abs(v) <= TOL; bad += 0 if ok else 1
        print(f"  {r['node_id']} {role:6s} in={f(r['mi'])} out={f(r['mo'])} acc={float(r['acc']):+7.3f} imb={v:+8.2f}  {r['status']}{'' if ok else '   <-- DOES NOT CLOSE'}")
    else:
        extra = "  (balance used to reconcile an unmeasured arc)" if r["status"]=="ESTIMATED" else ""
        print(f"  {r['node_id']} {role:6s} in={f(r['mi'])} out={f(r['mo'])} imb={'' if r['imb'] in (None,'') else format(float(r['imb']),'+8.2f'):>8s}  {r['status']}{extra}")

print(f"\n  arcs off design : {bad_arcs}")
print(f"  unit nodes failing to close: {bad}")
sys.exit(1 if (bad or bad_arcs) else 0)
