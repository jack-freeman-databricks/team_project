#!/usr/bin/env python3
"""Raise work orders against the real alerts in plant.alert_outbox.

The operator narrative is synthetic (this is a fictional company), but every
work order is anchored to a real alert: real asset, real tag, real breached
value and real timestamp. The prose is written per failure mode so Lakebase
Search has genuine domain language to index rather than lorem filler.
"""
import json, random, re, subprocess, uuid, psycopg

P="ironbark"; EP="projects/ironbark-ops/branches/production/endpoints/primary"
random.seed(27)

# Maintenance narrative per measure. Keyed to what the instrument actually saw,
# so the language a mining reader expects is present: ISO zones, bearing
# defects, chokes, weightometer calibration, cyclone spigot wear.
NARR = {
 "vibration": ("Elevated bearing vibration on {asset}",
   "Vibration on {tag} peaked at {val:.2f} mm/s against an alarm limit of {lim:.1f} mm/s, "
   "putting the bearing in ISO 10816 zone C. Attended site and took spectral readings at "
   "the drive end. Dominant peak at the outer race defect frequency with sidebands, "
   "consistent with early spalling rather than imbalance or looseness. Crest factor and "
   "kurtosis both elevated ahead of overall RMS, which is the expected early signature. "
   "Greased and rechecked, vibration unchanged, so this is mechanical not lubrication.",
   "Replaced the drive end bearing and housing seal. Alignment checked with laser and "
   "corrected, coupling shims replaced. Post-repair vibration back to 3.1 mm/s in zone A. "
   "Old bearing sent for analysis, outer race spalling confirmed over roughly 30 degrees of "
   "the raceway. Recommend shortening the greasing interval on this unit."),
 "temperature": ("Bearing temperature rising on {asset}",
   "Bearing temperature on {tag} reached {val:.1f} degC against a {lim:.1f} degC alarm. "
   "Rise developed over the shift rather than stepping, so ruling out an instrument fault. "
   "Lube oil level normal and cooler outlet within range, so heat is being generated at the "
   "bearing rather than lost from the cooling circuit. Vibration on the same housing is also "
   "trending up, which points at the bearing rather than the oil supply.",
   "Flushed the lube circuit and replaced the filter element, which was heavily loaded with "
   "fine metallic debris. Temperature settled to 61 degC. Debris finding logged against the "
   "bearing condition case for this asset."),
 "pressure": ("Crushing pressure excursion on {asset}",
   "Hydraulic pressure on {tag} hit {val:.0f} kPa against a {lim:.0f} kPa trip. Classic choke "
   "signature: feed surged while the discharge conveyor was still ramping, so the crushing "
   "chamber packed. Operator dumped the chamber and restarted on reduced feed. No mechanical "
   "damage found on inspection, mantle and concave wear within limits.",
   "Adjusted the feed ramp interlock so the apron feeder cannot lead the discharge conveyor. "
   "Reviewed the closed side setting, still within spec so left alone. Monitoring for repeat."),
 "level": ("Bin level excursion on {asset}",
   "Level on {tag} reached {val:.1f} percent against a {lim:.1f} percent alarm, close to "
   "overflow. Reclaim rate had dropped while feed continued. Level transmitter cross-checked "
   "against the sight glass and reading true, so this is a genuine process excursion and not "
   "an instrument fault.",
   "Cleared a partial blockage on the reclaim chute and restored draw down. Reviewed the "
   "reclaim rate interlock so feed cuts automatically above 90 percent."),
 "mass_flow": ("Weightometer reading suspect on {asset}",
   "Mass flow on {tag} read {val:.0f} t/h against a {lim:.0f} t/h alarm limit. The reading is "
   "inconsistent with the belt either side of it, and the mass balance across this node stopped "
   "closing at the same time, so the instrument is suspect rather than the process.",
   "Recalibrated the weightometer with test chains and corrected a 2.4 percent span error. "
   "Belt scale idlers cleaned and one seized roller replaced. Node balance closing again."),
 "density": ("Cyclone density off spec on {asset}",
   "Slurry density on {tag} read {val:.3f} t/m3 against a {lim:.3f} t/m3 limit. Underflow was "
   "running dilute, so solids were reporting to overflow and the fines reject rate climbed. "
   "Suspect spigot wear on the cyclone cluster.",
   "Replaced two worn spigots and rebalanced the cluster feed. Underflow density back to "
   "1.85 t/m3 and fines reject back within target."),
 "motor_current": ("Motor current excursion on {asset}",
   "Motor current on {tag} reached {val:.0f} A against a {lim:.0f} A limit. Load rose without a "
   "corresponding throughput increase, so the machine is working harder for the same output.",
   "Found a dragging skirt liner adding parasitic load. Adjusted clearance and current returned "
   "to normal."),
 "volume_flow": ("Process water flow excursion on {asset}",
   "Flow on {tag} read {val:.0f} m3/h against a {lim:.0f} m3/h limit, upsetting the desands "
   "circuit water balance and the resulting slurry density.",
   "Reset the control valve positioner, which had drifted, and retuned the flow loop."),
}
FALLBACK = ("Process excursion on {asset}",
  "Tag {tag} breached its limit at {val:.2f} against {lim:.2f}. Attended and inspected, "
  "no mechanical defect found. Logged for trending.",
  "No fault found. Excursion attributed to a transient process upset. Continuing to monitor.")

CREW = ["j.mwangi","s.oakley","d.petrov","a.nkemelu","r.calder","t.whitlam"]

def dbsql(sql):
    r=subprocess.run(["databricks","experimental","aitools","tools","query",sql,"--profile",P],
                     capture_output=True,text=True)
    out=r.stdout+r.stderr
    if r.returncode!=0 or out.lstrip().startswith("Error"): raise SystemExit(out[:800])
    m=re.search(r'\[.*\]', out, re.S)
    return json.loads(m.group(0)) if m else []

def cli(*a):
    r=subprocess.run(["databricks",*a,"--profile",P,"-o","json"],capture_output=True,text=True)
    return json.loads(r.stdout)

S="jack_freeman_catalog.tech_summit_scada_build"
# tag -> measure from the governed register. Parsing the instrument code out of
# the tag id missed six instrument types, so ask the source of truth.
MEASURE={r["tag_id"]: r["measure"] for r in dbsql(f"SELECT tag_id, measure FROM {S}.dim_tag")}

# The four assets with genuine labelled bearing failures in the 120 day feature
# history. These are real events, so they belong in the maintenance history even
# though today's 24 hour window contains no vibration excursion.
HIST=dbsql(f"""
WITH f AS (
  SELECT asset_id, tag_id, window_start, rms_mm_s, crest_factor, kurtosis,
         label_failure_30d,
         MAX(CASE WHEN label_failure_30d THEN window_start END) OVER (PARTITION BY tag_id) fail_ts
  FROM {S}.vibration_features_seed
)
SELECT asset_id, tag_id, CAST(fail_ts AS STRING) failed_at,
       CAST(ROUND(MAX(rms_mm_s),2) AS STRING)     peak_rms,
       CAST(ROUND(MAX(crest_factor),2) AS STRING) peak_crest,
       CAST(ROUND(MAX(kurtosis),2) AS STRING)     peak_kurt
FROM f WHERE fail_ts IS NOT NULL AND window_start <= fail_ts
GROUP BY asset_id, tag_id, fail_ts ORDER BY asset_id""")
host=cli("postgres","get-endpoint",EP)["status"]["hosts"]["host"]
tok =cli("postgres","generate-database-credential",EP)["token"]
usr =cli("current-user","me")["userName"]

with psycopg.connect(host=host,user=usr,password=tok,dbname="databricks_postgres",
                     sslmode="require",autocommit=True) as c:
    # Idempotent: this script owns work_order and can be re-run. alert_outbox
    # belongs to 02_derive_alerts.py and is left alone.
    c.execute("TRUNCATE plant.work_order")
    alerts=c.execute("""
      SELECT a.alert_id, a.asset_id, a.tag_id, a.area_code, a.severity, a.raised_at,
             a.trigger_value, a.limit_high, a.limit_low
      FROM plant.alert_outbox a ORDER BY a.raised_at""").fetchall()
    # Raise a work order on every CRITICAL/HIGH alert and about half the warnings:
    # a control room does not raise paperwork for every transient warning.
    made=0
    for aid, asset, tag, area, sev, raised, tv, hi, lo in alerts:
        if sev == "WARNING" and random.random() > 0.55: continue
        measure = MEASURE.get(tag)
        title, desc, res = NARR.get(measure, FALLBACK)
        lim = hi if (tv is not None and hi is not None and tv >= hi) else (lo if lo is not None else hi or 0)
        fmt = dict(asset=asset, tag=tag, val=float(tv or 0), lim=float(lim or 0))
        closed = random.random() < 0.6
        status = "CLOSED" if closed else random.choice(["OPEN","IN_PROGRESS","ON_HOLD"])
        prio = {"CRITICAL":"CRITICAL","HIGH":"HIGH","WARNING":"MEDIUM"}[sev]
        c.execute("""
          INSERT INTO plant.work_order
            (work_order_id, alert_id, asset_id, tag_id, area_code, status, priority,
             failure_mode, title, description, resolution_notes, raised_by, assigned_to,
             created_at, updated_at, closed_at, downtime_minutes)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (f"WO-{made+1001}", aid, asset, tag, area, status, prio, measure,
              title.format(**fmt), desc.format(**fmt),
              res.format(**fmt) if closed else None,
              random.choice(CREW), random.choice(CREW), raised, raised,
              raised if closed else None,
              random.randint(20, 480) if closed else None))
        made+=1
    # Historical bearing failures. Real labelled events from the feature history,
    # closed long ago, carrying the vibration narrative that today's excursions
    # do not produce because no vibration tag breached in the last 24 hours.
    for h in HIST:
        title, desc, res = NARR["vibration"]
        fmt = dict(asset=h["asset_id"], tag=h["tag_id"],
                   val=float(h["peak_rms"]), lim=11.0)
        extra = (f" Trended crest factor to {float(h['peak_crest']):.2f} and kurtosis to "
                 f"{float(h['peak_kurt']):.2f}, both of which moved before overall RMS did.")
        c.execute("""
          INSERT INTO plant.work_order
            (work_order_id, alert_id, asset_id, tag_id, area_code, status, priority,
             failure_mode, title, description, resolution_notes, raised_by, assigned_to,
             created_at, updated_at, closed_at, downtime_minutes)
          VALUES (%s,NULL,%s,%s,%s,'CLOSED','HIGH','vibration',%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (f"WO-{made+1001}", h["asset_id"], h["tag_id"], h["asset_id"].split("-")[0],
              title.format(**fmt), desc.format(**fmt) + extra, res.format(**fmt),
              random.choice(CREW), random.choice(CREW),
              h["failed_at"], h["failed_at"], h["failed_at"], random.randint(240, 960)))
        made += 1

    print(f"raised {made} work orders ({len(alerts)} alerts + {len(HIST)} historical bearing failures)")
    for s,cnt in c.execute("SELECT status, count(*) FROM plant.work_order GROUP BY 1 ORDER BY 2 DESC").fetchall():
        print(f"  {s:12s} {cnt}")
    print("  failure modes:", dict(c.execute(
        "SELECT failure_mode, count(*) FROM plant.work_order GROUP BY 1 ORDER BY 2 DESC").fetchall()))
    print("  avg description length:", c.execute(
        "SELECT round(avg(length(description))) FROM plant.work_order").fetchone()[0], "chars")
