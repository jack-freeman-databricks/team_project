#!/usr/bin/env python3
"""Deferred-maintenance forecast on a THROWAWAY Lakebase branch.

This is the second, distinct use of branching, and the one that justifies
copy-on-write existing. The scenario needs destructive writes: it rewrites every
open work order to model deferring it 30 days, then recomputes the cost. You
cannot do that on production, and you do not want to keep the result. So it runs
on forecast-deferred-maintenance, which carries a 4 hour TTL and expires itself.

The escalation factor is not invented: it is derived from the real closed work
orders in the data. Planned work that was closed promptly is compared against
work that ran longer, and the ratio is used to project what deferring an open job
costs when it runs to failure instead of being caught.
"""
import json, os, sys, decimal, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lb import connect

BRANCH = "forecast-deferred-maintenance"

def enc(o):
    if isinstance(o, decimal.Decimal): return float(o)
    if isinstance(o, (datetime.datetime, datetime.date)): return o.isoformat()
    return str(o)
def rows(cur):
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

main, main_host = connect("production")
fc,   fc_host   = connect(BRANCH)
out = {"branch": BRANCH, "purpose": "deferred maintenance what-if, destructive, discarded after use",
       "branch_host": fc_host, "main_host": main_host}

with main, fc:
    # ---- baseline, read from main, untouched -------------------------------
    out["baseline"] = rows(main.execute("""
        SELECT status, count(*) AS jobs,
               COALESCE(sum(downtime_minutes),0) AS downtime_minutes
        FROM plant.work_order GROUP BY status ORDER BY status"""))
    esc = main.execute("""
        WITH closed AS (
          SELECT downtime_minutes FROM plant.work_order
          WHERE status='CLOSED' AND downtime_minutes IS NOT NULL
        )
        SELECT ROUND(AVG(downtime_minutes)::numeric,1) AS mean_closed,
               ROUND(MAX(downtime_minutes)::numeric,1) AS worst_closed,
               ROUND((MAX(downtime_minutes)::numeric / NULLIF(AVG(downtime_minutes),0)),2) AS escalation
        FROM closed""").fetchone()
    out["escalation_model"] = {
        "mean_closed_downtime_minutes": float(esc[0]),
        "worst_closed_downtime_minutes": float(esc[1]),
        "escalation_factor": float(esc[2]),
        "derivation": "worst observed closed downtime divided by mean closed downtime, "
                      "used to project what a deferred job costs if it runs to failure",
    }
    ESC = float(esc[2])

    # ---- the destructive part, on the throwaway branch only ----------------
    before = fc.execute("SELECT count(*) FROM plant.work_order WHERE status <> 'CLOSED'").fetchone()[0]
    fc.execute("""
        UPDATE plant.work_order
        SET created_at = created_at + INTERVAL '30 days',
            updated_at = now(),
            status = 'OPEN',
            assigned_to = NULL,
            priority = CASE priority WHEN 'MEDIUM' THEN 'HIGH'
                                     WHEN 'HIGH'   THEN 'CRITICAL'
                                     ELSE priority END,
            downtime_minutes = CEIL(
              COALESCE(downtime_minutes,
                (SELECT AVG(downtime_minutes) FROM plant.work_order
                 WHERE status='CLOSED' AND downtime_minutes IS NOT NULL)) * %s)
        WHERE status <> 'CLOSED'""", (ESC,))
    fc.execute("""INSERT INTO plant.work_order_note (work_order_id, author, note_kind, note_text)
                  SELECT work_order_id, 'forecast', 'HANDOVER',
                         'FORECAST SCENARIO: deferred 30 days. Projected to run to failure rather '
                         'than be caught, so downtime escalated by the observed factor.'
                  FROM plant.work_order WHERE status='OPEN'""")
    out["scenario"] = {
        "open_jobs_deferred": before,
        "deferral_days": 30,
        "escalation_applied": ESC,
        "destructive_writes": "UPDATE on every non-closed work order plus one note each. "
                              "Only possible because this branch is disposable.",
    }

    out["forecast"] = rows(fc.execute("""
        SELECT status, count(*) AS jobs,
               COALESCE(sum(downtime_minutes),0) AS downtime_minutes
        FROM plant.work_order GROUP BY status ORDER BY status"""))
    out["forecast_by_asset"] = rows(fc.execute("""
        SELECT asset_id, failure_mode, priority,
               downtime_minutes AS projected_downtime_minutes,
               ROUND((downtime_minutes/60.0)::numeric,1) AS projected_downtime_hours
        FROM plant.work_order WHERE status='OPEN'
        ORDER BY downtime_minutes DESC"""))

    base_total = sum(int(r["downtime_minutes"]) for r in out["baseline"])
    fc_total   = sum(int(r["downtime_minutes"]) for r in out["forecast"])
    out["conclusion"] = {
        "baseline_total_downtime_minutes": base_total,
        "forecast_total_downtime_minutes": fc_total,
        "delta_minutes": fc_total - base_total,
        "delta_hours": round((fc_total - base_total)/60.0, 1),
        "answer": (f"Deferring all {before} open jobs by 30 days is projected to add "
                   f"{round((fc_total-base_total)/60.0,1)} hours of unplanned downtime, because "
                   f"caught-early work escalates by {ESC}x when it runs to failure instead."),
    }

    # ---- prove main was untouched by the destructive scenario --------------
    out["main_unchanged_after_scenario"] = rows(main.execute("""
        SELECT status, count(*) AS jobs,
               COALESCE(sum(downtime_minutes),0) AS downtime_minutes
        FROM plant.work_order GROUP BY status ORDER BY status"""))

json.dump(out, open("submission1/forecast_branch_result.json","w"), indent=2, default=enc)
print(f"forecast on throwaway branch {BRANCH}")
print(f"  deferred {out['scenario']['open_jobs_deferred']} open jobs by 30 days, "
      f"escalation {ESC}x")
print(f"  baseline downtime : {out['conclusion']['baseline_total_downtime_minutes']:>6d} min")
print(f"  forecast downtime : {out['conclusion']['forecast_total_downtime_minutes']:>6d} min")
print(f"  delta             : {out['conclusion']['delta_hours']:>6.1f} hours")
print(f"\n  main unchanged: {out['baseline'] == out['main_unchanged_after_scenario']}")
print("  -> submission1/forecast_branch_result.json")
