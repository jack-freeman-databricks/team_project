#!/usr/bin/env python3
"""Train the vibration predictive-maintenance model, and test it honestly.

The claim this model has to earn is specific: that it detects a developing
bearing defect EARLIER than an RMS alarm limit would. The data supports that
claim structurally (in phase 1, 30 to 12 days out, kurtosis rises ~48% and crest
factor ~27% while RMS moves only ~2%), so the model should beat an RMS threshold
in exactly that window and the evaluation is built to check it rather than to
flatter the model.

Two things make the evaluation trustworthy:

  1. LEAVE-ONE-ASSET-OUT. Only four assets ever fail, so a random row split would
     leak: adjacent 10-minute windows from the same bearing are nearly identical,
     and the model would be scored on rows it effectively trained on. Holding out
     a whole asset asks the real question, which is whether this generalises to a
     bearing it has never seen.
  2. AN RMS-THRESHOLD BASELINE, tuned in the model's favour. The baseline gets to
     pick its best threshold on the training fold. If the model cannot beat that,
     the model is not worth having and this script says so.
"""
import json, os, subprocess, sys
import numpy as np, pandas as pd

P = "ironbark"
S = "jack_freeman_catalog.tech_summit_scada_build"
WAREHOUSE = "/sql/1.0/warehouses/495cddfca8ecaa75"
HOST = "fevm-serverless-stable-1s8h43.cloud.databricks.com"

FEATURES = ["rms_mm_s","peak_mm_s","crest_factor","kurtosis","stddev_mm_s",
            "rms_delta_1h","rms_delta_24h","rms_slope_24h","rms_pct_of_baseline",
            "bearing_temp_c","motor_current_a","throughput_tph",
            "run_minutes","hours_since_maintenance"]
LABEL = "label_failure_30d"

def token():
    r = subprocess.run(["databricks","auth","token","--profile",P],
                       capture_output=True, text=True)
    return json.loads(r.stdout)["access_token"]

print("pulling features...")
from databricks import sql as dbsql
with dbsql.connect(server_hostname=HOST, http_path=WAREHOUSE, access_token=token()) as conn:
    df = pd.read_sql(f"""
        SELECT asset_id, tag_id, window_start, {', '.join(FEATURES)}, {LABEL}
        FROM {S}.vibration_features_seed ORDER BY asset_id, tag_id, window_start""", conn)
print(f"  {len(df):,} rows, {df[LABEL].sum():,} positives ({100*df[LABEL].mean():.1f}%), "
      f"{df.asset_id.nunique()} assets")

df["days_to_fail"] = np.nan
for tag, g in df.groupby("tag_id"):
    fail = g.loc[g[LABEL], "window_start"].max()
    if pd.notna(fail):
        df.loc[g.index, "days_to_fail"] = (fail - g["window_start"]).dt.total_seconds()/86400

X, y, grp = df[FEATURES].astype(float), df[LABEL].astype(int), df["asset_id"]
pos_assets = sorted(df.loc[df[LABEL], "asset_id"].unique())
print(f"  assets that fail: {', '.join(pos_assets)}")

from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import precision_score, recall_score, f1_score, average_precision_score

def rms_baseline(tr_X, tr_y, te_X):
    """Best-case RMS threshold, tuned on the training fold to be fair to it."""
    best_t, best_f1 = None, -1
    for t in np.quantile(tr_X["rms_mm_s"], np.linspace(0.50, 0.995, 60)):
        f1 = f1_score(tr_y, (tr_X["rms_mm_s"] >= t).astype(int), zero_division=0)
        if f1 > best_f1: best_f1, best_t = f1, t
    return (te_X["rms_mm_s"] >= best_t).astype(int), best_t

rows, early_rows = [], []
for held in pos_assets:
    tr, te = grp != held, grp == held
    w = (y[tr] == 0).sum() / max((y[tr] == 1).sum(), 1)      # class weighting
    m = HistGradientBoostingClassifier(max_iter=250, learning_rate=0.08,
                                       max_depth=6, random_state=27,
                                       class_weight={0: 1.0, 1: w})
    m.fit(X[tr], y[tr])
    pm = m.predict(X[te]); pp = m.predict_proba(X[te])[:, 1]
    pb, thr = rms_baseline(X[tr], y[tr], X[te])
    rows.append({"held_out_asset": held, "test_rows": int(te.sum()),
        "model_precision": round(precision_score(y[te], pm, zero_division=0), 3),
        "model_recall":    round(recall_score(y[te], pm, zero_division=0), 3),
        "model_f1":        round(f1_score(y[te], pm, zero_division=0), 3),
        "model_pr_auc":    round(average_precision_score(y[te], pp), 3),
        "rms_threshold_mm_s": round(float(thr), 2),
        "rms_precision":   round(precision_score(y[te], pb, zero_division=0), 3),
        "rms_recall":      round(recall_score(y[te], pb, zero_division=0), 3),
        "rms_f1":          round(f1_score(y[te], pb, zero_division=0), 3)})
    # The window that matters: phase 1, where RMS has barely moved.
    ph1 = te & df.days_to_fail.between(13, 30)
    if ph1.sum():
        early_rows.append({"held_out_asset": held, "phase1_rows": int(ph1.sum()),
            "model_recall_phase1": round(recall_score(y[ph1], m.predict(X[ph1]), zero_division=0), 3),
            "rms_recall_phase1":   round(recall_score(y[ph1], rms_baseline(X[tr], y[tr], X[ph1])[0], zero_division=0), 3)})

res = pd.DataFrame(rows); early = pd.DataFrame(early_rows)
print("\nleave-one-asset-out, model vs a best-case RMS threshold:")
print(res.to_string(index=False))
print("\nphase 1 only (30 to 13 days before failure, where RMS has barely moved):")
print(early.to_string(index=False))
print(f"\n  mean model F1 {res.model_f1.mean():.3f}  vs  RMS threshold F1 {res.rms_f1.mean():.3f}")
if len(early):
    print(f"  mean phase-1 recall: model {early.model_recall_phase1.mean():.3f}  "
          f"vs RMS {early.rms_recall_phase1.mean():.3f}")

res.to_json("lane_b/pdm_eval_loao.json", orient="records", indent=2)
early.to_json("lane_b/pdm_eval_phase1.json", orient="records", indent=2)
