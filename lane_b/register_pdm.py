#!/usr/bin/env python3
"""Train the final PdM model, log it to MLflow, register it in Unity Catalog.

Trained on the 12 vibration and process features only, deliberately EXCLUDING
run_minutes and hours_since_maintenance. Two reasons, both verified rather than
assumed:
  * dropping them improves held-out performance (F1 0.983 -> 0.989, phase-1
    recall 0.942 -> 0.962)
  * it removes any question of the model reading a maintenance clock instead of
    reading the bearing. Permutation importance on a held-out asset puts
    crest_factor at +0.815 and hours_since_maintenance at +0.001, so it was not
    leaning on them anyway, but excluding them makes that unarguable.
"""
import json, subprocess, numpy as np, pandas as pd, mlflow
from mlflow.models import infer_signature
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import f1_score, recall_score, precision_score, average_precision_score

P="ironbark"; S="jack_freeman_catalog.tech_summit_scada_build"
MODEL=f"{S}.vibration_pdm"
FEATURES=["rms_mm_s","peak_mm_s","crest_factor","kurtosis","stddev_mm_s","rms_delta_1h",
          "rms_delta_24h","rms_slope_24h","rms_pct_of_baseline","bearing_temp_c",
          "motor_current_a","throughput_tph"]

tok=json.loads(subprocess.run(["databricks","auth","token","--profile",P],
    capture_output=True,text=True).stdout)["access_token"]
from databricks import sql as dbsql
with dbsql.connect(server_hostname="fevm-serverless-stable-1s8h43.cloud.databricks.com",
                   http_path="/sql/1.0/warehouses/495cddfca8ecaa75", access_token=tok) as c:
    df=pd.read_sql(f"SELECT asset_id, tag_id, window_start, {', '.join(FEATURES)}, "
                   f"label_failure_30d FROM {S}.vibration_features_seed", c)
df["days_to_fail"]=np.nan
for tag,g in df.groupby("tag_id"):
    f=g.loc[g.label_failure_30d,"window_start"].max()
    if pd.notna(f): df.loc[g.index,"days_to_fail"]=(f-g.window_start).dt.total_seconds()/86400

X=df[FEATURES].astype(float); y=df.label_failure_30d.astype(int)
w=(y==0).sum()/max((y==1).sum(),1)

mlflow.set_tracking_uri("databricks://ironbark")
mlflow.set_registry_uri("databricks-uc://ironbark")
mlflow.set_experiment(f"/Users/chris.dorrington@databricks.com/ironbark_vibration_pdm")

loao=json.load(open("lane_b/pdm_eval_loao.json"))
ph1=json.load(open("lane_b/pdm_eval_phase1.json"))

with mlflow.start_run(run_name="vibration_pdm_hgb_vibration_only") as run:
    model=HistGradientBoostingClassifier(max_iter=250, learning_rate=0.08, max_depth=6,
              random_state=27, class_weight={0:1.0,1:w}).fit(X, y)
    mlflow.log_params({"algo":"HistGradientBoostingClassifier","max_iter":250,
        "learning_rate":0.08,"max_depth":6,"class_weight_positive":round(float(w),3),
        "n_features":len(FEATURES),"excluded_features":"run_minutes,hours_since_maintenance",
        "validation":"leave-one-asset-out over the 4 assets that fail"})
    mlflow.log_metrics({
        "loao_mean_f1": float(np.mean([r["model_f1"] for r in loao])),
        "loao_mean_pr_auc": float(np.mean([r["model_pr_auc"] for r in loao])),
        "rms_baseline_mean_f1": float(np.mean([r["rms_f1"] for r in loao])),
        "phase1_recall_model": float(np.mean([r["model_recall_phase1"] for r in ph1])),
        "phase1_recall_rms_baseline": float(np.mean([r["rms_recall_phase1"] for r in ph1])),
        "train_positive_rate": float(y.mean())})
    mlflow.log_dict({"leave_one_asset_out": loao, "phase1_early_detection": ph1,
        "interpretation":
          "The model's value is early detection. In phase 1 (30 to 13 days before "
          "failure) it recalls 95.3% of degrading windows while a best-case RMS "
          "threshold recalls 14.8%, and for 3 of the 4 assets the RMS threshold "
          "recalls exactly zero. That is the window where kurtosis and crest factor "
          "have moved but overall vibration energy has not."},
        "evaluation.json")
    mlflow.sklearn.log_model(model, name="model",
        signature=infer_signature(X.head(100), model.predict_proba(X.head(100))[:,1]),
        input_example=X.head(3), registered_model_name=MODEL)
    print(f"run_id={run.info.run_id}")

from mlflow.tracking import MlflowClient
cl=MlflowClient(registry_uri="databricks-uc://ironbark")
v=max(int(m.version) for m in cl.search_model_versions(f"name='{MODEL}'"))
cl.set_registered_model_alias(MODEL, "champion", v)
print(f"registered {MODEL} version {v}, alias champion")
open("lane_b/pdm_model_version.txt","w").write(f"{MODEL}\nversion={v}\nalias=champion\n")
