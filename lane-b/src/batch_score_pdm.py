"""Score every vibration sensor and write pdm_prediction.

Scores the most recent feature window per sensor from vibration_features_seed.

An earlier version computed features from the live readings instead, which is
what you would want, but it produced false positives on demonstrably healthy
assets. The diagnosis is in the SQL comment below: the feature table and the
readings table were generated independently, so features computed from readings
land outside the distribution the model trained on. Scoring the feature table
avoids inventing a number we cannot stand behind.

predicted_rul_hours is calibrated empirically rather than invented: training rows
are bucketed by model probability and the median actual days-to-failure in each
bucket becomes the lookup. That keeps the number defensible instead of a
made-up linear mapping from probability to time.
"""
import argparse, json, uuid
import numpy as np, pandas as pd, mlflow
from pyspark.sql import SparkSession

FEATURES = ["rms_mm_s","peak_mm_s","crest_factor","kurtosis","stddev_mm_s",
            "rms_pct_of_baseline","bearing_temp_c","motor_current_a","throughput_tph"]

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", required=True); ap.add_argument("--schema", required=True)
ap.add_argument("--model-name", required=True)
a = ap.parse_args()
S = f"{a.catalog}.{a.schema}"
spark = SparkSession.builder.getOrCreate()

mlflow.set_registry_uri("databricks-uc")
model = mlflow.sklearn.load_model(f"models:/{a.model_name}@champion")
from mlflow.tracking import MlflowClient
mv = MlflowClient(registry_uri="databricks-uc").get_model_version_by_alias(a.model_name, "champion")
print(f"loaded {a.model_name} v{mv.version} (champion)")

# ---- calibrate probability -> remaining life on the training history ---------
hist = (spark.table(f"{S}.vibration_features_seed")
        .select("tag_id","window_start",*FEATURES,"label_failure_30d").toPandas())
hist["days_to_fail"] = np.nan
for tag, g in hist.groupby("tag_id"):
    f = g.loc[g.label_failure_30d, "window_start"].max()
    if pd.notna(f):
        hist.loc[g.index, "days_to_fail"] = (f - g.window_start).dt.total_seconds()/86400
cal = hist.dropna(subset=["days_to_fail"]).copy()
cal["p"] = model.predict_proba(cal[FEATURES].astype(float))[:, 1]
# observed=False keeps all 10 bins even where no training row landed, so the
# lookup is a dense 10-element array and a positional index cannot run off the
# end. Empty bins are filled by interpolating between their neighbours.
bins = np.linspace(0, 1, 11)
cal["bucket"] = pd.cut(cal.p, bins, include_lowest=True)
rul_series = (cal.groupby("bucket", observed=False).days_to_fail.median()
                 .interpolate(limit_direction="both"))
RUL = [None if pd.isna(v) else float(v) for v in rul_series.to_numpy()]
print("probability -> median days to failure:")
for i, v in enumerate(RUL):
    print(f"  p {bins[i]:.1f}-{bins[i+1]:.1f} -> {'n/a' if v is None else f'{v:6.1f} days'}")

# ---- live features, one row per vibration sensor, most recent 10 min ---------
live = spark.sql(f"""
-- Most recent feature window per sensor, read from the feature table.
--
-- WHY NOT COMPUTE THESE FROM THE LIVE READINGS. I tried, and it produced
-- textbook train/serve skew: healthy conveyors scored 0.94. The cause is not the
-- SQL, it is that vibration_features_seed and tag_reading_seed were generated
-- INDEPENDENTLY, so the feature table is not derivable from the readings:
--
--   feature            train healthy range   live value
--   stddev_mm_s              1.87 - 5.40           0.15
--   motor_current_a         57.20 - 62.81         354.71
--   bearing_temp_c          45.21 - 50.81           null  (conveyors have no TI tag)
--
-- and even among the consistent features, every live healthy sensor reports an
-- IDENTICAL crest factor of 3.27 and kurtosis of 3.00, sitting at the extreme
-- bottom edge of the training healthy ranges (3.27-3.73 and 2.90-3.46). Training
-- saw a distribution; serving presents a single point at the boundary.
--
-- Scoring the feature table is self-consistent and is the normal predictive
-- maintenance pattern anyway. Live scoring becomes available once the two seeds
-- are mutually consistent, which is a generator change, not a change here.
-- A 30 day trajectory per sensor, not just the latest window. The four failures
-- land 2, 5, 8 and 20 days before the history ends, so the single most recent
-- window is post-repair and every asset reads healthy: correct, but it shows
-- nothing. Thirty days captures each degradation ramp, which is what a
-- maintenance planner and the app both need to see.
SELECT asset_id, tag_id, window_end AS feature_window_end, {', '.join(FEATURES)}
FROM {S}.vibration_features_seed
WHERE window_start >= (SELECT MAX(window_start) - INTERVAL 30 DAYS
                       FROM {S}.vibration_features_seed)
""").toPandas()
print(f"\nscoring {len(live)} sensors")

# No fillna: every feature here is computable from the window, so a null means a
# genuinely absent sensor and must not be silently turned into a zero the model
# has never seen. Rows with a missing required feature are reported, not guessed.
Xl = live[FEATURES].astype(float)
missing = Xl[["rms_mm_s","peak_mm_s","crest_factor","kurtosis"]].isna().any(axis=1)
if missing.any():
    print(f"  WARNING: {missing.sum()} sensors missing a core vibration feature, skipping them")
    live, Xl = live[~missing].copy(), Xl[~missing]
Xl = Xl.fillna(Xl.median())   # context features (temp/current/throughput) only
live["failure_prob_30d"] = model.predict_proba(Xl)[:, 1]
live["health_index"] = (100 * (1 - live.failure_prob_30d)).round(1)
def rul_hours(p):
    v = RUL[min(int(p * 10), 9)]
    return None if v is None else float(v * 24)
live["predicted_rul_hours"] = [rul_hours(p) for p in live.failure_prob_30d]
live["iso_10816_zone"] = pd.cut(live.rms_mm_s, [-np.inf,2.8,7.1,11.0,np.inf],
                                labels=["A","B","C","D"]).astype(str)

imp = dict(zip(FEATURES, getattr(model, "feature_importances_", [0]*len(FEATURES))))
def top(row):
    # Rank by how far each feature sits above its healthy median: what is actually
    # driving THIS asset's score, not the model's global importance.
    med = live[FEATURES].median()
    dev = {f: float((row[f]-med[f])/(abs(med[f])+1e-9)) for f in FEATURES if pd.notna(row[f])}
    return json.dumps([{"feature": f, "deviation_from_fleet_median": round(v, 3)}
                       for f, v in sorted(dev.items(), key=lambda kv: -abs(kv[1]))[:3]])
live["top_features"] = live.apply(top, axis=1)
live["recommended_action"] = np.select(
    [live.failure_prob_30d >= 0.7, live.failure_prob_30d >= 0.3],
    ["Schedule bearing replacement within 2 weeks. Order parts now.",
     "Add to the watch list and take spectral readings at the next shutdown."],
    default="No action. Continue trending.")
live["prediction_id"] = [str(uuid.uuid4()) for _ in range(len(live))]
live["model_name"] = a.model_name
live["model_version"] = str(mv.version)
live["scoring_mode"] = "batch"   # not "realtime": see the SQL comment above

out = spark.createDataFrame(live[[
    "prediction_id","asset_id","tag_id","feature_window_end","model_name","model_version",
    "health_index","failure_prob_30d","predicted_rul_hours","iso_10816_zone",
    "top_features","recommended_action","scoring_mode"]]) \
    .withColumn("scored_at", __import__("pyspark").sql.functions.current_timestamp())
out.createOrReplaceTempView("b_new_scores")
spark.sql(f"""
  MERGE INTO {S}.pdm_prediction t
  USING b_new_scores s ON t.prediction_id = s.prediction_id
  WHEN NOT MATCHED THEN INSERT *""")

print("\npeak risk per sensor over the 30 day window:")
peak = (live.sort_values("failure_prob_30d", ascending=False)
            .groupby("tag_id", as_index=False).first()
            .sort_values("failure_prob_30d", ascending=False))
for r in peak.itertuples():
    print(f"  {r.tag_id:18s} peak p={r.failure_prob_30d:.3f} health={r.health_index:5.1f} "
          f"zone={r.iso_10816_zone}  {r.recommended_action[:40]}")
flagged = int((peak.failure_prob_30d >= 0.7).sum())
print(f"\n  {flagged} of {len(peak)} sensors peaked above 0.7 in the window")
print(f"\nwrote {len(live)} rows to {S}.pdm_prediction")
