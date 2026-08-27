# Databricks notebook source
# MAGIC %md
# MAGIC # Lane A — Step 1: generate the two seed tables (physics simulation)
# MAGIC
# MAGIC Populates, in `jack_freeman_catalog.tech_summit_scada_build`:
# MAGIC
# MAGIC | Table | Horizon | Grain | Serves |
# MAGIC |---|---|---|---|
# MAGIC | `tag_reading_seed` | 24 h, ending at generation time | 1 Hz (10 Hz vibration) | lanes B & C: mass balance, flowsheet, app |
# MAGIC | `vibration_features_seed` | 120 d, contiguous with the 24 h window | 10 min | lane B: PdM model training |
# MAGIC
# MAGIC The register (`dim_tag`) and the flowsheet (`dim_flowsheet_node` / `dim_flowsheet_arc`)
# MAGIC drive everything: adding a tag adds it to the output with no code change.
# MAGIC
# MAGIC **The mass balance actually closes.** A single time-varying plant load factor `L(t)`
# MAGIC scales every arc, so every unit node closes by construction (linear scaling preserves
# MAGIC `in = out`). The tertiary-crusher recirculating load (250 t/h, A10) is injected as the
# MAGIC known design constant. The surge bin N04 closes exactly — its outflow is derived as
# MAGIC `inflow − accumulation`, where accumulation is `d(level)/dt × capacity`. The desands
# MAGIC split uses the contract `Cw` formula (`SOLIDS_SG = 4.9`): densities are held at nominal
# MAGIC and volume flows back-calculated from target solids, so lane B recovers 2100 t/h
# MAGIC underflow / 400 t/h overflow exactly when it applies the same formula.
# MAGIC
# MAGIC Physics (small, stateful) is simulated on the driver in numpy; bulk stateless noise
# MAGIC (the independent tags, vibration at 10 Hz) is generated distributed in Spark.
# MAGIC
# MAGIC **Runtime:** needs a cluster with Spark (classic or serverless), not a SQL warehouse.
# MAGIC Re-runnable: each write is `INSERT OVERWRITE`, so it refills the seed tables without
# MAGIC ever `CREATE OR REPLACE`-ing them (the tables are owned by the contract).

# COMMAND ----------

# MAGIC %md ## Config

# COMMAND ----------

import datetime as dt
import numpy as np
import pandas as pd
from pyspark.sql import functions as F, Window

CATALOG = "jack_freeman_catalog"
SCHEMA  = "tech_summit_scada_build"
FQ      = f"{CATALOG}.{SCHEMA}"

# Physical constant from the contract. Both lanes MUST use the same value.
SOLIDS_SG = 4.9

# Horizons. The 24 h reading window ends at generation time; the 120 d vibration history
# ends where the 24 h window begins, so the two are contiguous.
SEED_HOURS = 24
PDM_DAYS   = 120
FEATURE_GRAIN_MIN = 10

# Reported vibration sample rate, used for the features `sample_count`. NOTE: the
# tag_reading_seed row count is driven by each tag's real dim_tag.sample_hz (10 Hz), not by
# this constant — there are now 45 vibration tags (VI+VP+VK), so the 10 Hz reading stream is
# ~39M rows. To lighten the reading seed, downsample by sample_hz in build_group; the mass
# balance (1-min) and the PdM model (features seed) are unaffected either way.
VIBRATION_HZ = 10.0

PRODUCER = "gen-lane-a-seed"
SEED_RNG = 20260827          # deterministic run

# Anchor the windows (truncate to the second for clean timestamps).
NOW           = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
READING_END   = NOW
READING_START = READING_END - dt.timedelta(hours=SEED_HOURS)
PDM_END       = READING_START                     # contiguous
PDM_START     = PDM_END - dt.timedelta(days=PDM_DAYS)
READING_START_EPOCH = READING_START.timestamp()
PDM_START_EPOCH     = PDM_START.timestamp()

print(f"Reading window : {READING_START}  ->  {READING_END}")
print(f"PDM window     : {PDM_START}  ->  {PDM_END}")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md ## Load the register and flowsheet (drive everything)

# COMMAND ----------

pt   = spark.table(f"{FQ}.dim_tag").toPandas()               # register-driven (151 tags)
arcs = spark.table(f"{FQ}.dim_flowsheet_arc").toPandas()     # 15 arcs
nodes = spark.table(f"{FQ}.dim_flowsheet_node").toPandas()   # 13 nodes
assert len(pt) >= 100, f"dim_tag looks wrong: {len(pt)} rows"
print(f"{len(pt)} tags, {len(arcs)} arcs, {len(nodes)} nodes")

hz_groups = sorted(pt["sample_hz"].unique().tolist())
print("sample_hz groups:", hz_groups)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Physics simulation (driver, numpy)
# MAGIC
# MAGIC Per-second series for the 24 h window: the global load factor, the derived desands
# MAGIC slurry flows, and the three integrated bin levels. Everything correlated is computed
# MAGIC here so mass-flow, motor-current, slurry and level tags stay physically consistent.

# COMMAND ----------

N    = SEED_HOURS * 3600
dt_h = 1.0 / 3600.0
t    = np.arange(N)
rs   = np.random.default_rng(SEED_RNG)

def smooth_noise(scale, win=180):
    """Autocorrelated noise via a moving average of white noise."""
    w = rs.standard_normal(N + win)
    return scale * np.convolve(w, np.ones(win) / win, mode="valid")[:N]

def mean_revert(nom, amp, period_h, lo, hi):
    x = nom + amp * np.sin(2 * np.pi * t / (period_h * 3600)) + smooth_noise(amp * 0.2, 240)
    return np.clip(x, lo, hi)

# --- Global plant load factor: shift wave + drift, with one slowdown event to ~0.86.
L = 1.0 + 0.02 * np.sin(2 * np.pi * t / (8 * 3600)) + smooth_noise(0.01, 300)
c, half = int(0.60 * N), 1200
seg = slice(c - half, c + half)
dip = np.zeros(N)
dip[seg] = -0.14 * np.exp(-(((t[seg] - c) / (half / 2.0)) ** 2))
L = np.clip(L + dip, 0.80, 1.05)

# --- Desands slurry: hold density at nominal, back-calculate volume flow from target solids
# so solids = Q * rho * Cw reconstructs exactly (contract Cw formula, SOLIDS_SG = 4.9).
def Cw(rho):
    return SOLIDS_SG * (rho - 1.0) / (rho * (SOLIDS_SG - 1.0))

rho_of   = mean_revert(1.18,  0.03, 4, 1.09, 1.30)     # cyclone overflow (dilute)
rho_uf   = mean_revert(1.85,  0.04, 5, 1.63, 2.00)     # cyclone underflow (thick)
rho_feed = mean_revert(1.533, 0.03, 6, 1.30, 1.70)     # cyclone feed
rho_thk  = mean_revert(1.42,  0.03, 7, 1.22, 1.63)     # thickener
q_of    = (400.0  * L) / (rho_of * Cw(rho_of))         # DSD-CYC01-FI02 (overflow)
q_uf    = (2100.0 * L) / (rho_uf * Cw(rho_uf))         # DSD-CYC01-FI03 (underflow)
q_feed  = q_of + q_uf                                  # DSD-CYC01-FI01 (feed = of + uf)
q_water = 3225.0 * np.clip(0.98 + smooth_noise(0.01, 240), 0.90, 1.05)  # UTL-WTR01-FI01

def moving_average(x, win):
    # Boundary-safe centred MA. A plain np.convolve(mode="same") zero-pads the ends, which
    # collapses the average in the first/last window and would crash A04 there.
    return pd.Series(x).rolling(window=win, center=True, min_periods=1).mean().to_numpy()

# --- Bin levels (%). Source/sink bins (N01, N12) are not required to close, so they just
# mean-revert. The surge bin N04 IS a unit node and must close, and it is a buffer: it
# smooths the reclaim feed while its LEVEL absorbs the in/out difference.
lvl_rom = mean_revert(68, 6, 5, 22, 90)                # ROM-BIN01-LI01 (source bin)
lvl_thk = mean_revert(62, 7, 6, 26, 86)                # DSD-THK01-LI01 (thickener, sink)

# --- N04 surge bin. FIX 2: a real weightometer reads to ~0.5-1%, so A04 (the reclaim belt)
# must not carry the high-frequency garbage that differentiating a noisy level produced
# before (lane B saw sd 84 t/h = 3.4%). A surge bin's whole job is to BUFFER: the reclaim
# A04 tracks slowly-smoothed demand (low noise), the feed A03 tracks instantaneous load,
# and the bin LEVEL integrates the difference. N04 then closes exactly by construction:
# accumulation = A03 - A04 = d(level)/dt * capacity. This keeps lane B's validated
# reconciliation (accumulation shows up during load transients) while A04 sits at ~0.6%.
demand = moving_average(L, 2 * 3600)                                    # bin buffers ~2h of load swing
a01 = 2500.0 * L * (1 + smooth_noise(0.006, 120))                      # ROM-APF01 out of ROM bin (~0.6%)
a03 = 2500.0 * L * (1 + smooth_noise(0.006, 120))                      # CV-CV001 into N04 (~0.6%, tracks load)
a04 = 2500.0 * demand * (1 + smooth_noise(0.006, 120))                 # CV-CV002 reclaim: buffered, ~0.6%
net_sgb = a03 - a04                                                     # what the surge bin accumulates (tph)
lvl_sgb_true = np.clip(55 + np.cumsum(net_sgb) * dt_h / 850.0 * 100.0, 18, 87)
lvl_sgb = np.clip(lvl_sgb_true + smooth_noise(0.4, 60), 17, 88)        # CRU-SGB01-LI01 reported (+ meas noise)

# --- N05 scalping screen. FIX 1: the outputs must FOLLOW the actual A04 rate on the design
# split (900/1600 = 0.36/0.64), not track a steady design value, or the node cannot close
# when A04 moves. Each output gets its own small measurement error.
a05 = a04 * 0.36 * (1 + smooth_noise(0.006, 120))                      # CV-CV003 oversize (N05->N06)
a06 = a04 * 0.64 * (1 + smooth_noise(0.006, 120))                      # CV-CV004 undersize (N05->N09)

sim_pdf = pd.DataFrame({
    "t_ix": t, "load_factor": L,
    "a01": a01, "a03": a03, "a04": a04, "a05": a05, "a06": a06,
    "q_of": q_of, "q_uf": q_uf, "q_feed": q_feed, "q_water": q_water,
    "rho_of": rho_of, "rho_uf": rho_uf, "rho_feed": rho_feed, "rho_thk": rho_thk,
    "lvl_rom": lvl_rom, "lvl_sgb": lvl_sgb, "lvl_thk": lvl_thk,
})
sim_sdf = F.broadcast(spark.createDataFrame(sim_pdf))
print(f"sim series built: L in [{L.min():.3f},{L.max():.3f}], "
      f"underflow solids ~ {np.mean(q_uf*rho_uf*Cw(rho_uf)):.0f} t/h, "
      f"overflow solids ~ {np.mean(q_of*rho_of*Cw(rho_of)):.0f} t/h")

# COMMAND ----------

# MAGIC %md ### Map each correlated tag to its physics source

# COMMAND ----------

# arc measure_tag_id -> design tonnage, for the dry-solids weightometers that just scale with L
arc_by_tag = arcs.dropna(subset=["measure_tag_id"]).set_index("measure_tag_id")

# series tags read a precomputed sim column directly (noise already baked in)
SERIES = {
    "ROM-APF01-WI01": "a01", "CV-CV001-WI01": "a03", "CV-CV002-WI01": "a04",
    "CV-CV003-WI01": "a05", "CV-CV004-WI01": "a06",     # FIX 1: N05 outputs follow A04
    "DSD-CYC01-FI02": "q_of", "DSD-CYC01-FI03": "q_uf",
    "DSD-CYC01-FI01": "q_feed", "UTL-WTR01-FI01": "q_water",
    "DSD-CYC01-DI02": "rho_of", "DSD-CYC01-DI03": "rho_uf",
    "DSD-CYC01-DI01": "rho_feed", "DSD-THK01-DI01": "rho_thk",
    "ROM-BIN01-LI01": "lvl_rom", "CRU-SGB01-LI01": "lvl_sgb", "DSD-THK01-LI01": "lvl_thk",
}

def classify(row):
    """Return (sim_kind, design_val, sim_col) for a tag."""
    tid = row.tag_id
    if tid in SERIES:
        return ("series", float("nan"), SERIES[tid])
    if row.measure == "mass_flow":                       # weightometer scaling with L
        design = float(arc_by_tag.loc[tid, "design_tph"]) if tid in arc_by_tag.index else float(row.nominal)
        return ("scale_flow", design, None)
    if row.measure == "motor_current":                   # tracks throughput via L
        return ("scale_motor", float(row.nominal), None)
    if row.measure == "belt_speed":                      # near-constant
        return ("belt", float(row.nominal), None)
    return ("indep", float("nan"), None)                 # bounded noise around nominal

kinds = pt.apply(classify, axis=1, result_type="expand")
pt["sim_kind"], pt["design_val"], pt["sim_col"] = kinds[0], kinds[1], kinds[2]
print(pt.groupby("sim_kind").size().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deterministic schedules: excursions, one STALE tag, one INSUFFICIENT_DATA node

# COMMAND ----------

import random
rng = random.Random(SEED_RNG)
SEC = SEED_HOURS * 3600
analog = pt[pt.value_class == "analog"].copy()
exc_pool = analog[(analog.sample_hz == 1.0) & analog.hi.notna()].copy()

excursions, used_tags = [], set()

def add_excursion(row, kind, sustained_final=False):
    hi = row.hi
    hihi = row.hi_hi if pd.notna(row.hi_hi) else hi * 1.15
    target = hi + 0.5 * (hihi - hi) if kind == "warning" else hihi + 0.30 * (hihi - hi)
    dur = rng.randint(30, 240)
    if sustained_final:
        t_end = SEC - rng.randint(60, 300)
        t_start = t_end - max(dur, 900)
    else:
        t_start = rng.randint(0, SEC - dur - 1)
        t_end = t_start + dur
    excursions.append(dict(tag_id=row.tag_id, t_start=int(t_start), t_end=int(t_end),
                           target=float(target), kind=kind))
    used_tags.add(row.tag_id)

# Dedicated trips the contract calls out.
press = analog[(analog.measure == "pressure") & (analog.area_code == "CRU") & (analog.sample_hz == 1.0)]
if len(press):
    add_excursion(press.iloc[0], "trip")                 # crusher pressure choke (R07)
level = analog[(analog.measure == "level") & (analog.sample_hz == 1.0)]
if len(level):
    add_excursion(level.iloc[0], "trip")                 # bin high level (R08)

while len([e for e in excursions if e["kind"] == "trip"]) < rng.randint(4, 6):
    add_excursion(exc_pool.sample(1, random_state=rng.randint(0, 1 << 30)).iloc[0], "trip")

for i in range(rng.randint(15, 25)):
    if len(used_tags) < 8:
        cand = exc_pool[~exc_pool.tag_id.isin(used_tags)]
        r = (cand if len(cand) else exc_pool).sample(1, random_state=rng.randint(0, 1 << 30)).iloc[0]
    else:
        r = exc_pool.sample(1, random_state=rng.randint(0, 1 << 30)).iloc[0]
    add_excursion(r, "warning", sustained_final=(i == 0))

n_warn = len([e for e in excursions if e["kind"] == "warning"])
n_trip = len([e for e in excursions if e["kind"] == "trip"])
print(f"excursions: {n_warn} warning, {n_trip} trip, across {len(used_tags)} distinct tags")

# One tag STALE for several minutes (fires R05); freeze value.
stale_row = analog[analog.measure == "mass_flow"].iloc[rng.randrange(len(analog[analog.measure == "mass_flow"]))]
ss = rng.randint(0, SEC - 600)
STALE = dict(tag_id=stale_row.tag_id, t_start=ss, t_end=ss + rng.randint(300, 540),
             value=float(stale_row.nominal))

# One node dropped to INSUFFICIENT_DATA: force BAD on its measured arc tags for a window.
node_arcs = arcs[arcs.measure_tag_id.notna()].groupby("to_node_id")["measure_tag_id"].apply(list)
insuff_node = node_arcs[node_arcs.apply(len) >= 1].index[0]
bs = rng.randint(0, SEC - 600)
INSUFF = dict(node_id=insuff_node, tags=[str(x) for x in node_arcs.loc[insuff_node]],
              t_start=bs, t_end=bs + 400)
print("stale tag:", STALE["tag_id"], "| INSUFFICIENT_DATA node:", insuff_node, INSUFF["tags"])

# Discrete tags must hold state, not flicker per sample. Build a contiguous, non-overlapping
# timeline per tag so each second maps to exactly one state (long dwell -> realistic ZI tags).
# A run-status FAULT is correlated with a trip excursion on the same asset where one exists.
trip_by_asset = {}
for e in excursions:
    if e["kind"] == "trip":
        aid = pt.loc[pt.tag_id == e["tag_id"], "asset_id"].iloc[0]
        trip_by_asset.setdefault(aid, []).append((e["t_start"], e["t_end"]))

disc_segments = []   # (tag_id, seg_start, seg_end, state)
for _, drow in pt[pt.value_class == "discrete"].iterrows():
    tag, asset = drow.tag_id, drow.asset_id
    states = str(drow.enum_values).split("|")
    if tag.endswith("ZI02"):                              # stacking: 2-6 h campaigns, IDLE between
        piles = [s for s in states if s != "IDLE"] or states
        t, pi = 0, rng.randrange(len(piles))
        while t < SEC:
            e = min(SEC, t + rng.randint(2 * 3600, 6 * 3600))
            disc_segments.append((tag, t, e - 1, piles[pi % len(piles)])); t, pi = e, pi + 1
            if t < SEC:
                gp = min(SEC, t + rng.randint(600, 2400))
                disc_segments.append((tag, t, gp - 1, "IDLE")); t = gp
    else:                                                 # run status: RUNNING w/ a few stops/faults
        events = []
        for _ in range(rng.randint(2, 4)):               # STOPPED 10-60 min
            s = rng.randint(0, SEC - 3600); events.append((s, s + rng.randint(600, 3600), "STOPPED"))
        for (ts, te) in trip_by_asset.get(asset, []):    # FAULT correlated with a trip on this asset
            events.append((max(0, ts - 30), min(SEC, te + 60), "FAULT"))
        if not trip_by_asset.get(asset) and rng.random() < 0.15:   # else a rare standalone FAULT
            s = rng.randint(0, SEC - 600); events.append((s, s + rng.randint(120, 600), "FAULT"))
        t = 0
        for (s, e, st) in sorted(events):
            if s < t:                                    # drop overlaps to keep the timeline clean
                continue
            if s > t:
                disc_segments.append((tag, t, s - 1, "RUNNING"))
            disc_segments.append((tag, s, min(e, SEC) - 1, st)); t = min(e, SEC)
        if t < SEC:
            disc_segments.append((tag, t, SEC - 1, "RUNNING"))

disc_seg_sdf = F.broadcast(spark.createDataFrame(
    [dict(seg_tag=a, seg_start=b, seg_end=c, seg_state=d) for (a, b, c, d) in disc_segments]))
print(f"discrete segments: {len(disc_segments)} across {int(pt.value_class.eq('discrete').sum())} tags")

exc_sdf   = F.broadcast(spark.createDataFrame(excursions)) if excursions else None
stale_sdf = F.broadcast(spark.createDataFrame([STALE]))
insuff_sdf = F.broadcast(spark.createDataFrame(
    [dict(tag_id=x, t_start=INSUFF["t_start"], t_end=INSUFF["t_end"]) for x in INSUFF["tags"]]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## tag_reading_seed
# MAGIC
# MAGIC Per sampling rate, cross-join the tags with a step grid, join the physics series (1 Hz),
# MAGIC compute the value, then overlay the excursions / stale / bad-quality windows.

# COMMAND ----------

META_COLS = ["tag_id", "asset_id", "area_code", "measure", "value_class", "unit",
             "nominal", "lo", "lo_lo", "hi", "hi_hi", "enum_values",
             "sim_kind", "design_val", "sim_col"]

def build_group(hz):
    g = pt[pt.sample_hz == hz]
    n_steps = int(round(SEED_HOURS * 3600 * hz))
    if n_steps == 0 or len(g) == 0:
        return None
    meta = F.broadcast(spark.createDataFrame(g[META_COLS]))
    grid = spark.range(0, n_steps).withColumnRenamed("id", "t_ix")
    df = meta.crossJoin(grid).withColumn("z", F.randn(SEED_RNG)).withColumn("u", F.rand(SEED_RNG + 1))

    # 1 Hz group joins the physics series on the second index.
    if hz == 1.0:
        df = df.join(sim_sdf, "t_ix", "left").fillna({"load_factor": 1.0})
    else:
        df = df.withColumn("load_factor", F.lit(1.0))

    z = "greatest(-2.5, least(2.5, z))"
    series_case = """CASE sim_col
        WHEN 'a01' THEN a01 WHEN 'a03' THEN a03 WHEN 'a04' THEN a04
        WHEN 'a05' THEN a05 WHEN 'a06' THEN a06
        WHEN 'q_of' THEN q_of WHEN 'q_uf' THEN q_uf WHEN 'q_feed' THEN q_feed WHEN 'q_water' THEN q_water
        WHEN 'rho_of' THEN rho_of WHEN 'rho_uf' THEN rho_uf WHEN 'rho_feed' THEN rho_feed WHEN 'rho_thk' THEN rho_thk
        WHEN 'lvl_rom' THEN lvl_rom WHEN 'lvl_sgb' THEN lvl_sgb WHEN 'lvl_thk' THEN lvl_thk END""" \
        if hz == 1.0 else "CAST(NULL AS DOUBLE)"

    value_expr = f"""
      CASE
        WHEN value_class = 'discrete'    THEN NULL
        WHEN sim_kind = 'series'         THEN {series_case}
        WHEN sim_kind = 'scale_flow'     THEN design_val * load_factor * (1 + 0.008*{z})
        WHEN sim_kind = 'scale_motor'    THEN nominal * (0.40 + 0.60*load_factor) * (1 + 0.02*{z})
        WHEN sim_kind = 'belt'           THEN nominal * (1 + 0.006*{z})
        -- Live degradation: SCR-SCN01 & CRU-SCC01 held mid-phase-1 in the 24h window so the
        -- live PdM path has something to score (VK~5.0, VP/VI crest~4.6, VI a few % over nominal).
        WHEN asset_id IN ('SCR-SCN01','CRU-SCC01') AND measure='vibration'         THEN nominal * 1.04 * (1 + 0.02*{z})
        WHEN asset_id IN ('SCR-SCN01','CRU-SCC01') AND measure='vibration_peak'     THEN nominal * 1.367 * (1 + 0.02*{z})
        WHEN asset_id IN ('SCR-SCN01','CRU-SCC01') AND measure='vibration_kurtosis' THEN 5.0 + 0.15*{z}
        WHEN measure  = 'vibration'      THEN nominal * (1 + 0.09*abs({z}))
        ELSE nominal + coalesce((hi - lo)/6.0, abs(nominal)*0.03 + 0.05) * {z}
      END"""
    bounded = """CASE WHEN value_class='discrete' THEN NULL
                      WHEN lo IS NOT NULL AND hi IS NOT NULL THEN greatest(lo, least(hi, _v))
                      ELSE _v END"""
    df = df.withColumn("_v", F.expr(value_expr)).withColumn("value", F.expr(bounded))

    # Discrete value_text comes from the persistent-state timeline (one state per second),
    # not a per-row draw. All discrete tags are 1 Hz, so only that group joins the segments.
    if hz == 1.0:
        df = df.join(disc_seg_sdf,
                     (F.col("tag_id") == F.col("seg_tag")) &
                     (F.col("t_ix") >= F.col("seg_start")) & (F.col("t_ix") <= F.col("seg_end")),
                     "left")
        df = df.withColumn("value_text", F.expr(
            "CASE WHEN value_class='discrete' THEN seg_state ELSE NULL END"))
    else:
        df = df.withColumn("value_text", F.expr("CAST(NULL AS STRING)"))

    df = df.withColumn("source_ts", F.expr(
        f"timestamp_micros(cast(({READING_START_EPOCH} + t_ix/{hz})*1e6 as long))"))
    return df.select("tag_id", "unit", "t_ix", "source_ts", "value", "value_text",
                     F.lit(hz).alias("hz")).withColumn("quality_raw", F.lit("GOOD"))

reading = None
for hz in hz_groups:
    grp = build_group(hz)
    reading = grp if reading is None else reading.unionByName(grp)

# COMMAND ----------

# ---- Weighted quality (~99.5% GOOD), then overlays.
r = reading.withColumn(
    "quality_raw",
    F.expr("CASE WHEN rand() < 0.995 THEN 'GOOD' "
           "WHEN rand() < 0.5 THEN 'BAD' WHEN rand() < 0.5 THEN 'UNCERTAIN' ELSE 'STALE' END"))

# Excursions: triangular ramp in/out.
if exc_sdf is not None:
    r = (r.join(exc_sdf, "tag_id", "left")
           .withColumn("_in", F.expr("t_start IS NOT NULL AND t_ix BETWEEN t_start AND t_end"))
           .withColumn("_ramp", F.expr("CASE WHEN _in THEN greatest(0.0, "
                       "1 - abs((t_ix-(t_start+t_end)/2.0)/((t_end-t_start)/2.0))) ELSE 0.0 END"))
           .withColumn("value", F.expr("CASE WHEN _in THEN value + (target-value)*_ramp ELSE value END"))
           .drop("t_start", "t_end", "target", "kind", "_in", "_ramp"))

# STALE window: freeze value, mark STALE.
st = stale_sdf.select(F.col("tag_id").alias("_t"), F.col("t_start").alias("_s"),
                      F.col("t_end").alias("_e"), F.col("value").alias("_v"))
r = (r.join(st, r.tag_id == st._t, "left")
       .withColumn("_stale", F.expr("_t IS NOT NULL AND t_ix BETWEEN _s AND _e"))
       .withColumn("value", F.expr("CASE WHEN _stale THEN _v ELSE value END"))
       .withColumn("quality_raw", F.expr("CASE WHEN _stale THEN 'STALE' ELSE quality_raw END"))
       .drop("_t", "_s", "_e", "_v", "_stale"))

# INSUFFICIENT_DATA node: force BAD on its measured tags for the window.
bd = insuff_sdf.select(F.col("tag_id").alias("_t"), F.col("t_start").alias("_s"), F.col("t_end").alias("_e"))
r = (r.join(bd, r.tag_id == bd._t, "left")
       .withColumn("_bad", F.expr("_t IS NOT NULL AND t_ix BETWEEN _s AND _e"))
       .withColumn("quality_raw", F.expr("CASE WHEN _bad THEN 'BAD' ELSE quality_raw END"))
       .drop("_t", "_s", "_e", "_bad"))

# COMMAND ----------

reading_final = r.selectExpr(
    "uuid() as event_id", "tag_id", "source_ts",
    "cast(value as double) as value", "value_text",
    "quality_raw as quality", "unit",
    "cast(t_ix as bigint) as seq", f"'{PRODUCER}' as producer",
    "timestampadd(MILLISECOND, cast(rand()*400 as int), source_ts) as ingest_ts")

reading_final.createOrReplaceTempView("_reading_final")
spark.sql(f"""
  INSERT OVERWRITE TABLE {FQ}.tag_reading_seed
  SELECT event_id, tag_id, source_ts, value, value_text, quality, unit, seq, producer, ingest_ts
  FROM _reading_final
""")
print("tag_reading_seed written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## vibration_features_seed
# MAGIC
# MAGIC 120 d of 10-min windows per vibration tag. Healthy tags stay near baseline; the four
# MAGIC injected faults ramp RMS exponentially toward failure with **crest factor and kurtosis
# MAGIC leading RMS**, bearing temperature and motor current rising alongside. `label_failure_30d`
# MAGIC marks windows within 30 days of a failure point.

# COMMAND ----------

# Fault spec from contract/naming.md. Failing sensor = the vibration tag whose nominal
# matches the ramp start value.
FAULTS = [
    dict(asset="SCR-SCN01", rms_start=7.2, rms_end=16.0, ramp_days=21, day_offset=54),
    dict(asset="SCR-SCN01", rms_start=7.2, rms_end=16.0, ramp_days=21, day_offset=115),  # 2 events
    dict(asset="CRU-SCC01", rms_start=3.8, rms_end=12.0, ramp_days=30, day_offset=112),
    dict(asset="CV-CV009",  rms_start=2.8, rms_end=7.0,  ramp_days=14, day_offset=118),
    dict(asset="DSD-DWS01", rms_start=6.4, rms_end=14.0, ramp_days=18, day_offset=100),
]
# One feature row per VI sensor. VP (peak) and VK (raw kurtosis) are now their own tags in
# the register (fed into tag_reading_seed automatically), so filter to VI here — not the
# broader 'vibration' measure family — to avoid emitting feature rows for VP/VK.
vib = pt[pt.instrument_type == "VI"].copy()

def failing_tag(asset, rms_start):
    cand = vib[vib.asset_id == asset]
    return cand.iloc[(cand.nominal - rms_start).abs().argmin()].tag_id

for f in FAULTS:
    f["tag_id"] = failing_tag(f["asset"], f["rms_start"])
    f["fail_epoch"] = (PDM_START + dt.timedelta(days=f["day_offset"])).timestamp()
faults_sdf = F.broadcast(spark.createDataFrame(FAULTS))
print("fault sensors:", [(f["asset"], f["tag_id"], f["day_offset"]) for f in FAULTS])

# COMMAND ----------

n_win = int(PDM_DAYS * 24 * 60 / FEATURE_GRAIN_MIN)
GRAIN_SEC = FEATURE_GRAIN_MIN * 60
vmeta = F.broadcast(spark.createDataFrame(vib[["tag_id", "asset_id", "nominal", "hi", "hi_hi"]]))
grid = spark.range(0, n_win).withColumnRenamed("id", "w_ix")

vf = (vmeta.crossJoin(grid)
      .withColumn("zr", F.randn(SEED_RNG + 2)).withColumn("zt", F.randn(SEED_RNG + 3))
      .withColumn("down", F.rand(SEED_RNG + 4))
      .withColumn("win_epoch", F.expr(f"{PDM_START_EPOCH} + w_ix*{GRAIN_SEC}"))
      .withColumn("window_start", F.expr(f"timestamp_micros(cast(({PDM_START_EPOCH}+w_ix*{GRAIN_SEC})*1e6 as long))"))
      .withColumn("window_end",   F.expr(f"timestamp_micros(cast(({PDM_START_EPOCH}+(w_ix+1)*{GRAIN_SEC})*1e6 as long))")))

# Attach the fault, keeping ONE row per (tag, window). A tag can have multiple events
# (SCR-SCN01 has 2), so a plain join would duplicate windows; pick the event whose 30-day
# degradation window this falls in, else the nearest.
vf = (vf.join(faults_sdf.select(F.col("tag_id"),
                                F.col("rms_end").alias("f_end"),
                                F.col("fail_epoch").alias("f_fail")),
              "tag_id", "left")
        .withColumn("days_to_fail", F.expr("(f_fail - win_epoch)/86400.0"))
        .withColumn("_active", F.expr("CASE WHEN days_to_fail BETWEEN 0 AND 30 THEN 0 ELSE 1 END"))
        .withColumn("_rn", F.expr("row_number() over (partition by tag_id, w_ix "
                                  "order by _active asc, abs(coalesce(days_to_fail, 1e12)) asc)"))
        .where("_rn = 1")
        .withColumn("label_failure_30d", F.expr("f_fail IS NOT NULL AND days_to_fail BETWEEN 0 AND 30")))

# Two-phase degradation (contract fault-injection spec). The point: waveform statistics
# (kurtosis, crest) move FIRST while RMS barely changes, so a model that reads them beats an
# RMS threshold; then in the final phase RMS climbs and the statistics fall back.
zr, zt = "greatest(-2.5,least(2.5,zr))", "greatest(-2.5,least(2.5,zt))"
IN_FAULT = "f_fail IS NOT NULL AND days_to_fail BETWEEN 0 AND 30"
PHASE1   = "days_to_fail > 12"                 # 30 -> 12 days before failure (early, impulsive)
P1 = "((30 - days_to_fail)/18.0)"              # 0 at 30d -> 1 at 12d
P2 = "((12 - days_to_fail)/12.0)"              # 0 at 12d -> 1 at 0d (final 12 days)
vf = (vf
    # RMS: baseline until phase 2. Phase 1 rises <=10%; phase 2 climbs to the table terminal.
    .withColumn("rms_mm_s", F.expr(
        f"""CASE WHEN NOT ({IN_FAULT}) THEN nominal*(1+0.05*{zr})
                 WHEN {PHASE1}          THEN nominal*(1 + 0.10*{P1})*(1+0.02*{zr})
                 ELSE (nominal*1.10 + (f_end - nominal*1.10)*{P2})*(1+0.02*{zr}) END"""))
    # Crest factor: 3.5 -> 6.0 in phase 1, eases back to ~4.5 in phase 2.
    .withColumn("crest_factor", F.expr(
        f"""CASE WHEN NOT ({IN_FAULT}) THEN 3.5 + 0.10*{zr}
                 WHEN {PHASE1}          THEN 3.5 + 2.5*{P1} + 0.08*{zr}
                 ELSE 6.0 - 1.5*{P2} + 0.08*{zr} END"""))
    # Kurtosis (RAW, Gaussian = 3.0): 3.0 -> 7.5 in phase 1, falls back to ~4.5 in phase 2.
    .withColumn("kurtosis", F.expr(
        f"""CASE WHEN NOT ({IN_FAULT}) THEN 3.18 + 0.12*{zt}
                 WHEN {PHASE1}          THEN 3.0 + 4.5*{P1} + 0.12*{zt}
                 ELSE 7.5 - 3.0*{P2} + 0.12*{zt} END"""))
    .withColumn("peak_mm_s",     F.expr("rms_mm_s*crest_factor"))   # VP = crest * VI
    .withColumn("stddev_mm_s",   F.expr(f"rms_mm_s*(0.68+0.03*abs({zr}))"))
    .withColumn("sample_count",  F.expr(f"cast(round({VIBRATION_HZ}*{GRAIN_SEC}) as bigint)"))
    # Temperature and current rise monotonically with the defect through both phases.
    .withColumn("_tp", F.expr(f"CASE WHEN {IN_FAULT} THEN least(1.0, greatest(0.0, (30-days_to_fail)/30.0)) ELSE 0.0 END"))
    .withColumn("bearing_temp_c",  F.expr(f"48 + 12*_tp + 1.2*{zt}"))
    .withColumn("motor_current_a", F.expr(f"60*(1+0.06*_tp)*(1+0.02*{zr})"))
    .withColumn("throughput_tph",  F.expr(f"2100*(0.97+0.03*sin(w_ix/144.0))*(1+0.01*{zr})"))
    .withColumn("run_minutes", F.expr(f"CASE WHEN down < 0.03 THEN round(down/0.03*{FEATURE_GRAIN_MIN},1) ELSE {FEATURE_GRAIN_MIN}.0 END"))
    .withColumn("iso_10816_zone", F.expr(
        "CASE WHEN rms_mm_s < hi*0.5 THEN 'A' WHEN rms_mm_s < hi THEN 'B' "
        "WHEN rms_mm_s < hi_hi THEN 'C' ELSE 'D' END")))

# hours_since_maintenance: resets at each failure point (post-repair), else climbs.
wtag = Window.partitionBy("tag_id").orderBy("window_start")
vf = (vf.withColumn("_reset", F.expr("CASE WHEN f_fail IS NOT NULL AND win_epoch >= f_fail THEN 1 ELSE 0 END"))
        .withColumn("_grp", F.sum("_reset").over(wtag))
        .withColumn("hours_since_maintenance",
                    F.expr(f"(row_number() over (partition by tag_id,_grp order by window_start)-1)*{FEATURE_GRAIN_MIN}/60.0")))

# Trend features (a bearing failure is a trajectory, not an absolute value — these are what
# let the model beat a plain RMS threshold).
w6    = Window.partitionBy("tag_id").orderBy("window_start").rowsBetween(-6, 0)     # ~1 h
w144  = Window.partitionBy("tag_id").orderBy("window_start").rowsBetween(-144, 0)   # ~24 h
wbase = Window.partitionBy("tag_id").orderBy("window_start").rowsBetween(-1008, 0)  # ~7 d
vf = (vf
    .withColumn("rms_delta_1h",  F.col("rms_mm_s") - F.first("rms_mm_s").over(w6))
    .withColumn("rms_delta_24h", F.col("rms_mm_s") - F.first("rms_mm_s").over(w144))
    .withColumn("rms_slope_24h", (F.col("rms_mm_s") - F.first("rms_mm_s").over(w144)) / 24.0)
    .withColumn("rms_pct_of_baseline", F.col("rms_mm_s") / F.avg("rms_mm_s").over(wbase) * 100.0))

vf.selectExpr(
    "asset_id", "tag_id", "window_start", "window_end",
    "cast(rms_mm_s as double) rms_mm_s", "cast(peak_mm_s as double) peak_mm_s",
    "cast(crest_factor as double) crest_factor", "cast(kurtosis as double) kurtosis",
    "cast(stddev_mm_s as double) stddev_mm_s", "cast(sample_count as bigint) sample_count",
    "cast(rms_delta_1h as double) rms_delta_1h", "cast(rms_delta_24h as double) rms_delta_24h",
    "cast(rms_slope_24h as double) rms_slope_24h", "cast(rms_pct_of_baseline as double) rms_pct_of_baseline",
    "cast(bearing_temp_c as double) bearing_temp_c", "cast(motor_current_a as double) motor_current_a",
    "cast(throughput_tph as double) throughput_tph", "cast(run_minutes as double) run_minutes",
    "cast(hours_since_maintenance as double) hours_since_maintenance",
    "iso_10816_zone", "cast(label_failure_30d as boolean) label_failure_30d"
).createOrReplaceTempView("_vib_final")

spark.sql(f"""
  INSERT OVERWRITE TABLE {FQ}.vibration_features_seed
  SELECT asset_id, tag_id, window_start, window_end,
         rms_mm_s, peak_mm_s, crest_factor, kurtosis, stddev_mm_s, sample_count,
         rms_delta_1h, rms_delta_24h, rms_slope_24h, rms_pct_of_baseline,
         bearing_temp_c, motor_current_a, throughput_tph, run_minutes,
         hours_since_maintenance, iso_10816_zone, label_failure_30d
  FROM _vib_final
""")
print("vibration_features_seed written")

# COMMAND ----------

# MAGIC %md ## Validation

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'tag_reading_seed' t, count(*) rows, count(distinct tag_id) tags,
         min(source_ts) mn, max(source_ts) mx FROM {FQ}.tag_reading_seed
  UNION ALL
  SELECT 'vibration_features_seed', count(*), count(distinct tag_id),
         min(window_start), max(window_end) FROM {FQ}.vibration_features_seed
"""))

# COMMAND ----------

# MAGIC %md #### Mass balance closes: per-unit-node imbalance from dry-solids weightometers

# COMMAND ----------

display(spark.sql(f"""
  WITH avgf AS (SELECT tag_id, avg(value) tph FROM {FQ}.tag_reading_seed GROUP BY tag_id),
       af AS (SELECT a.from_node_id, a.to_node_id, f.tph
              FROM {FQ}.dim_flowsheet_arc a JOIN avgf f ON a.measure_tag_id = f.tag_id
              WHERE a.stream_type='dry_solids'),
       i AS (SELECT to_node_id node, sum(tph) tin  FROM af GROUP BY to_node_id),
       o AS (SELECT from_node_id node, sum(tph) tout FROM af GROUP BY from_node_id)
  SELECT n.node_id, n.node_type, round(i.tin,1) mass_in, round(o.tout,1) mass_out,
         round(i.tin-o.tout,1) imbalance_tph,
         round(100*(i.tin-o.tout)/nullif(i.tin,0),2) imbalance_pct
  FROM {FQ}.dim_flowsheet_node n
  LEFT JOIN i ON n.node_id=i.node LEFT JOIN o ON n.node_id=o.node
  WHERE n.balance_role='unit' AND (i.tin IS NOT NULL AND o.tout IS NOT NULL)
  ORDER BY n.node_id
"""))

# COMMAND ----------

# MAGIC %md #### Desands reconstruction via Cw (should recover ~2100 underflow / ~400 overflow)

# COMMAND ----------

display(spark.sql(f"""
  WITH q AS (
    SELECT avg(CASE WHEN tag_id='DSD-CYC01-FI03' THEN value END) q_uf,
           avg(CASE WHEN tag_id='DSD-CYC01-DI03' THEN value END) rho_uf,
           avg(CASE WHEN tag_id='DSD-CYC01-FI02' THEN value END) q_of,
           avg(CASE WHEN tag_id='DSD-CYC01-DI02' THEN value END) rho_of
    FROM {FQ}.tag_reading_seed)
  SELECT round(q_uf*rho_uf*({SOLIDS_SG}*(rho_uf-1)/(rho_uf*({SOLIDS_SG}-1))),1) underflow_solids_tph,
         round(q_of*rho_of*({SOLIDS_SG}*(rho_of-1)/(rho_of*({SOLIDS_SG}-1))),1) overflow_solids_tph
  FROM q
"""))

# COMMAND ----------

# MAGIC %md #### Quality distribution, excursions, fault labels, freshness

# COMMAND ----------

display(spark.sql(f"""
  SELECT quality, count(*) n, round(100.0*count(*)/sum(count(*)) over (),3) pct
  FROM {FQ}.tag_reading_seed GROUP BY quality ORDER BY n DESC
"""))

display(spark.sql(f"""
  SELECT sum(case when r.value > t.hi_hi or r.value < t.lo_lo then 1 else 0 end) trip_breaches,
         sum(case when (r.value between t.hi and t.hi_hi) or (r.value between t.lo_lo and t.lo) then 1 else 0 end) warn_breaches,
         count(distinct case when r.value > t.hi or r.value < t.lo then r.tag_id end) tags_excursed
  FROM {FQ}.tag_reading_seed r JOIN {FQ}.dim_tag t USING (tag_id) WHERE t.value_class='analog'
"""))

display(spark.sql(f"""
  SELECT label_failure_30d, count(*) windows, round(avg(rms_mm_s),2) avg_rms,
         round(avg(crest_factor),2) avg_crest, round(avg(kurtosis),2) avg_kurt,
         round(avg(bearing_temp_c),1) avg_temp
  FROM {FQ}.vibration_features_seed GROUP BY label_failure_30d
"""))

display(spark.sql(f"""
  SELECT count(*) rows_last_30min FROM {FQ}.tag_reading_seed
  WHERE source_ts >= current_timestamp() - INTERVAL 30 MINUTES
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Two-phase degradation shape (lane B's check)
# MAGIC Phase 1 (30→12 d): crest and kurtosis move while RMS barely does (RMS +<10%, crest
# MAGIC 3.5→6.0, kurtosis 3.0→7.5). Phase 2 (final 12 d): RMS climbs to terminal, crest and
# MAGIC kurtosis fall back to 4–5. Also confirms VP/VK tags exist (VI/VP/VK = 15 each).

# COMMAND ----------

display(spark.sql(f"""
  SELECT instrument_type, count(*) n FROM {FQ}.dim_tag
  WHERE instrument_type LIKE 'V%' GROUP BY instrument_type ORDER BY instrument_type
"""))   # expect VI 15, VK 15, VP 15

_faults_df = spark.createDataFrame([(f["tag_id"], float(f["fail_epoch"])) for f in FAULTS], ["tag_id", "fail_epoch"])
_chk = (spark.table(f"{FQ}.vibration_features_seed").join(F.broadcast(_faults_df), "tag_id")
        .withColumn("d", F.expr("(fail_epoch - unix_timestamp(window_start))/86400.0"))
        .withColumn("bucket", F.expr(
            "CASE WHEN d BETWEEN 12 AND 30 THEN '1 phase1 (30-12d)' "
            "WHEN d BETWEEN 0 AND 12 THEN '2 phase2 (12-0d)' "
            "WHEN d BETWEEN 30 AND 60 THEN '0 baseline (60-30d)' END"))
        .where("bucket IS NOT NULL")
        .groupBy("bucket").agg(
            F.round(F.avg("rms_mm_s"), 2).alias("avg_rms"),
            F.round(F.avg("crest_factor"), 2).alias("avg_crest"),
            F.round(F.avg("kurtosis"), 2).alias("avg_kurt"))
        .orderBy("bucket"))
display(_chk)
