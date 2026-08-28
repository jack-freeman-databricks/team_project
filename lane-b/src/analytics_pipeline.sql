-- Lane B analytical layer as a Lakeflow declarative pipeline.
--
-- These are MATERIALIZED VIEWS, not streaming tables, and that is deliberate.
-- The mass balance needs LAG (bin accumulation is a rate of change) and
-- ROW_NUMBER (choosing one reconciliation candidate per unmeasured arc), and
-- Structured Streaming rejects non-time-based window functions outright:
-- NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING. Verified by experiment earlier in
-- this build, not assumed. An MV recomputes and handles them correctly.
--
-- The one minute grain comes from the GROUP BY window, not from streaming.
--
-- Everything reads vw_tag_reading, the seam, so lane A can repoint the source
-- without touching anything here.

-- ---------------------------------------------------------------------
-- Per-tag one minute aggregates. The base grain everything else builds on.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW tag_reading_1m
  COMMENT "Per-tag one minute aggregates over the seam view."
AS
SELECT
  tag_id,
  window(source_ts, '1 minute').start AS window_start,
  window(source_ts, '1 minute').end   AS window_end,
  AVG(CASE WHEN quality = 'GOOD' THEN value END)    AS avg_value,
  MIN(CASE WHEN quality = 'GOOD' THEN value END)    AS min_value,
  MAX(CASE WHEN quality = 'GOOD' THEN value END)    AS max_value,
  STDDEV(CASE WHEN quality = 'GOOD' THEN value END) AS stddev_value,
  MIN_BY(value, source_ts)  AS first_value,
  MAX_BY(value, source_ts)  AS last_value,
  COUNT(*)                  AS sample_count,
  COUNT_IF(quality = 'GOOD') AS good_count,
  100.0 * COUNT_IF(quality = 'GOOD') / COUNT(*) AS good_pct
FROM vw_tag_reading
GROUP BY tag_id, window(source_ts, '1 minute');

-- ---------------------------------------------------------------------
-- Flow/density pairing for slurry arcs.
--
-- CONTRACT GAP, carried forward: dim_flowsheet_arc.measure_tag_id names ONE
-- tag, but measure_method='slurry_flow_density' needs a flow AND a density tag.
-- The pairing is recovered from the naming convention (FI02 pairs with DI02 on
-- the same asset). dim_flowsheet_arc should gain a nullable density_tag_id so
-- this stops being implicit knowledge in a string pattern.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW b_slurry_pair AS
SELECT f.tag_id AS flow_tag_id, d.tag_id AS density_tag_id
FROM dim_tag f
JOIN dim_tag d
  ON  d.asset_id = f.asset_id
  AND f.instrument_type = 'FI' AND d.instrument_type = 'DI'
  AND regexp_extract(f.tag_id, 'FI([0-9]+)$', 1)
    = regexp_extract(d.tag_id, 'DI([0-9]+)$', 1);

-- Window spine, so arcs with NO instrument still appear in every window.
-- Without it they inherit a null window_start from the failed reading join,
-- group alone, and reconciliation never sees them.
CREATE OR REFRESH MATERIALIZED VIEW b_windows AS
SELECT DISTINCT window_start, window_end FROM tag_reading_1m;

-- ---------------------------------------------------------------------
-- Per-arc tonnage. Weightometers read directly. Slurry streams are derived
-- from volumetric flow and density using the contract's SOLIDS_SG = 4.9:
--   Cw  = SG * (rho - 1) / (rho * (SG - 1))
--   t/h = Q_m3h * rho * Cw
-- Both lanes must use the same SG or the desands node never closes.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW mass_flow_1m
  COMMENT "Dry solids tonnage per flowsheet arc per minute."
AS
SELECT
  a.arc_id, w.window_start, w.window_end,
  a.from_node_id, a.to_node_id, a.stream_type, a.measure_method, a.design_tph,
  CASE a.measure_method
    WHEN 'weightometer' THEN fr.avg_value
    WHEN 'slurry_flow_density' THEN
      fr.avg_value * dr.avg_value
        * (4.9 * (dr.avg_value - 1.0) / (dr.avg_value * (4.9 - 1.0)))
  END AS measured_tph,
  (a.measure_tag_id IS NULL OR a.measure_method = 'inferred') AS is_estimated,
  LEAST(COALESCE(fr.good_pct, 100.0), COALESCE(dr.good_pct, 100.0)) AS good_sample_pct
FROM dim_flowsheet_arc a
CROSS JOIN b_windows w
LEFT JOIN tag_reading_1m fr
       ON fr.tag_id = a.measure_tag_id AND fr.window_start = w.window_start
LEFT JOIN b_slurry_pair sp
       ON sp.flow_tag_id = a.measure_tag_id AND a.measure_method = 'slurry_flow_density'
LEFT JOIN tag_reading_1m dr
       ON dr.tag_id = sp.density_tag_id AND dr.window_start = w.window_start;

-- Bin accumulation. A bin stores ore, so in minus out need not be zero: the
-- difference goes into or out of inventory. Ignoring it makes every bin look
-- permanently unbalanced.
CREATE OR REFRESH MATERIALIZED VIEW b_node_accum AS
SELECT n.node_id, r.window_start,
       (r.avg_value - LAG(r.avg_value) OVER (PARTITION BY n.node_id ORDER BY r.window_start))
         / 100.0 * n.capacity_t * 60.0 AS accumulation_tph
FROM dim_flowsheet_node n
JOIN tag_reading_1m r ON r.tag_id = n.accumulation_tag_id
WHERE n.accumulation_tag_id IS NOT NULL AND n.capacity_t > 0;

-- ---------------------------------------------------------------------
-- Reconciliation: back-calculate arcs that carry no instrument.
--
-- A02 (apron feeder to primary crusher) has no weightometer because the
-- feeder's own weightometer IS that measurement. Treating it as missing data
-- would grey out the plant inlet forever, so where a node has exactly ONE
-- unmeasured arc its tonnage follows from closing the balance.
--
-- An unmeasured arc sits between two nodes, so BOTH can infer it. Taking both
-- double counts the tonnage, so exactly one candidate is chosen, preferring the
-- UPSTREAM node: material flows downstream, and it leaves the downstream node
-- free to compare the inferred value against its own instrument.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW b_arc_infer AS
WITH solids AS (
  SELECT * FROM mass_flow_1m
  WHERE stream_type <> 'water' AND from_node_id <> to_node_id
),
per_node AS (
  SELECT n.node_id, s.window_start,
    SUM(CASE WHEN s.to_node_id   = n.node_id AND s.measured_tph IS NOT NULL THEN s.measured_tph END) known_in,
    SUM(CASE WHEN s.from_node_id = n.node_id AND s.measured_tph IS NOT NULL THEN s.measured_tph END) known_out,
    COUNT_IF(s.measured_tph IS NULL) unknown_arcs,
    MAX(CASE WHEN s.measured_tph IS NULL THEN s.arc_id END) unknown_arc_id,
    MAX(CASE WHEN s.measured_tph IS NULL
             THEN CASE WHEN s.to_node_id = n.node_id THEN 'in' ELSE 'out' END END) unknown_side
  FROM dim_flowsheet_node n
  JOIN solids s ON n.node_id IN (s.from_node_id, s.to_node_id)
  WHERE n.balance_role = 'unit'
  GROUP BY n.node_id, s.window_start
),
candidate AS (
  SELECT p.unknown_arc_id AS arc_id, p.window_start,
         p.node_id AS inferred_from_node_id, p.unknown_side,
         CASE p.unknown_side
           WHEN 'out' THEN COALESCE(p.known_in,0)  - COALESCE(p.known_out,0) - COALESCE(a.accumulation_tph,0)
           WHEN 'in'  THEN COALESCE(p.known_out,0) - COALESCE(p.known_in,0)  + COALESCE(a.accumulation_tph,0)
         END AS inferred_tph
  FROM per_node p
  LEFT JOIN b_node_accum a ON a.node_id = p.node_id AND a.window_start = p.window_start
  WHERE p.unknown_arcs = 1
)
SELECT arc_id, window_start, inferred_from_node_id, inferred_tph,
       candidate_count, inference_spread_tph
FROM (
  SELECT c.*,
    ROW_NUMBER() OVER (PARTITION BY arc_id, window_start
                       ORDER BY CASE unknown_side WHEN 'out' THEN 0 ELSE 1 END,
                                inferred_from_node_id) rn,
    COUNT(*)          OVER (PARTITION BY arc_id, window_start) candidate_count,
    MAX(inferred_tph) OVER (PARTITION BY arc_id, window_start)
  - MIN(inferred_tph) OVER (PARTITION BY arc_id, window_start) inference_spread_tph
  FROM candidate c)
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW b_mass_flow_final AS
SELECT f.arc_id, f.window_start, f.window_end, f.from_node_id, f.to_node_id,
       f.stream_type, f.measure_method, f.design_tph,
       COALESCE(f.measured_tph, i.inferred_tph) AS measured_tph,
       (f.measured_tph IS NULL AND i.inferred_tph IS NOT NULL) AS is_reconciled,
       f.is_estimated OR i.inferred_tph IS NOT NULL AS is_estimated,
       f.good_sample_pct
FROM mass_flow_1m f
LEFT JOIN b_arc_infer i ON i.arc_id = f.arc_id AND i.window_start = f.window_start;

-- ---------------------------------------------------------------------
-- THE flowsheet table the control room app renders.
--
-- imbalance is computed ONLY for balance_role='unit'. Sources and sinks never
-- close by definition and reporting one against them would put permanent false
-- alarms on screen.
--
-- status prefers honesty over precision: if arcs are unmeasured or quality is
-- poor it says INSUFFICIENT_DATA rather than publishing a plausible-looking
-- number. ESTIMATED means this node's own balance was used to reconcile an
-- unmeasured arc, so its imbalance is zero by construction and must NOT be read
-- as a verified balance. LANE C: render ESTIMATED distinctly from BALANCED.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW mass_balance_node_1m
  COMMENT "Per-node mass balance per minute. The flowsheet the app renders."
AS
WITH solids AS (
  SELECT * FROM b_mass_flow_final
  WHERE stream_type <> 'water' AND from_node_id <> to_node_id
),
flow AS (
  SELECT COALESCE(i.node_id, o.node_id) node_id,
         COALESCE(i.window_start, o.window_start) window_start,
         COALESCE(i.window_end, o.window_end) window_end,
         i.mass_in_tph, i.arcs_in_measured, COALESCE(i.arcs_in_total,0) arcs_in_total,
         o.mass_out_tph, o.arcs_out_measured, COALESCE(o.arcs_out_total,0) arcs_out_total,
         LEAST(COALESCE(i.q,100.0), COALESCE(o.q,100.0)) data_quality_pct
  FROM (SELECT to_node_id node_id, window_start, window_end,
               SUM(measured_tph) mass_in_tph,
               COUNT_IF(measured_tph IS NOT NULL) arcs_in_measured,
               COUNT(*) arcs_in_total, MIN(good_sample_pct) q
        FROM solids GROUP BY 1,2,3) i
  FULL OUTER JOIN
       (SELECT from_node_id node_id, window_start, window_end,
               SUM(measured_tph) mass_out_tph,
               COUNT_IF(measured_tph IS NOT NULL) arcs_out_measured,
               COUNT(*) arcs_out_total, MIN(good_sample_pct) q
        FROM solids GROUP BY 1,2,3) o
    ON i.node_id = o.node_id AND i.window_start = o.window_start
),
imb AS (
  SELECT n.node_id, f.window_start, f.window_end, n.balance_role,
         f.mass_in_tph, f.mass_out_tph,
         COALESCE(acc.accumulation_tph, 0.0) accumulation_tph,
         f.mass_in_tph - f.mass_out_tph - COALESCE(acc.accumulation_tph, 0.0) imbalance_raw,
         f.arcs_in_measured, f.arcs_in_total, f.arcs_out_measured, f.arcs_out_total,
         f.data_quality_pct,
         inf.inferred_from_node_id IS NOT NULL used_for_inference
  FROM dim_flowsheet_node n
  JOIN flow f ON f.node_id = n.node_id
  LEFT JOIN b_node_accum acc ON acc.node_id = n.node_id AND acc.window_start = f.window_start
  LEFT JOIN (SELECT DISTINCT inferred_from_node_id, window_start FROM b_arc_infer) inf
         ON inf.inferred_from_node_id = n.node_id AND inf.window_start = f.window_start
)
SELECT node_id, window_start, window_end, balance_role,
  mass_in_tph, mass_out_tph, accumulation_tph,
  CASE WHEN balance_role='unit' THEN imbalance_raw END imbalance_tph,
  CASE WHEN balance_role='unit' AND mass_in_tph>0
       THEN 100.0*imbalance_raw/mass_in_tph END imbalance_pct,
  CASE WHEN balance_role='unit' AND mass_in_tph>0
       THEN 100.0*mass_out_tph/mass_in_tph END closure_pct,
  arcs_in_measured, arcs_in_total, arcs_out_measured, arcs_out_total, data_quality_pct,
  CASE
    WHEN balance_role <> 'unit' THEN 'NOT_APPLICABLE'
    WHEN mass_in_tph IS NULL OR mass_out_tph IS NULL
      OR arcs_in_measured < arcs_in_total OR arcs_out_measured < arcs_out_total
      OR data_quality_pct < 80.0                THEN 'INSUFFICIENT_DATA'
    WHEN used_for_inference                     THEN 'ESTIMATED'
    WHEN ABS(100.0*imbalance_raw/NULLIF(mass_in_tph,0)) <= 2.0 THEN 'BALANCED'
    WHEN ABS(100.0*imbalance_raw/NULLIF(mass_in_tph,0)) <= 5.0 THEN 'DRIFT'
    ELSE 'ALARM'
  END status
FROM imb;

-- ---------------------------------------------------------------------
-- Equipment state. Only buildable now that discrete tags hold a state:
-- average dwell went from about 2 samples to over 11,000, so run/stop seconds
-- and a starts count finally mean something.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW equipment_state_1m
  COMMENT "Per-asset run state, availability and starts per minute."
AS
WITH z AS (
  SELECT t.asset_id, r.source_ts, r.value_text state,
         LAG(r.value_text) OVER (PARTITION BY t.asset_id ORDER BY r.source_ts) prev_state
  FROM vw_tag_reading r JOIN dim_tag t ON t.tag_id = r.tag_id
  WHERE t.instrument_type = 'ZI' AND t.measure = 'state' AND r.quality = 'GOOD'
),
w AS (
  SELECT asset_id, window(source_ts,'1 minute').start window_start,
         window(source_ts,'1 minute').end window_end,
         COUNT_IF(state='RUNNING') run_seconds,
         COUNT_IF(state='STOPPED') stop_seconds,
         COUNT_IF(state='FAULT')   fault_seconds,
         COUNT_IF(state='RUNNING' AND prev_state <> 'RUNNING') starts,
         MAX_BY(state, source_ts)  state
  FROM z GROUP BY asset_id, window(source_ts,'1 minute')
)
SELECT w.asset_id, w.window_start, w.window_end, w.state,
       w.run_seconds, w.stop_seconds, w.fault_seconds,
       100.0*w.run_seconds/NULLIF(w.run_seconds+w.stop_seconds+w.fault_seconds,0) availability_pct,
       c.avg_current_a, c.max_current_a, w.starts, c.throughput_tph
FROM w
LEFT JOIN (
  SELECT t.asset_id, r.window_start,
         AVG(CASE WHEN t.instrument_type='II' THEN r.avg_value END) avg_current_a,
         MAX(CASE WHEN t.instrument_type='II' THEN r.max_value END) max_current_a,
         AVG(CASE WHEN t.instrument_type='WI' THEN r.avg_value END) throughput_tph
  FROM tag_reading_1m r JOIN dim_tag t ON t.tag_id = r.tag_id
  GROUP BY t.asset_id, r.window_start) c
  ON c.asset_id = w.asset_id AND c.window_start = w.window_start;

-- ---------------------------------------------------------------------
-- Desands circuit water and solids balance, one row per minute.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW desands_balance_1m
  COMMENT "Desands cyclone water and solids balance per minute."
AS
WITH p AS (
  SELECT window_start, window_end,
    MAX(CASE WHEN tag_id='DSD-CYC01-FI01' THEN avg_value END) feed_q,
    MAX(CASE WHEN tag_id='DSD-CYC01-DI01' THEN avg_value END) feed_rho,
    MAX(CASE WHEN tag_id='DSD-CYC01-FI02' THEN avg_value END) of_q,
    MAX(CASE WHEN tag_id='DSD-CYC01-DI02' THEN avg_value END) of_rho,
    MAX(CASE WHEN tag_id='DSD-CYC01-FI03' THEN avg_value END) uf_q,
    MAX(CASE WHEN tag_id='DSD-CYC01-DI03' THEN avg_value END) uf_rho,
    MAX(CASE WHEN tag_id='UTL-WTR01-FI01' THEN avg_value END) water_q
  FROM tag_reading_1m
  WHERE tag_id IN ('DSD-CYC01-FI01','DSD-CYC01-DI01','DSD-CYC01-FI02','DSD-CYC01-DI02',
                   'DSD-CYC01-FI03','DSD-CYC01-DI03','UTL-WTR01-FI01')
  GROUP BY window_start, window_end
)
SELECT window_start, window_end,
  feed_q feed_slurry_m3h, feed_rho feed_density_tm3,
  feed_q*feed_rho*(4.9*(feed_rho-1)/(feed_rho*3.9)) feed_solids_tph,
  of_q overflow_slurry_m3h, of_rho overflow_density_tm3,
  of_q*of_rho*(4.9*(of_rho-1)/(of_rho*3.9)) overflow_solids_tph,
  uf_q underflow_slurry_m3h, uf_rho underflow_density_tm3,
  uf_q*uf_rho*(4.9*(uf_rho-1)/(uf_rho*3.9)) underflow_solids_tph,
  water_q water_added_m3h,
  100.0*(uf_q*uf_rho*(4.9*(uf_rho-1)/(uf_rho*3.9)))
       /NULLIF(feed_q*feed_rho*(4.9*(feed_rho-1)/(feed_rho*3.9)),0) solids_recovery_pct,
  100.0*(of_q*of_rho*(4.9*(of_rho-1)/(of_rho*3.9)))
       /NULLIF(feed_q*feed_rho*(4.9*(feed_rho-1)/(feed_rho*3.9)),0) fines_reject_pct,
  feed_q*feed_rho*(4.9*(feed_rho-1)/(feed_rho*3.9))
   - of_q*of_rho*(4.9*(of_rho-1)/(of_rho*3.9))
   - uf_q*uf_rho*(4.9*(uf_rho-1)/(uf_rho*3.9)) solids_imbalance_tph
FROM p;

-- ---------------------------------------------------------------------
-- Vibration features at 10 minute grain. The live counterpart of
-- vibration_features_seed, same 21 columns so the two can be unioned.
--
-- Crest factor is DERIVED as VP/VI and kurtosis is READ from the VK tag. It
-- cannot be computed from the VI tag: that tag carries an RMS value already
-- averaged by the instrument, and neither peak nor a fourth moment survives
-- that averaging. An earlier attempt derived both from VI and produced crest
-- factors around 1.1, below a sine wave's 1.41 and physically impossible for
-- bearing vibration. Hence the VP and VK partner tags.
--
-- VK carries RAW kurtosis, where a Gaussian signal reads 3.0. Do NOT pass it
-- through Spark's KURTOSIS(), which returns EXCESS kurtosis and reads 0.0 for a
-- Gaussian. Getting that backwards makes a healthy bearing look catastrophic.
--
-- Three columns cannot be derived from instrument readings at all:
-- hours_since_maintenance needs a maintenance log, and label_failure_30d is a
-- training label that only exists in hindsight. Both are NULL here and that is
-- correct rather than lazy. run_minutes IS derivable now that discrete tags hold
-- a state, so it comes from equipment_state_1m.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW vibration_features_10m
  COMMENT "Live 10-minute vibration features. Same columns as vibration_features_seed so the two union."
AS
WITH v AS (
  SELECT t.asset_id, t.instrument_type, t.nominal,
         regexp_extract(t.tag_id, '([0-9]+)$', 1) AS seq,
         window(r.source_ts, '10 minutes').start AS window_start,
         window(r.source_ts, '10 minutes').end   AS window_end,
         AVG(r.value) AS av, STDDEV(r.value) AS sd, COUNT(*) AS n
  FROM vw_tag_reading r
  JOIN dim_tag t ON t.tag_id = r.tag_id
  WHERE t.instrument_type IN ('VI','VP','VK') AND r.quality = 'GOOD'
  GROUP BY t.asset_id, t.instrument_type, t.nominal,
           regexp_extract(t.tag_id, '([0-9]+)$', 1),
           window(r.source_ts, '10 minutes')
),
ctx AS (   -- temperature, current and throughput on the same asset
  SELECT t.asset_id,
         window(r.source_ts, '10 minutes').start AS window_start,
         AVG(CASE WHEN t.instrument_type = 'TI' THEN r.value END) AS bearing_temp_c,
         AVG(CASE WHEN t.instrument_type = 'II' THEN r.value END) AS motor_current_a,
         AVG(CASE WHEN t.instrument_type = 'WI' THEN r.value END) AS throughput_tph
  FROM vw_tag_reading r
  JOIN dim_tag t ON t.tag_id = r.tag_id
  WHERE t.instrument_type IN ('TI','II','WI') AND r.quality = 'GOOD'
  GROUP BY t.asset_id, window(r.source_ts, '10 minutes')
),
run AS (   -- minutes actually running in the window, from the discrete state
  SELECT asset_id, window(window_start, '10 minutes').start AS window_start,
         SUM(run_seconds) / 60.0 AS run_minutes
  FROM equipment_state_1m
  GROUP BY asset_id, window(window_start, '10 minutes')
),
joined AS (
  SELECT
    vi.asset_id,
    CONCAT(vi.asset_id, '-VI', vi.seq) AS tag_id,
    vi.window_start, vi.window_end,
    vi.av AS rms_mm_s,
    vp.av AS peak_mm_s,
    vp.av / NULLIF(vi.av, 0) AS crest_factor,
    vk.av AS kurtosis,
    vi.sd AS stddev_mm_s,
    vi.n  AS sample_count,
    100.0 * vi.av / NULLIF(vi.nominal, 0) AS rms_pct_of_baseline,
    c.bearing_temp_c, c.motor_current_a, c.throughput_tph,
    rn.run_minutes
  FROM v vi
  JOIN v vp ON vp.asset_id = vi.asset_id AND vp.seq = vi.seq
           AND vp.window_start = vi.window_start AND vp.instrument_type = 'VP'
  JOIN v vk ON vk.asset_id = vi.asset_id AND vk.seq = vi.seq
           AND vk.window_start = vi.window_start AND vk.instrument_type = 'VK'
  LEFT JOIN ctx c  ON c.asset_id  = vi.asset_id AND c.window_start  = vi.window_start
  LEFT JOIN run rn ON rn.asset_id = vi.asset_id AND rn.window_start = vi.window_start
  WHERE vi.instrument_type = 'VI'
)
SELECT
  asset_id, tag_id, window_start, window_end,
  rms_mm_s, peak_mm_s, crest_factor, kurtosis, stddev_mm_s, sample_count,
  -- 6 and 144 ten-minute periods back, so one hour and 24 hours.
  rms_mm_s - LAG(rms_mm_s, 6)   OVER (PARTITION BY tag_id ORDER BY window_start) AS rms_delta_1h,
  rms_mm_s - LAG(rms_mm_s, 144) OVER (PARTITION BY tag_id ORDER BY window_start) AS rms_delta_24h,
  -- Trend in mm/s per day across the trailing 24h of 10-minute windows.
  REGR_SLOPE(rms_mm_s, UNIX_TIMESTAMP(window_start))
    OVER (PARTITION BY tag_id ORDER BY window_start
          ROWS BETWEEN 143 PRECEDING AND CURRENT ROW) * 86400 AS rms_slope_24h,
  rms_pct_of_baseline,
  bearing_temp_c, motor_current_a, throughput_tph,
  run_minutes,
  CAST(NULL AS DOUBLE)  AS hours_since_maintenance,  -- needs a maintenance log
  CASE WHEN rms_mm_s <  2.8 THEN 'A'
       WHEN rms_mm_s <  7.1 THEN 'B'
       WHEN rms_mm_s < 11.0 THEN 'C'
       ELSE 'D' END       AS iso_10816_zone,
  CAST(NULL AS BOOLEAN) AS label_failure_30d          -- a training label, hindsight only
FROM joined;
