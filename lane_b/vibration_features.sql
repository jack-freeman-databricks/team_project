WITH vib_readings AS (
  SELECT
    dt.asset_id,
    tr.tag_id,
    tr.source_ts,
    tr.value,
    dt.nominal,
    CAST(FLOOR(UNIX_TIMESTAMP(tr.source_ts) / 600) * 600 AS LONG) AS window_ts
  FROM jack_freeman_catalog.tech_summit_scada_build.vw_tag_reading tr
  INNER JOIN jack_freeman_catalog.tech_summit_scada_build.dim_tag dt
    ON tr.tag_id = dt.tag_id
  WHERE dt.instrument_type = 'VI'
    AND tr.quality = 'GOOD'
),
window_stats AS (
  SELECT
    asset_id,
    tag_id,
    window_ts,
    from_unixtime(window_ts) AS window_start,
    from_unixtime(window_ts + 600) AS window_end,
    MAX(nominal) AS nominal,
    SQRT(AVG(value * value)) AS rms_mm_s,
    MAX(ABS(value)) AS peak_mm_s,
    COUNT(*) AS sample_count,
    STDDEV_POP(value) AS stddev_mm_s,
    KURTOSIS(value) AS kurtosis
  FROM vib_readings
  GROUP BY asset_id, tag_id, window_ts
),
window_with_lag AS (
  SELECT
    asset_id,
    tag_id,
    window_start,
    window_end,
    window_ts,
    nominal,
    rms_mm_s,
    peak_mm_s,
    sample_count,
    stddev_mm_s,
    kurtosis,
    LAG(rms_mm_s, 6) OVER (PARTITION BY asset_id, tag_id ORDER BY window_ts) AS rms_1h_ago,
    LAG(rms_mm_s, 144) OVER (PARTITION BY asset_id, tag_id ORDER BY window_ts) AS rms_24h_ago
  FROM window_stats
),
window_with_deltas AS (
  SELECT
    asset_id,
    tag_id,
    window_start,
    window_end,
    window_ts,
    nominal,
    rms_mm_s,
    peak_mm_s,
    sample_count,
    stddev_mm_s,
    kurtosis,
    CASE WHEN rms_mm_s > 0 THEN peak_mm_s / rms_mm_s ELSE NULL END AS crest_factor,
    CASE WHEN rms_1h_ago IS NOT NULL THEN rms_mm_s - rms_1h_ago ELSE NULL END AS rms_delta_1h,
    CASE WHEN rms_24h_ago IS NOT NULL THEN rms_mm_s - rms_24h_ago ELSE NULL END AS rms_delta_24h
  FROM window_with_lag
),
window_with_slope AS (
  SELECT
    asset_id,
    tag_id,
    window_start,
    window_end,
    window_ts,
    nominal,
    rms_mm_s,
    peak_mm_s,
    sample_count,
    stddev_mm_s,
    kurtosis,
    crest_factor,
    rms_delta_1h,
    rms_delta_24h,
    REGR_SLOPE(rms_mm_s, CAST(window_ts AS DOUBLE))
      OVER (PARTITION BY asset_id, tag_id ORDER BY window_ts ROWS BETWEEN 143 PRECEDING AND CURRENT ROW)
      AS slope_raw
  FROM window_with_deltas
),
window_with_normalized_slope AS (
  SELECT
    asset_id,
    tag_id,
    window_start,
    window_end,
    window_ts,
    nominal,
    rms_mm_s,
    peak_mm_s,
    sample_count,
    stddev_mm_s,
    kurtosis,
    crest_factor,
    rms_delta_1h,
    rms_delta_24h,
    CASE
      WHEN slope_raw IS NOT NULL THEN slope_raw * 86400.0
      ELSE NULL
    END AS rms_slope_24h
  FROM window_with_slope
),
window_with_baseline_pct AS (
  SELECT
    asset_id,
    tag_id,
    window_start,
    window_end,
    window_ts,
    nominal,
    rms_mm_s,
    peak_mm_s,
    sample_count,
    stddev_mm_s,
    kurtosis,
    crest_factor,
    rms_delta_1h,
    rms_delta_24h,
    rms_slope_24h,
    CASE
      WHEN nominal > 0 THEN (rms_mm_s / nominal) * 100.0
      ELSE NULL
    END AS rms_pct_of_baseline
  FROM window_with_normalized_slope
),
window_with_iso_zone AS (
  SELECT
    asset_id,
    tag_id,
    window_start,
    window_end,
    window_ts,
    rms_mm_s,
    peak_mm_s,
    sample_count,
    stddev_mm_s,
    kurtosis,
    crest_factor,
    rms_delta_1h,
    rms_delta_24h,
    rms_slope_24h,
    rms_pct_of_baseline,
    CASE
      WHEN rms_mm_s < 2.8 THEN 'A'
      WHEN rms_mm_s < 7.1 THEN 'B'
      WHEN rms_mm_s < 11.0 THEN 'C'
      ELSE 'D'
    END AS iso_10816_zone
  FROM window_with_baseline_pct
),
context_sensors AS (
  SELECT
    dt.asset_id,
    from_unixtime(CAST(FLOOR(UNIX_TIMESTAMP(tr.source_ts) / 600) * 600 AS LONG)) AS window_start,
    from_unixtime(CAST(FLOOR(UNIX_TIMESTAMP(tr.source_ts) / 600) * 600 AS LONG) + 600) AS window_end,
    dt.instrument_type,
    dt.measure,
    AVG(tr.value) AS avg_value
  FROM jack_freeman_catalog.tech_summit_scada_build.vw_tag_reading tr
  INNER JOIN jack_freeman_catalog.tech_summit_scada_build.dim_tag dt
    ON tr.tag_id = dt.tag_id
  WHERE dt.instrument_type IN ('TI', 'II', 'WI')
    AND dt.measure IN ('temperature', 'motor_current', 'mass_flow')
    AND tr.quality = 'GOOD'
  GROUP BY dt.asset_id, window_start, window_end, dt.instrument_type, dt.measure
),
pivoted_context AS (
  SELECT
    asset_id,
    window_start,
    window_end,
    MAX(CASE WHEN measure = 'temperature' THEN avg_value ELSE NULL END) AS bearing_temp_c,
    MAX(CASE WHEN measure = 'motor_current' THEN avg_value ELSE NULL END) AS motor_current_a,
    MAX(CASE WHEN measure = 'mass_flow' THEN avg_value ELSE NULL END) AS throughput_tph
  FROM context_sensors
  GROUP BY asset_id, window_start, window_end
),
features_with_context AS (
  SELECT
    vf.asset_id,
    vf.tag_id,
    vf.window_start,
    vf.window_end,
    vf.rms_mm_s,
    vf.peak_mm_s,
    vf.crest_factor,
    vf.kurtosis,
    vf.stddev_mm_s,
    vf.sample_count,
    vf.rms_delta_1h,
    vf.rms_delta_24h,
    vf.rms_slope_24h,
    vf.rms_pct_of_baseline,
    vf.iso_10816_zone,
    pc.bearing_temp_c,
    pc.motor_current_a,
    pc.throughput_tph
  FROM window_with_iso_zone vf
  LEFT JOIN pivoted_context pc
    ON vf.asset_id = pc.asset_id
    AND vf.window_start = pc.window_start
    AND vf.window_end = pc.window_end
)
SELECT
  asset_id,
  tag_id,
  window_start,
  window_end,
  ROUND(rms_mm_s, 4) AS rms_mm_s,
  ROUND(peak_mm_s, 4) AS peak_mm_s,
  ROUND(crest_factor, 4) AS crest_factor,
  ROUND(kurtosis, 4) AS kurtosis,
  ROUND(stddev_mm_s, 4) AS stddev_mm_s,
  sample_count,
  ROUND(rms_delta_1h, 4) AS rms_delta_1h,
  ROUND(rms_delta_24h, 4) AS rms_delta_24h,
  ROUND(rms_slope_24h, 4) AS rms_slope_24h,
  ROUND(rms_pct_of_baseline, 2) AS rms_pct_of_baseline,
  ROUND(bearing_temp_c, 2) AS bearing_temp_c,
  ROUND(motor_current_a, 2) AS motor_current_a,
  ROUND(throughput_tph, 2) AS throughput_tph,
  CAST(NULL AS INT) AS run_minutes,
  CAST(NULL AS INT) AS hours_since_maintenance,
  iso_10816_zone,
  CAST(NULL AS INT) AS label_failure_30d
FROM features_with_context
ORDER BY asset_id, tag_id, window_start;
