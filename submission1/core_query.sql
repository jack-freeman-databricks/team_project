-- Which assets are costing the most unplanned downtime, what is driving it, and
-- how does that compare against the governed ISO 10816 alarm limits?
--
-- Joins the SYNCED governed Unity Catalog table (dim_tag_online, read-only in
-- Postgres) against writable operational tables (work_order, work_order_note).
SELECT
  t.asset_id,
  MAX(t.asset_name)                                        AS asset_name,
  MAX(t.asset_type)                                        AS asset_type,
  MAX(t.area_name)                                         AS area,
  COUNT(DISTINCT w.work_order_id)                          AS work_orders,
  COUNT(DISTINCT w.work_order_id)
    FILTER (WHERE w.status <> 'CLOSED')                    AS still_open,
  COALESCE(SUM(w.downtime_minutes), 0)                     AS downtime_minutes,
  ROUND(COALESCE(SUM(w.downtime_minutes), 0) / 60.0, 1)    AS downtime_hours,
  STRING_AGG(DISTINCT w.failure_mode, ', '
             ORDER BY w.failure_mode)                      AS failure_modes,
  -- Governed engineering limits, straight from the synced UC register
  MAX(t.hi)    FILTER (WHERE t.instrument_type = 'VI')     AS vib_warn_limit_mm_s,
  MAX(t.hi_hi) FILTER (WHERE t.instrument_type = 'VI')     AS vib_trip_limit_mm_s,
  COUNT(n.note_id)                                         AS diagnostic_notes
FROM plant.work_order w
JOIN tech_summit_scada_build.dim_tag_online t
  ON t.tag_id = w.tag_id
LEFT JOIN plant.work_order_note n
  ON n.work_order_id = w.work_order_id
GROUP BY t.asset_id
HAVING COALESCE(SUM(w.downtime_minutes), 0) > 0
ORDER BY downtime_minutes DESC;
