-- Current state per product stockpile, from the stacker instrumentation.
--
-- Which stockpile is being fed comes from the stacking indicator
-- STK-STK01-ZI02, a discrete tag carrying IDLE / SP01 / SP02 / SP03 in
-- value_text. How much is going there comes from the stacker boom weightometer
-- STK-STK01-WI01 in t/h. Tonnage is the integral of rate over time, so the
-- sample interval matters: it is measured per reading with LEAD rather than
-- assumed to be 1 Hz, because a dropped sample would otherwise silently
-- inflate the tonnage.
--
-- Grade comes from the product sampler, averaged over the window each
-- stockpile was actually being stacked, not over the whole shift.
MERGE INTO jack_freeman_catalog.tech_summit_scada_build.stockpile_state AS t
USING (
  WITH ind AS (   -- which stockpile, and for how long
    SELECT value_text AS stockpile_id, source_ts,
           LEAD(source_ts) OVER (ORDER BY source_ts) AS next_ts
    FROM jack_freeman_catalog.tech_summit_scada_build.vw_tag_reading
    WHERE tag_id = 'STK-STK01-ZI02' AND quality = 'GOOD'
  ),
  rate AS (       -- stacker boom rate, t/h
    SELECT source_ts, value AS tph
    FROM jack_freeman_catalog.tech_summit_scada_build.vw_tag_reading
    WHERE tag_id = 'STK-STK01-WI01' AND quality = 'GOOD'
  ),
  runs AS (       -- group contiguous samples of the same state into runs, so
                  -- stack_started_at means "when the CURRENT campaign began",
                  -- not "the first time this pile was ever fed"
    SELECT stockpile_id, source_ts, next_ts,
           SUM(CASE WHEN prev_pile = stockpile_id THEN 0 ELSE 1 END)
             OVER (ORDER BY source_ts) AS run_id
    FROM (SELECT *, LAG(stockpile_id) OVER (ORDER BY source_ts) AS prev_pile FROM ind)
  ),
  stacked AS (    -- integrate rate over the interval each sample covers
    SELECT i.stockpile_id,
           SUM(r.tph * TIMESTAMPDIFF(SECOND, i.source_ts,
                 COALESCE(i.next_ts, i.source_ts + INTERVAL 1 SECOND)) / 3600.0) AS tonnes,
           MAX(i.source_ts) AS last_ts,
           COUNT(DISTINCT i.run_id) AS campaigns
    FROM runs i
    JOIN rate r ON r.source_ts = i.source_ts
    WHERE i.stockpile_id <> 'IDLE'
    GROUP BY i.stockpile_id
  ),
  latest_run AS (  -- start of the most recent contiguous campaign per pile
    SELECT stockpile_id, MIN(source_ts) AS run_started_at
    FROM runs r
    WHERE stockpile_id <> 'IDLE'
      AND run_id = (SELECT MAX(run_id) FROM runs r2 WHERE r2.stockpile_id = r.stockpile_id)
    GROUP BY stockpile_id
  ),
  grade AS (      -- product grade while each stockpile was being fed
    SELECT i.stockpile_id,
           AVG(CASE WHEN q.tag_id = 'LAB-PRD01-QI01' THEN q.value END) AS avg_fe_pct,
           AVG(CASE WHEN q.tag_id = 'LAB-PRD01-QI02' THEN q.value END) AS avg_sio2_pct
    FROM ind i
    JOIN jack_freeman_catalog.tech_summit_scada_build.vw_tag_reading q
      ON q.source_ts >= i.source_ts
     AND q.source_ts <  COALESCE(i.next_ts, i.source_ts + INTERVAL 1 SECOND)
     AND q.tag_id IN ('LAB-PRD01-QI01','LAB-PRD01-QI02') AND q.quality = 'GOOD'
    WHERE i.stockpile_id <> 'IDLE'
    GROUP BY i.stockpile_id
  ),
  active AS (     -- the one being stacked right now
    SELECT value_text AS stockpile_id
    FROM jack_freeman_catalog.tech_summit_scada_build.vw_tag_reading
    WHERE tag_id = 'STK-STK01-ZI02' AND quality = 'GOOD'
    QUALIFY ROW_NUMBER() OVER (ORDER BY source_ts DESC) = 1
  )
  SELECT
    s.stockpile_id,
    s.last_ts                                   AS as_at,
    (s.stockpile_id = (SELECT stockpile_id FROM active)) AS is_stacking,
    'STK-STK01'                                 AS stacker_id,
    ROUND(s.tonnes, 1)                          AS tonnes_stacked_cum,
    -- Nothing has been reclaimed in this window, so what was stacked is what is
    -- on the ground. Reclaim instrumentation would subtract here.
    ROUND(s.tonnes, 1)                          AS tonnes_on_ground,
    ROUND(g.avg_fe_pct, 2)                      AS avg_fe_pct,
    ROUND(g.avg_sio2_pct, 2)                    AS avg_sio2_pct,
    lr.run_started_at                           AS stack_started_at
  FROM stacked s
  LEFT JOIN grade g      USING (stockpile_id)
  LEFT JOIN latest_run lr USING (stockpile_id)
) AS src
ON t.stockpile_id = src.stockpile_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
