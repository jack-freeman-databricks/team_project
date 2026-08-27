-- Populate the analytical alert table from the Lakebase change feed.
--
-- This is the seam pattern in miniature and the real path, not a load of
-- fixtures. The rule engine writes alerts into Lakebase plant.alert_outbox;
-- Lakebase CDF replicates every change into lb_alert_outbox_history; this reads
-- that history and projects the current state of each alert into the analytical
-- table lane C renders.
--
-- Two things the CDF shape forces:
--   * every Postgres UPDATE arrives as a preimage/postimage PAIR, so filtering
--     to insert + update_postimage is mandatory or acknowledgements double count
--   * ordering is by _pg_lsn, the Postgres log sequence number, not by wall
--     clock: two changes inside one transaction share a _timestamp
MERGE INTO jack_freeman_catalog.tech_summit_scada_build.alert AS t
USING (
  SELECT * FROM (
    SELECT
      alert_id, raised_at, tag_id, asset_id, area_code, node_id,
      rule_id, rule_name, rule_type, severity,
      trigger_value, trigger_text, limit_low, limit_high, message,
      delivery_status, delivery_attempts, delivered_at, http_status,
      acknowledged_by, acknowledged_at,
      ROW_NUMBER() OVER (PARTITION BY alert_id ORDER BY _pg_lsn DESC, _sort_by DESC) AS rn
    FROM jack_freeman_catalog.tech_summit_scada_build.lb_alert_outbox_history
    WHERE _pg_change_type IN ('insert', 'update_postimage')
  ) WHERE rn = 1
) AS s
ON t.alert_id = s.alert_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
