-- Query against the SYNCED Unity Catalog table, read from Postgres.
--
-- jack_freeman_catalog.tech_summit_scada_build.dim_tag is a governed Unity
-- Catalog table (the plant tag register). It is synced into Lakebase as
-- tech_summit_scada_build.dim_tag_online with scheduling_policy TRIGGERED, so
-- the application reads governed reference data at Postgres latency without
-- querying the lakehouse.
--
-- This is the query the control room app runs to render an asset panel: the
-- engineering alarm limits for every instrument on one asset.
SELECT
  tag_id,
  asset_name,
  area_name,
  measure,
  instrument_type,
  unit,
  nominal,
  lo_lo AS trip_low,
  lo    AS warn_low,
  hi    AS warn_high,
  hi_hi AS trip_high,
  is_pdm
FROM tech_summit_scada_build.dim_tag_online
WHERE asset_id = 'CRU-SCC01'
ORDER BY instrument_type, tag_id;
