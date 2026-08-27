-- =====================================================================
-- Lane B: the mass balance, driven entirely by the flowsheet topology.
-- =====================================================================
-- Nothing about the plant is hardcoded here. Nodes, arcs, design tonnages,
-- measurement methods and accumulation tags all come from
-- dim_flowsheet_node / dim_flowsheet_arc, because lane C's app draws its
-- diagram from those same two tables and the two must agree.
--
-- Written as batch SQL first so it can be validated against a fixture of
-- design values before being wrapped in a Lakeflow pipeline. The seed data
-- is not populated yet, and this logic is the highest-risk part of the lane.
-- =====================================================================

USE CATALOG jack_freeman_catalog;
USE SCHEMA tech_summit_scada_build;

CREATE OR REPLACE TEMPORARY VIEW b_reading_1m AS
SELECT
  tag_id,
  window(source_ts, '1 minute').start AS window_start,
  window(source_ts, '1 minute').end   AS window_end,
  AVG(CASE WHEN quality = 'GOOD' THEN value END)   AS avg_value,
  COUNT(*)                                          AS sample_count,
  COUNT_IF(quality = 'GOOD')                        AS good_count,
  100.0 * COUNT_IF(quality = 'GOOD') / COUNT(*)     AS good_pct
FROM ${src_readings}
GROUP BY tag_id, window(source_ts, '1 minute');

-- ---------------------------------------------------------------------
-- Flow/density pairing for slurry arcs.
--
-- CONTRACT GAP: dim_flowsheet_arc.measure_tag_id names a single tag, but
-- measure_method='slurry_flow_density' needs BOTH a flow and a density tag
-- to derive dry solids tonnage. There is no density_tag_id column, so the
-- pairing has to be recovered from the tag naming convention: the density
-- tag is the DI tag on the same asset carrying the same numeric suffix as
-- the FI tag (DSD-CYC01-FI02 pairs with DSD-CYC01-DI02).
--
-- That is implicit knowledge encoded in a string pattern, which is exactly
-- what the contract exists to prevent. dim_flowsheet_arc should gain a
-- nullable density_tag_id. Until it does, the assertion in
-- lane_b/validate_mass_balance.py fails loudly if any slurry arc cannot
-- resolve a density partner, so a future arc cannot silently produce
-- wrong tonnage.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW b_slurry_pair AS
SELECT
  f.tag_id AS flow_tag_id,
  d.tag_id AS density_tag_id
FROM dim_tag f
JOIN dim_tag d
  ON  d.asset_id        = f.asset_id
  AND f.instrument_type = 'FI'
  AND d.instrument_type = 'DI'
  AND regexp_extract(f.tag_id, 'FI([0-9]+)$', 1)
    = regexp_extract(d.tag_id, 'DI([0-9]+)$', 1);

-- ---------------------------------------------------------------------
-- Per-arc tonnage. Dry solids from a belt weightometer read directly;
-- slurry streams derived from volumetric flow and slurry density using the
-- contract's SOLIDS_SG = 4.9 (hematite):
--   Cw   = SG * (rho - 1) / (rho * (SG - 1))     solids mass fraction
--   t/h  = Q_m3h * rho * Cw
-- Both lanes must use the same SG or the desands node never closes.
-- ---------------------------------------------------------------------
-- Window spine. Arcs with no measuring tag (measure_method='inferred') must
-- still appear in every window, otherwise they inherit a NULL window_start
-- from the failed reading join, group on their own, and the reconciliation
-- step never sees them alongside their sibling arcs.
CREATE OR REPLACE TEMPORARY VIEW b_windows AS
SELECT DISTINCT window_start, window_end FROM b_reading_1m;

-- ---------------------------------------------------------------------
-- Per-arc tonnage. Dry solids from a belt weightometer read directly;
-- slurry streams derived from volumetric flow and slurry density using the
-- contract's SOLIDS_SG = 4.9 (hematite):
--   Cw   = SG * (rho - 1) / (rho * (SG - 1))     solids mass fraction
--   t/h  = Q_m3h * rho * Cw
-- Both lanes must use the same SG or the desands node never closes.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW b_mass_flow_1m AS
WITH sg AS (SELECT 4.9 AS solids_sg)
SELECT
  a.arc_id,
  w.window_start,
  w.window_end,
  a.from_node_id,
  a.to_node_id,
  a.stream_type,
  a.measure_method,
  a.design_tph,
  CASE a.measure_method
    WHEN 'weightometer' THEN fr.avg_value
    WHEN 'slurry_flow_density' THEN
      fr.avg_value * dr.avg_value
        * (sg.solids_sg * (dr.avg_value - 1.0)
           / (dr.avg_value * (sg.solids_sg - 1.0)))
    ELSE NULL          -- 'inferred' and 'volume_flow' carry no solids tonnage
  END AS measured_tph,
  -- An arc with no measuring tag, or a method that yields no solids figure,
  -- is estimated. Lane C renders these visually distinct: showing an inferred
  -- number as though it were measured is how a control room loses trust.
  (a.measure_tag_id IS NULL OR a.measure_method = 'inferred') AS is_estimated,
  LEAST(COALESCE(fr.good_pct, 100.0), COALESCE(dr.good_pct, 100.0)) AS good_sample_pct
FROM dim_flowsheet_arc a
CROSS JOIN sg
CROSS JOIN b_windows w
LEFT JOIN b_reading_1m fr ON fr.tag_id = a.measure_tag_id
                         AND fr.window_start = w.window_start
LEFT JOIN b_slurry_pair sp ON sp.flow_tag_id = a.measure_tag_id
                          AND a.measure_method = 'slurry_flow_density'
LEFT JOIN b_reading_1m dr ON dr.tag_id = sp.density_tag_id
                         AND dr.window_start = w.window_start;

-- ---------------------------------------------------------------------
-- Data reconciliation: back-calculate arcs that have no instrument.
--
-- A02 (apron feeder -> primary crusher) carries no weightometer, because the
-- feeder's own weightometer IS the measurement of that stream. It is marked
-- measure_method='inferred' in the topology for exactly that reason.
--
-- Treating it as INSUFFICIENT_DATA, which is what a literal reading of the
-- contract gives, conflates two different situations: an instrument that has
-- failed, and a stream that is unmeasured by design. The second is a
-- permanent topology fact, and reporting it as missing data would grey out
-- the plant inlet on the control room screen forever.
--
-- Standard practice is to reconcile: where a node has exactly ONE unmeasured
-- arc, that arc's tonnage follows from closing the balance. The node used to
-- do the inferring then cannot also be used to verify it (its imbalance is
-- zero by construction), so it is reported as ESTIMATED rather than BALANCED.
-- Downstream nodes still get a genuine check: inferring A02 from N02 lets N03
-- compare it against A03, which is a real comparison of two instruments.
--
-- Single pass only. This topology needs no more, because every inferred arc
-- is adjacent to a node where everything else is measured. A topology with
-- two unknowns meeting at a node would need iteration, and would show up as
-- a remaining NULL rather than a wrong number.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW b_arc_infer AS
WITH solids AS (
  SELECT * FROM b_mass_flow_1m
  WHERE stream_type <> 'water' AND from_node_id <> to_node_id
),
per_node AS (
  SELECT n.node_id, s.window_start,
         SUM(CASE WHEN s.to_node_id   = n.node_id AND s.measured_tph IS NOT NULL THEN s.measured_tph END) AS known_in,
         SUM(CASE WHEN s.from_node_id = n.node_id AND s.measured_tph IS NOT NULL THEN s.measured_tph END) AS known_out,
         COUNT_IF(s.measured_tph IS NULL)                                        AS unknown_arcs,
         MAX(CASE WHEN s.measured_tph IS NULL THEN s.arc_id END)                 AS unknown_arc_id,
         MAX(CASE WHEN s.measured_tph IS NULL THEN
                  CASE WHEN s.to_node_id = n.node_id THEN 'in' ELSE 'out' END END) AS unknown_side
  FROM dim_flowsheet_node n
  JOIN solids s ON n.node_id IN (s.from_node_id, s.to_node_id)
  WHERE n.balance_role = 'unit'
  GROUP BY n.node_id, s.window_start
),
candidate AS (
  SELECT
    p.unknown_arc_id AS arc_id,
    p.window_start,
    p.node_id        AS inferred_from_node_id,
    p.unknown_side,
    CASE p.unknown_side
      WHEN 'out' THEN COALESCE(p.known_in, 0)  - COALESCE(p.known_out, 0) - COALESCE(a.accumulation_tph, 0)
      WHEN 'in'  THEN COALESCE(p.known_out, 0) - COALESCE(p.known_in, 0)  + COALESCE(a.accumulation_tph, 0)
    END AS inferred_tph
  FROM per_node p
  LEFT JOIN b_node_accum a
         ON a.node_id = p.node_id AND a.window_start = p.window_start
  WHERE p.unknown_arcs = 1
),
-- An unmeasured arc sits between two nodes, so BOTH can usually infer it and
-- we get two candidate values. Taking both fans the join out and double counts
-- the tonnage, and marks both neighbours ESTIMATED, which throws away a real
-- check. Pick exactly one, preferring the UPSTREAM node (the one where the
-- unknown arc is an outflow): material flows downstream, so filling an outflow
-- from a known inflow is the natural direction, and it leaves the downstream
-- node free to compare the inferred value against its own instrument.
--
-- inference_spread_tph keeps the discarded candidate's information: when two
-- neighbours disagree about the same stream, that gap is a genuine signal that
-- their instruments are inconsistent.
ranked AS (
  SELECT c.*,
         ROW_NUMBER() OVER (PARTITION BY arc_id, window_start
                            ORDER BY CASE unknown_side WHEN 'out' THEN 0 ELSE 1 END,
                                     inferred_from_node_id) AS rn,
         COUNT(*)          OVER (PARTITION BY arc_id, window_start) AS candidate_count,
         MAX(inferred_tph) OVER (PARTITION BY arc_id, window_start)
       - MIN(inferred_tph) OVER (PARTITION BY arc_id, window_start) AS inference_spread_tph
  FROM candidate c
)
SELECT arc_id, window_start, inferred_from_node_id, inferred_tph,
       candidate_count, inference_spread_tph
FROM ranked
WHERE rn = 1;

-- Measured where an instrument exists, reconciled where one does not.
CREATE OR REPLACE TEMPORARY VIEW b_mass_flow_1m_final AS
SELECT
  f.arc_id, f.window_start, f.window_end, f.from_node_id, f.to_node_id,
  f.stream_type, f.measure_method, f.design_tph,
  COALESCE(f.measured_tph, i.inferred_tph)          AS measured_tph,
  (f.measured_tph IS NULL AND i.inferred_tph IS NOT NULL) AS is_reconciled,
  f.is_estimated OR i.inferred_tph IS NOT NULL      AS is_estimated,
  f.good_sample_pct
FROM b_mass_flow_1m f
LEFT JOIN b_arc_infer i
       ON i.arc_id = f.arc_id AND i.window_start = f.window_start;

-- ---------------------------------------------------------------------
-- Node inflow / outflow.
--
-- Two exclusions matter:
--   * stream_type='water' is not ore. A15 is process water addition and must
--     never enter the solids balance.
--   * from_node_id = to_node_id is a self-loop (A15 again) and would other-
--     wise count as both an inflow and an outflow on the same node.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW b_node_flow AS
WITH solids AS (
  SELECT * FROM b_mass_flow_1m_final
  WHERE stream_type <> 'water' AND from_node_id <> to_node_id
),
inflow AS (
  SELECT to_node_id AS node_id, window_start, window_end,
         SUM(measured_tph)                  AS mass_in_tph,
         COUNT_IF(measured_tph IS NOT NULL) AS arcs_in_measured,
         COUNT(*)                            AS arcs_in_total,
         MIN(good_sample_pct)                AS in_quality_pct
  FROM solids GROUP BY to_node_id, window_start, window_end
),
outflow AS (
  SELECT from_node_id AS node_id, window_start, window_end,
         SUM(measured_tph)                  AS mass_out_tph,
         COUNT_IF(measured_tph IS NOT NULL) AS arcs_out_measured,
         COUNT(*)                            AS arcs_out_total,
         MIN(good_sample_pct)                AS out_quality_pct
  FROM solids GROUP BY from_node_id, window_start, window_end
)
SELECT
  COALESCE(i.node_id, o.node_id)            AS node_id,
  COALESCE(i.window_start, o.window_start)   AS window_start,
  COALESCE(i.window_end,   o.window_end)     AS window_end,
  i.mass_in_tph, i.arcs_in_measured,  COALESCE(i.arcs_in_total, 0)  AS arcs_in_total,
  o.mass_out_tph, o.arcs_out_measured, COALESCE(o.arcs_out_total, 0) AS arcs_out_total,
  LEAST(COALESCE(i.in_quality_pct, 100.0), COALESCE(o.out_quality_pct, 100.0)) AS data_quality_pct
FROM inflow i
FULL OUTER JOIN outflow o
  ON i.node_id = o.node_id AND i.window_start = o.window_start;

-- ---------------------------------------------------------------------
-- Accumulation. A bin genuinely stores ore, so inflow minus outflow does
-- not have to be zero: the difference goes into or comes out of inventory.
-- Ignoring this term makes every bin look permanently unbalanced.
--
--   tonnes held = level_pct/100 * capacity_t
--   accumulation_tph = d(tonnes)/dt, and a 1-minute delta scales by 60.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW b_node_accum AS
SELECT
  n.node_id,
  r.window_start,
  (r.avg_value - LAG(r.avg_value) OVER (PARTITION BY n.node_id ORDER BY r.window_start))
    / 100.0 * n.capacity_t * 60.0 AS accumulation_tph
FROM dim_flowsheet_node n
JOIN b_reading_1m r ON r.tag_id = n.accumulation_tag_id
WHERE n.accumulation_tag_id IS NOT NULL
  AND n.capacity_t > 0;

-- ---------------------------------------------------------------------
-- The flowsheet table lane C renders.
--
-- imbalance is computed ONLY for balance_role='unit'. Sources and sinks
-- never close by definition, and reporting an imbalance against them would
-- put permanent false alarms on the control room screen.
--
-- status values, in the order the CASE tests them:
--   NOT_APPLICABLE    source or sink; closure is meaningless
--   INSUFFICIENT_DATA an instrument is genuinely missing or quality is poor
--   ESTIMATED         this node's balance was used to reconcile an unmeasured
--                     arc, so its own imbalance is zero by construction and
--                     must NOT be read as a verified balance
--   BALANCED / DRIFT / ALARM   a real imbalance, within 2% / 5% / beyond
--
-- LANE C: ESTIMATED is new since the first draft of the contract. Render it
-- distinctly from BALANCED, it is "we filled this in", not "this checks out".
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW b_mass_balance_node_1m AS
WITH imb AS (
  SELECT
    n.node_id, f.window_start, f.window_end, n.balance_role,
    f.mass_in_tph, f.mass_out_tph,
    COALESCE(acc.accumulation_tph, 0.0) AS accumulation_tph,
    f.mass_in_tph - f.mass_out_tph - COALESCE(acc.accumulation_tph, 0.0) AS imbalance_raw,
    f.arcs_in_measured, f.arcs_in_total, f.arcs_out_measured, f.arcs_out_total,
    f.data_quality_pct,
    inf.inferred_from_node_id IS NOT NULL AS used_for_inference
  FROM dim_flowsheet_node n
  JOIN b_node_flow f ON f.node_id = n.node_id
  LEFT JOIN b_node_accum acc
         ON acc.node_id = n.node_id AND acc.window_start = f.window_start
  LEFT JOIN (SELECT DISTINCT inferred_from_node_id, window_start FROM b_arc_infer) inf
         ON inf.inferred_from_node_id = n.node_id AND inf.window_start = f.window_start
)
SELECT
  node_id, window_start, window_end, balance_role,
  mass_in_tph, mass_out_tph, accumulation_tph,
  CASE WHEN balance_role = 'unit' THEN imbalance_raw END AS imbalance_tph,
  CASE WHEN balance_role = 'unit' AND mass_in_tph > 0
       THEN 100.0 * imbalance_raw / mass_in_tph END      AS imbalance_pct,
  CASE WHEN balance_role = 'unit' AND mass_in_tph > 0
       THEN 100.0 * mass_out_tph / mass_in_tph END       AS closure_pct,
  arcs_in_measured, arcs_in_total, arcs_out_measured, arcs_out_total,
  data_quality_pct,
  CASE
    WHEN balance_role <> 'unit' THEN 'NOT_APPLICABLE'
    WHEN mass_in_tph IS NULL OR mass_out_tph IS NULL
      OR arcs_in_measured  < arcs_in_total
      OR arcs_out_measured < arcs_out_total
      OR data_quality_pct  < 80.0                THEN 'INSUFFICIENT_DATA'
    WHEN used_for_inference                       THEN 'ESTIMATED'
    WHEN ABS(100.0 * imbalance_raw / NULLIF(mass_in_tph, 0)) <= 2.0 THEN 'BALANCED'
    WHEN ABS(100.0 * imbalance_raw / NULLIF(mass_in_tph, 0)) <= 5.0 THEN 'DRIFT'
    ELSE 'ALARM'
  END AS status
FROM imb;
