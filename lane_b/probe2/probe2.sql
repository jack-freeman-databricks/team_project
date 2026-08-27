-- Seam-1 shape: projection + WHERE filter over a UC view. Expect this to work.
CREATE OR REFRESH STREAMING TABLE b_probe2_filter AS
SELECT tag_id, source_ts, value FROM STREAM(jack_freeman_catalog.tech_summit_scada_build.scratch_b_v_filter);
