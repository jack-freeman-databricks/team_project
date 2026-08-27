-- The claim under test: a view containing a non-time-based window function
-- cannot be a streaming source. I asserted this and changed the contract on it,
-- so it needs proving rather than assuming.
CREATE OR REFRESH STREAMING TABLE b_probe3_window AS
SELECT tag_id, source_ts, value FROM STREAM(jack_freeman_catalog.tech_summit_scada_build.scratch_b_v_window);
