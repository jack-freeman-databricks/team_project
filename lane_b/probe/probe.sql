-- Decisive test: can a Lakeflow streaming table read a persisted Unity Catalog
-- VIEW as a streaming source? The entire seam design (lanes B and C read
-- vw_tag_reading, lane A repoints it at seam 1) depends on the answer.
--
-- Two datasets: the view read is the one under test, the table read is the
-- control. If the control passes and the view read fails, the seam has to
-- become a table rather than a view, and lane A must know before building.

CREATE OR REFRESH STREAMING TABLE b_probe_from_view AS
SELECT tag_id, source_ts, value
FROM STREAM(jack_freeman_catalog.tech_summit_scada_build.vw_tag_reading);

CREATE OR REFRESH STREAMING TABLE b_probe_from_table AS
SELECT tag_id, source_ts, value
FROM STREAM(jack_freeman_catalog.tech_summit_scada_build.tag_reading_seed);
