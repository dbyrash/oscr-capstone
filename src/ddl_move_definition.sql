-- One-off: defines brand popularity and freezes the rise/hold/fall cutoffs that 05 and 06 both read.

-- US storefront only: other storefronts drop in and out, which moves a cross-storefront average.
CREATE OR REPLACE VIEW bootcamp_students.oscr_gold.brand_popularity AS
SELECT name AS brand, snapshot_date AS d, CAST(101 - rank AS DOUBLE) AS popularity
FROM bootcamp_students.oscr_gold.app_rank_history
WHERE _storefront = 'us' AND name IN ('Netflix', 'Disney+');

-- Terciles of all 7-day changes, both brands pooled. IF NOT EXISTS freezes them: a rerun never recomputes.
-- Rule: fall if delta < fall_below, rise if delta > rise_above, else hold.
CREATE TABLE IF NOT EXISTS bootcamp_students.oscr_gold.move_cutoffs AS
WITH deltas AS (
  SELECT a.d, b.popularity - a.popularity AS delta
  FROM bootcamp_students.oscr_gold.brand_popularity a
  JOIN bootcamp_students.oscr_gold.brand_popularity b
    ON b.brand = a.brand AND b.d = date_add(a.d, 7)
)
SELECT
  7 AS horizon_days,
  percentile_cont(1.0 / 3) WITHIN GROUP (ORDER BY delta) AS fall_below,
  percentile_cont(2.0 / 3) WITHIN GROUP (ORDER BY delta) AS rise_above,
  COUNT(*) AS n_deltas,
  MIN(d) AS first_d,
  MAX(d) AS last_d,
  current_timestamp() AS frozen_at
FROM deltas;
