-- src/06_prediction_scorecard.sql
-- Self-grading backtest: did each agent call come true one week later?
--
-- Popularity = AVG(101 - rank) per brand per day, read from app_rank_history
-- (the time-series table, months of history). Brand = the flagship app only
-- (Netflix, Disney+); this must match NAMES_OF in 05, which builds the
-- agent's popularity signal from the same table.
--
-- Each call is scored against popularity ~7 days later and compared to the
-- naive "always hold" baseline. Beating the baseline is what makes it a
-- result, not a demo. Because app_rank_history already holds months of data,
-- this runs as a true backtest today -- no week of waiting -- as soon as
-- predictions exist for historical "as of" dates.

CREATE OR REPLACE TABLE bootcamp_students.oscr_gold.prediction_scorecard AS
WITH sym AS (
  SELECT 'NFLX' AS symbol, 'Netflix' AS brand UNION ALL
  SELECT 'DIS'  AS symbol, 'Disney+' AS brand
),
pop AS (
  SELECT
    CASE WHEN name = 'Netflix' THEN 'Netflix'
         WHEN name = 'Disney+' THEN 'Disney+' END AS brand,
    snapshot_date AS d,
    ROUND(AVG(101 - rank), 1) AS popularity
  FROM bootcamp_students.oscr_gold.app_rank_history
  WHERE name IN ('Netflix', 'Disney+')
  GROUP BY ALL
),
p AS (
  SELECT m.brand, pr.prediction_date AS d0, pr.call, p0.popularity AS pop_then
  FROM bootcamp_students.oscr_gold.predictions pr
  JOIN sym m       ON m.symbol = pr.symbol
  LEFT JOIN pop p0 ON p0.brand = m.brand AND p0.d = pr.prediction_date
),
fut_pop AS (
  -- popularity closest to d0+7 within [d0+6, d0+8]; ties go to the earlier day
  SELECT k.brand, k.d0,
    min_by(f.popularity, struct(abs(datediff(f.d, date_add(k.d0, 7))), f.d)) AS pop_future
  FROM (SELECT DISTINCT brand, d0 FROM p) k
  JOIN pop f
    ON f.brand = k.brand
   AND f.d BETWEEN date_add(k.d0, 6) AND date_add(k.d0, 8)
  GROUP BY k.brand, k.d0
),
fut AS (
  SELECT p.*, fp.pop_future
  FROM p
  LEFT JOIN fut_pop fp ON fp.brand = p.brand AND fp.d0 = p.d0
)
SELECT brand, d0 AS prediction_date, call, pop_then, pop_future,
  CASE WHEN pop_future IS NULL THEN NULL
       WHEN pop_future - pop_then >  2 THEN 'rise'
       WHEN pop_future - pop_then < -2 THEN 'fall'
       ELSE 'hold' END AS actual_move,
  CASE WHEN pop_future IS NULL THEN NULL
       WHEN call = (CASE WHEN pop_future - pop_then >  2 THEN 'rise'
                         WHEN pop_future - pop_then < -2 THEN 'fall'
                         ELSE 'hold' END) THEN 1 ELSE 0 END AS hit
FROM fut;

-- Headline: agent hit-rate vs the always-"hold" baseline
-- SELECT ROUND(AVG(hit), 2) AS agent_hit_rate,
--        ROUND(AVG(CASE WHEN actual_move = 'hold' THEN 1 ELSE 0 END), 2) AS always_hold_baseline,
--        COUNT(*) AS scored_predictions
-- FROM bootcamp_students.oscr_gold.prediction_scorecard WHERE hit IS NOT NULL;
