-- src/06_prediction_scorecard.sql
-- Self-grading backtest: did each agent call come true one week later?
--
-- Popularity = AVG(101 - rank) per brand per day, read from app_rank_history
-- (the time-series table, months of history). Disney's apps
-- (Disney+, Disney Plus, Hotstar, JioHotstar) fold into one "Disney+" brand,
-- matching the dashboard. Predictions store a ticker (NFLX/DIS), so the `sym`
-- map bridges ticker -> brand name; there is no linking table.
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
         WHEN name IN ('Disney+', 'Disney Plus', 'Hotstar', 'JioHotstar') THEN 'Disney+' END AS brand,
    snapshot_date AS d,
    ROUND(AVG(101 - rank), 1) AS popularity
  FROM bootcamp_students.oscr_gold.app_rank_history
  WHERE name IN ('Netflix', 'Disney+', 'Disney Plus', 'Hotstar', 'JioHotstar')
  GROUP BY ALL
),
p AS (
  SELECT m.brand, pr.prediction_date AS d0, pr.call, p0.popularity AS pop_then
  FROM bootcamp_students.oscr_gold.predictions pr
  JOIN sym m       ON m.symbol = pr.symbol
  LEFT JOIN pop p0 ON p0.brand = m.brand AND p0.d = pr.prediction_date
),
fut AS (
  SELECT p.*,
    (SELECT popularity FROM pop f
      WHERE f.brand = p.brand
        AND f.d BETWEEN date_add(p.d0, 6) AND date_add(p.d0, 8)
      ORDER BY abs(datediff(f.d, date_add(p.d0, 7))) LIMIT 1) AS pop_future
  FROM p
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
