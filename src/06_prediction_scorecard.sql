-- src/06_prediction_scorecard.sql
-- Self-grading backtest: did each agent call come true one week later?
--
-- Each prediction (rise/hold/fall) is checked against what the brand's
-- app-store popularity (101 - rank) actually did ~7 days later, then
-- measured against the naive "always hold" baseline. Beating that baseline
-- is what turns the agent from a demo into a result.
--
-- NOTE: predictions store a ticker (NFLX/DIS); rankings store a brand name
-- (Netflix/Disney+). The `map` CTE bridges them; there is no linking table.
--
-- This reads 0 scored until a week of daily snapshots accrues, then fills
-- automatically once the scheduled pipeline runs each day. That is the point:
-- the agent grades itself over time instead of being taken on trust.

CREATE OR REPLACE TABLE bootcamp_students.oscr_gold.prediction_scorecard AS
WITH map AS (
  SELECT 'NFLX' AS symbol, 'Netflix' AS brand UNION ALL
  SELECT 'DIS'  AS symbol, 'Disney+' AS brand
),
pop AS (
  SELECT brand, snapshot_date AS d,
         ROUND(AVG(101 - _rank), 1) AS popularity
  FROM bootcamp_students.oscr_gold.app_rankings_by_brand
  GROUP BY brand, snapshot_date
),
p AS (
  SELECT m.brand, pr.prediction_date AS d0, pr.call, p0.popularity AS pop_then
  FROM bootcamp_students.oscr_gold.predictions pr
  JOIN map m       ON m.symbol = pr.symbol
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
-- SELECT ROUND(AVG(hit), 2)                                               AS agent_hit_rate,
--        ROUND(AVG(CASE WHEN actual_move = 'hold' THEN 1 ELSE 0 END), 2)  AS always_hold_baseline,
--        COUNT(*)                                                         AS scored_predictions
-- FROM bootcamp_students.oscr_gold.prediction_scorecard WHERE hit IS NOT NULL;
