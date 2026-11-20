-- Scores each prediction against brand_popularity horizon_days later, using the frozen cutoffs in move_cutoffs.

-- One row per prediction. actual_move and hit stay NULL until the future snapshot exists.
CREATE OR REPLACE TABLE bootcamp_students.oscr_gold.prediction_scorecard AS
WITH c AS (SELECT * FROM bootcamp_students.oscr_gold.move_cutoffs),
sym AS (
  SELECT 'NFLX' AS symbol, 'Netflix' AS brand UNION ALL
  SELECT 'DIS', 'Disney+'
),
pop AS (SELECT * FROM bootcamp_students.oscr_gold.brand_popularity),
scored AS (
  SELECT pr.symbol, m.brand, pr.prediction_date, pr.source, pr.variant, pr.call,
    p0.popularity AS pop_then, pf.popularity AS pop_future,
    pf.popularity - p0.popularity AS delta,
    p0.popularity - pb.popularity AS prior_delta,
    c.fall_below, c.rise_above
  FROM bootcamp_students.oscr_gold.predictions pr
  JOIN sym m USING (symbol)
  CROSS JOIN c
  LEFT JOIN pop p0 ON p0.brand = m.brand AND p0.d = pr.prediction_date
  LEFT JOIN pop pf ON pf.brand = m.brand AND pf.d = date_add(pr.prediction_date, c.horizon_days)
  LEFT JOIN pop pb ON pb.brand = m.brand AND pb.d = date_sub(pr.prediction_date, c.horizon_days)
),
classed AS (
  SELECT *,
    CASE WHEN delta IS NULL THEN NULL WHEN delta > rise_above THEN 'rise'
         WHEN delta < fall_below THEN 'fall' ELSE 'hold' END AS actual_move,
    -- Persistence baseline: the move over the previous window repeats.
    CASE WHEN prior_delta IS NULL THEN NULL WHEN prior_delta > rise_above THEN 'rise'
         WHEN prior_delta < fall_below THEN 'fall' ELSE 'hold' END AS prior_move
  FROM scored
)
SELECT symbol, brand, prediction_date, source, variant, call, pop_then, pop_future, delta,
  actual_move, prior_move,
  CASE WHEN actual_move IS NULL THEN NULL WHEN call = actual_move THEN 1 ELSE 0 END AS hit
FROM classed;

-- One row per source x model. Baselines are graded on the same scored brand-dates as the agent.
CREATE OR REPLACE TABLE bootcamp_students.oscr_gold.prediction_scorecard_summary AS
WITH s AS (SELECT * FROM bootcamp_students.oscr_gold.prediction_scorecard WHERE actual_move IS NOT NULL),
keys AS (SELECT DISTINCT source, brand, prediction_date, actual_move, prior_move FROM s),
graded AS (
  SELECT source, concat('agent_', variant) AS model, hit FROM s
  UNION ALL SELECT source, 'always_hold', CAST(actual_move = 'hold' AS INT) FROM keys
  UNION ALL SELECT source, 'always_rise', CAST(actual_move = 'rise' AS INT) FROM keys
  UNION ALL SELECT source, 'persistence', CAST(actual_move = prior_move AS INT) FROM keys WHERE prior_move IS NOT NULL
  -- Seeded hash, so the random baseline is uniform over the 3 calls yet identical on every rerun.
  UNION ALL SELECT source, 'random',
    CAST(actual_move = element_at(array('rise', 'hold', 'fall'),
         CAST(pmod(xxhash64(brand, prediction_date, 'oscr'), 3) AS INT) + 1) AS INT) FROM keys
),
grid AS (
  SELECT source, model
  FROM VALUES ('backfill'), ('live') AS t(source)
  CROSS JOIN VALUES ('agent_rag'), ('agent_no_rag'), ('agent_no_rag_big'), ('always_hold'), ('always_rise'), ('persistence'), ('random') AS m(model)
),
agg AS (SELECT source, model, COUNT(*) AS n, SUM(hit) AS hits FROM graded GROUP BY 1, 2),
skips AS (
  SELECT source, concat('agent_', variant) AS model, COUNT(*) AS skipped
  FROM bootcamp_students.oscr_gold.prediction_skips GROUP BY 1, 2
),
w AS (
  SELECT g.source, g.model, coalesce(a.n, 0) AS n, coalesce(a.hits, 0) AS hits,
    a.n AS nn, a.hits / a.n AS p, 1.96 AS z,
    CASE WHEN g.model LIKE 'agent_%' THEN coalesce(k.skipped, 0) END AS skipped
  FROM grid g
  LEFT JOIN agg a USING (source, model)
  LEFT JOIN skips k USING (source, model)
)
SELECT source, model, n, hits, ROUND(p, 3) AS hit_rate,
  -- Wilson score interval, 95%; nn is NULL when n = 0, so empty cells stay NULL instead of dividing by zero.
  ROUND((p + z * z / (2 * nn) - z * sqrt(p * (1 - p) / nn + z * z / (4 * nn * nn))) / (1 + z * z / nn), 3) AS wilson_lo,
  ROUND((p + z * z / (2 * nn) + z * sqrt(p * (1 - p) / nn + z * z / (4 * nn * nn))) / (1 + z * z / nn), 3) AS wilson_hi,
  skipped
FROM w;
