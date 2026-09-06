# OSCR Capstone — Data Architecture Summary

## Approach in one paragraph

The pipeline follows a medallion architecture: bronze holds raw, append-only API pulls exactly as received (each row stamped with `_ingested_at` so every historical ingestion is preserved); silver cleans, deduplicates, and enriches that raw data into a queryable current state; gold aggregates silver into business-ready tables and one growing history fact table. A single Databricks Job orchestrates all three layers plus a data quality gate after each layer, triggered automatically when new raw files land in the bronze volume.

## Bronze layer — raw ingestion

| Table | Source | Raw shape | Load pattern |
|---|---|---|---|
| `finnhub_recommendation_trends` | Finnhub API | symbol, period, strongBuy/buy/hold/sell/strongSell counts | Append, `_ingested_at` timestamp |
| `finnhub_company_news` | Finnhub API | category, datetime, headline, id, related symbol, source, summary, url | Append (table reset on schema change) |
| `fmp_analyst_estimates` | Financial Modeling Prep API | symbol, date, revenue/EBITDA/EBIT/net income/EPS low-high-avg, analyst counts | Append |
| `tmdb_reviews` | TMDB API | show id, review author + nested rating, content, timestamps | Append, exploded from paginated results |
| `tmdb_trending_shows` | TMDB API | show_id, name, platform (registry built during exploration) | Append |
| `tmdb_watch_providers` | TMDB API | provider name/id/logo per platform | Append, exploded from nested provider list |
| `appstore_top_free` | Apple App Store RSS feed | storefront, rank (via `posexplode`), app id/name/artist/kind/artwork | Append, `_rank` + `_storefront` derived columns |

All bronze tables are intentionally append-only — every ingestion run adds a new batch rather than overwriting, which is what makes bronze the source of truth for "what did we actually receive and when."

## Silver layer — cleaned, deduplicated, current state

Silver's job is to turn bronze's raw historical log into one trustworthy row per real-world entity. The fully-worked example is `appstore_top_free`:
- Dedupe each day's bronze batch to one row per `(id, _storefront)`, keeping the most recent `_ingested_at` (handles the case where the ingest job runs more than once a day)
- Join a `brand_aliases` mapping table to normalize regional app name variants (e.g., "Hotstar," "JioHotstar," "Disney Plus" → canonical "Disney+")
- Write via `MERGE` (upsert: update matched apps, insert new ones) rather than overwrite, so silver always reflects current state and is idempotent regardless of how many times the job reruns

The remaining silver tables (`finnhub_recommendation_trends`, `finnhub_company_news`, `fmp_analyst_estimates`, `tmdb_reviews`, `tmdb_trending_shows`) apply the same underlying principle — typed, cleaned, deduplicated pass-throughs of their bronze counterparts — feeding directly into the gold aggregations below.

## Gold layer — business-ready outputs

| Table | Built from | What it computes | Load pattern |
|---|---|---|---|
| `symbol_analyst_sentiment` | silver `finnhub_recommendation_trends` + `finnhub_company_news` | Weighted mean analyst recommendation score per symbol/period, joined with monthly news article counts | Overwrite |
| `symbol_forward_estimates` | silver `fmp_analyst_estimates` | YoY revenue and EPS growth % per symbol, via window function (`lag` over prior period) | Overwrite |
| `app_rankings_by_brand` | silver `appstore_top_free` | Branded (Netflix/Disney+) app rank by storefront, most recent snapshot | Overwrite |
| `show_engagement` | silver `tmdb_reviews` + `tmdb_trending_shows` | Review count and average rating per show, joined with platform metadata | Overwrite |
| `app_rank_history` | bronze `appstore_top_free` (all apps, not just branded) | Daily periodic-snapshot fact table — one row per app/storefront/day — for tracking rank movement over time | Append, idempotent (delete + insert per day) |

## Engineering practices worth mentioning to interviewers

- **Idempotency**: every write in the pipeline is safe to rerun. Silver uses MERGE (upsert); the gold history table uses delete-then-insert scoped to the current day, so retries or reruns never duplicate data.
- **Data quality gates**: a dedicated `data_quality_check.py` runs after each layer (bronze/silver/gold) as its own job task, asserting row counts, no-null constraints, and no-duplicate-key constraints before downstream tasks are allowed to run.
- **Orchestration**: one Databricks Job wires `ingest → bronze → DQ → silver → DQ → gold → DQ` as a single DAG, triggered on new file arrival in the raw landing volume — no manual intervention needed for the daily run.
- **Historical vs. current-state design**: a deliberate split between silver (current state, overwritten/merged) and the gold history table (append-only, grows over time) — chosen specifically to support both "what's true right now" and "how has this changed" questions from the same underlying data.
