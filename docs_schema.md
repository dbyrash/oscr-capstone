# OSCR — Bronze Layer Schemas

Grounded in real API responses pulled and saved under `exploration/raw_data/` (not guessed, not inferred). Replaces the earlier draft, which was written before any live data existed.

## Conventions

- **One table per source endpoint, not per symbol.** `NFLX` and `DIS` rows live in the same table, distinguished by a `symbol` column — never a table-per-ticker.
- **Bronze mirrors the source verbatim.** Field names, casing, and nesting match the API response exactly. No renaming, no flattening, no filtering.
- **Only pipeline metadata gets added**, always prefixed with `_` so it's obviously not from the source: `_ingested_at` (timestamp, every table) plus whatever request context isn't already in the payload (`_symbol_requested`, `_storefront`, etc.).
- Types below are PySpark `StructType` types, since that's what you'll hand to `spark.read.schema(...)` when you load these.

---

## 1. `bronze_finnhub_company_news`
Source: Finnhub `/company-news` · Grain: one row per article per symbol per pull

| column | type | notes |
|---|---|---|
| category | string | |
| datetime | long | unix seconds — cast to timestamp in silver |
| headline | string | |
| id | long | Finnhub's article id |
| image | string | often empty string, not null |
| related | string | ticker(s), comma-separated |
| source | string | e.g. "SeekingAlpha" |
| summary | string | often empty on free tier |
| url | string | |
| _symbol_requested | string | metadata |
| _ingested_at | timestamp | metadata |

## 2. `bronze_finnhub_recommendation_trends`
Source: Finnhub `/stock/recommendation` · Grain: one row per symbol per month

| column | type | notes |
|---|---|---|
| symbol | string | |
| period | string | `YYYY-MM-01`, monthly snapshot |
| strongBuy | long | |
| buy | long | |
| hold | long | |
| sell | long | |
| strongSell | long | |
| _ingested_at | timestamp | metadata |

**Reference PySpark schema** (smallest table — use this as the pattern for the rest):
```python
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

bronze_finnhub_recommendation_trends_schema = StructType([
    StructField("symbol", StringType()),
    StructField("period", StringType()),
    StructField("strongBuy", LongType()),
    StructField("buy", LongType()),
    StructField("hold", LongType()),
    StructField("sell", LongType()),
    StructField("strongSell", LongType()),
    StructField("_ingested_at", TimestampType()),
])
```

## 3. `bronze_fmp_analyst_estimates`
Source: FMP `/analyst-estimates` · Grain: one row per symbol per forecast year (~10 years out)

| column | type | notes |
|---|---|---|
| symbol | string | |
| date | string | `YYYY-MM-DD`, forecast year |
| revenueLow / revenueHigh / revenueAvg | double | |
| ebitdaLow / ebitdaHigh / ebitdaAvg | double | |
| ebitLow / ebitHigh / ebitAvg | double | |
| netIncomeLow / netIncomeHigh / netIncomeAvg | double | |
| sgaExpenseLow / sgaExpenseHigh / sgaExpenseAvg | double | |
| epsAvg / epsHigh / epsLow | double | this is what replaces Finnhub's blocked eps-estimate endpoint |
| numAnalystsRevenue | long | |
| numAnalystsEps | long | |
| _ingested_at | timestamp | metadata |

## 4. `bronze_tmdb_reviews`
Source: TMDB `/search/tv` → `/tv/{id}/reviews` · Grain: one row per review per show

| column | type | notes |
|---|---|---|
| id | string | TMDB review ids are hash strings, not ints |
| author | string | |
| author_details | struct\<name: string, username: string, avatar_path: string, rating: double\> | `rating` nullable — most reviewers skip it |
| content | string | the actual review text — this is what feeds the RAG step |
| created_at | string | ISO8601 |
| updated_at | string | ISO8601 |
| url | string | |
| _show_title_requested | string | metadata, e.g. "Wednesday" |
| _show_id | long | metadata, TMDB's internal id from the search step |
| _ingested_at | timestamp | metadata |

## 5. `bronze_tmdb_watch_providers`
Source: TMDB `/watch/providers/tv` · Grain: one row per streaming provider (reference/lookup table — 272 rows, not symbol-specific)

| column | type | notes |
|---|---|---|
| provider_id | long | **this is how we resolve Netflix=8, Disney Plus=337 live** — never hardcode these |
| provider_name | string | verified real value: "Disney Plus", not "Disney+" |
| logo_path | string | |
| display_priority | long | |
| display_priorities | map\<string, long\> | per-country priority ranking |
| _ingested_at | timestamp | metadata |

Checked this table directly: no "Hotstar" or "JioHotstar" entry exists under any name — TMDB just doesn't track that brand split. Confirms the India-market gotcha is real and has to be handled with a manually maintained alias list, not something a lookup table hands you for free.

## 6. `bronze_appstore_top_free`
Source: Apple `top-free/100` per storefront · Grain: one row per app per storefront per pull

| column | type | notes |
|---|---|---|
| artistName | string | publisher, e.g. "Netflix, Inc." |
| id | string | Apple's app id |
| name | string | app display name |
| releaseDate | string | |
| kind | string | |
| artworkUrl100 | string | |
| genres | array\<string\> | |
| url | string | |
| _storefront | string | "us" / "de" / "in" |
| _rank | long | 1–100, computed from array position — not a source field |
| _ingested_at | timestamp | metadata |

**No brand-matching here on purpose.** Bronze lands all 100 apps untouched, false positives ("Netflix Game Controller") and all. Deciding which rows represent Netflix/Disney — including the Hotstar/JioHotstar alias problem from today — is business logic, so it belongs in silver as an explicit alias-mapping step, not baked into the fetch.

## 7. `bronze_finnhub_trades` (streaming)
Source: Finnhub WebSocket `type: "trade"` messages · Grain: one row per trade tick

| column | type | notes |
|---|---|---|
| s | string | symbol — Finnhub's real field name, kept as-is |
| p | double | price |
| t | long | epoch **milliseconds** (not seconds — different from the REST endpoints above) |
| v | double | volume |
| c | array\<string\> | trade condition codes, nullable |
| _ingested_at | timestamp | metadata |

Not yet live-verified — the Custom Data Source scaffold hasn't been run inside an actual Databricks notebook yet. Field names come from Finnhub's documented WS contract; treat as provisional until we confirm against a real message.

## 8. `bronze_polygon_aggs` — TODO

Not defined yet. We haven't pulled real Polygon data (5 calls/day quota, deliberately saved), and after today's lesson — schemas come from real samples, not guesses — this one stays open until that first real call happens.

---

## Next
Silver layer is where the Hotstar/JioHotstar alias resolution and the Netflix/Disney name-matching actually happen. RAG-based sentiment extraction over `bronze_tmdb_reviews.content` (and eventually Finnhub news `summary`) comes after bronze + silver are loaded and proven — not before.
