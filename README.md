# OSCR — Streaming Content Popularity & Sentiment Pipeline

A Databricks-native data pipeline that tracks Netflix and Disney+ popularity from
three angles that don't normally talk to each other: app-store rank, Wall Street
sentiment on the parent stock, and what viewers actually write in reviews and
news coverage. That last one is unstructured text, so a retrieval-augmented
generation (RAG) layer sits on top of the pipeline to let an LLM read it and
answer questions in plain English instead of guessing.

There is no Docker image and nothing to run locally end-to-end — this runs
entirely on Databricks (Unity Catalog + Delta Lake + Mosaic AI Vector Search).
Local Python is only used to pull raw API data before it lands in the
workspace. Setup below reflects that.

## Architecture

```
                         STRUCTURED SIDE                                    UNSTRUCTURED SIDE
                         ----------------                                   ------------------
  APIs                Finnhub, FMP,                                       TMDB reviews,
                       Apple App Store RSS                                 Finnhub news
    |                        |                                                  |
    v                        v                                                  v
+--------+          +------------------+                              +------------------+
| BRONZE |  ----->  | raw, append-only |  ----------------------------| raw review/news  |
+--------+          | one table per    |                              | text, untouched  |
                     | source endpoint  |                              +------------------+
                     +--------+---------+                                        |
                              |                                                   |
                              v                                                   v
                     +------------------+                              +------------------+
                     |     SILVER       |                              |     SILVER       |
                     | dedupe, alias-   |                              | dedupe, typed,   |
                     | match, MERGE     |                              | cleaned pass-    |
                     | (current state)  |                              | through          |
                     +--------+---------+                              +--------+---------+
                              |                                                  |
                              v                                                  v
      +--------------------------------------------+              +---------------------------+
      |                   GOLD                      |              |   GOLD: rag_text_corpus   |
      |  symbol_analyst_sentiment                    |              |   (doc_id, text_content)  |
      |  symbol_forward_estimates                     |             +-------------+-------------+
      |  app_rankings_by_brand                        |                           |
      |  app_rank_history  <- time series             |                           v
      |  show_engagement                              |             +---------------------------+
      +--------------------+-------------------------+              |  Vector Search index      |
                            |                                        |  rag_text_corpus_index    |
                            |                                        |  (hybrid, embeds on       |
                            |                                        |   Triggered sync)         |
                            |                                        +-------------+-------------+
                            |                                                      |
                            v                                                      v
                  numeric popularity signals                        question -> hybrid search ->
                  (rank, sentiment score,                            augmented prompt -> LLM ->
                  revenue growth, ratings)                            grounded answer, e.g. via
                                                                       Databricks Playground/Agent
                            \_______________________  ______________________/
                                                    \/
                                     one popularity picture: what
                                     changed (numbers) + why (text)
```

Every bronze/silver/gold write is idempotent (silver uses `MERGE`, the gold
history table uses delete-then-insert per day, bronze is append-only by
design) so reruns never duplicate data. A `data_quality_checks.py` gate runs
after each layer and asserts row counts, null constraints, and key
uniqueness before the next layer is allowed to run.

## What's version-controlled here vs. what's in the Databricks workspace

Being upfront about this because it matters if you're reviewing the repo:

| Layer | Where it lives |
|---|---|
| API pull script (`src/00_fetch_to_volume.py`) | This repo, `src/` |
| Bronze ingestion (`src/01_bronze_ingest.ipynb`) | This repo, `src/` |
| Silver transforms (`src/02_silver_transform.py`) | This repo, `src/` |
| Gold transforms (`src/03_gold_transform.py`) | This repo, `src/` |
| Data quality gate (`src/data_quality_checks.py`) | This repo, `src/` |
| `app_rank_history` DDL (`src/ddl_app_rank_history.py`) | This repo, `src/` |
| Job orchestration and the Vector Search index config (`rag_text_corpus_index`) | Databricks workspace only — a scheduled Job definition and a UI-configured search index aren't files that export cleanly to git |

Silver and gold were originally built and iterated on directly in a
Databricks Git folder before being pulled into this repo (via Databricks'
notebook "Download as > Zip - Source" export) — worth knowing if you ever
see two copies of a notebook drift: the Databricks workspace is where things
get authored and run, this repo is where they get reviewed and versioned.
One thing to clean up in the workspace as a result: a stray
`src/01_bronze_ingest.py` there writes bronze tables with `mode("overwrite")`,
which contradicts the append-only bronze design below — delete it in favor
of the notebook actually committed here.

## Data sources

| Source | What it feeds |
|---|---|
| Finnhub `/stock/recommendation` | Analyst buy/hold/sell counts -> `symbol_analyst_sentiment` |
| Finnhub `/company-news` | News volume + RAG text corpus |
| Financial Modeling Prep `/analyst-estimates` | Revenue/EPS growth -> `symbol_forward_estimates` |
| TMDB `/tv/{id}/reviews` | Review text -> `show_engagement` + RAG text corpus |
| TMDB `/watch/providers/tv` | Netflix/Disney+ provider ID resolution |
| Apple App Store `top-free/100` RSS | App rank -> `app_rankings_by_brand`, `app_rank_history` |

Full bronze schemas (grounded in real captured API responses, not assumed)
are in `docs_schema.md`.

## Setup — Databricks

This assumes a Unity Catalog-enabled workspace and a catalog named
`bootcamp_students` (rename throughout if yours differs).

**1. Create the catalog structure**

```sql
CREATE SCHEMA IF NOT EXISTS bootcamp_students.oscr_bronze;
CREATE SCHEMA IF NOT EXISTS bootcamp_students.oscr_silver;
CREATE SCHEMA IF NOT EXISTS bootcamp_students.oscr_gold;
CREATE VOLUME IF NOT EXISTS bootcamp_students.oscr_bronze.raw_landing;
```

**2. Connect this repo as a Databricks Repo**

Workspace -> Repos -> Add Repo -> paste this repo's URL. Editing
`src/01_bronze_ingest.ipynb` directly in Databricks keeps it runnable on a
cluster/serverless SQL warehouse with Spark already available.

**3. Set API secrets**

Copy `.env.example` (or create `.env`) with:

```
POLYGON_API_KEY=...
TMDB_API_KEY=...
FINNHUB_API_KEY=...
FMP_API_KEY=...
```

`.env` is gitignored on purpose — never commit real keys. For a shared
workspace, prefer Databricks Secrets over a `.env` file:

```
databricks secrets create-scope oscr
databricks secrets put-secret oscr finnhub_api_key
```

**4. Pull raw data into the landing volume**

Run `src/00_fetch_to_volume.py` (from a notebook cell or `%run`) to hit the
Finnhub, TMDB, and FMP APIs and write raw JSON to
`/Volumes/bootcamp_students/oscr_bronze/raw_landing/raw_data/`. This is the
only step that talks to the public internet; everything after this reads
from the volume.

**5. Run bronze ingestion**

Run `src/01_bronze_ingest.ipynb` top to bottom. Each cell reads one source's
JSON with an explicit schema (no inference) and appends into
`bootcamp_students.oscr_bronze.<table>`.

**6. Run silver + gold**

Run `src/02_silver_transform.py` (alias resolution, MERGE upserts, typing),
then `src/03_gold_transform.py` (aggregations + the `rag_text_corpus` build).
`src/ddl_app_rank_history.py` creates the one gold table gold_transform
expects to already exist before it appends to it. See
`oscr-capstone-data-architecture.md` for exactly what each gold table
computes.

**7. Run the data quality gate after each layer**

```
python src/data_quality_checks.py --layer bronze
python src/data_quality_checks.py --layer silver
python src/data_quality_checks.py --layer gold
```

Each raises an `AssertionError` (row counts, null checks, duplicate-key
checks) if that layer isn't fit for the next one to build on.

**8. Stand up the RAG add-on**

From Catalog Explorer, open `bootcamp_students.oscr_gold.rag_text_corpus`
(columns: `doc_id`, `text_content`) and click Create -> Vector Search Index.
Use hybrid search (semantic + keyword), Triggered sync, and a
Databricks-hosted embedding model (this project uses
`databricks-qwen3-embedding-0-6b`). Once the index shows `Online`, either:

- Click **Query index** on the index's page to test hybrid search directly, or
- Click **Try in Playground** to attach the index as a retrieval tool to a
  chat model and hold an actual grounded conversation over the corpus.

Use a model that supports tool-calling on `/v1/chat/completions` (most
standard chat models; some reasoning-tier models require `reasoning_effort`
set to `none` or the `/v1/responses` API instead).

## Engineering notes worth knowing before you dig in

- **Idempotency everywhere**: reruns are safe by design, not by luck — silver
  MERGEs, the gold history table deletes-and-reinserts per day, bronze just
  appends.
- **RAG is additive, not load-bearing**: the numeric gold tables answer "what
  changed"; RAG only exists to answer "why," over data (reviews, news text)
  that can't be averaged into a score. If the index or serving endpoint is
  down, the rest of the pipeline is unaffected.
- **Retrieval ranks, it doesn't filter.** A `num_results: 3` query always
  returns 3 rows even if only one is truly relevant — verified directly
  against this index, where the correct match scored ~5x higher than the two
  off-topic ones. Don't assume every retrieved chunk is relevant; check the
  score gap.
- **Known gaps**: `bronze_finnhub_trades` (streaming) is scaffolded but not
  yet live-verified against a real WebSocket message. `bronze_polygon_aggs`
  is not implemented — deliberately, until a real sample response justifies
  the schema. Job orchestration and the vector index are configured directly
  in the Databricks workspace and aren't files this repo can track (see
  table above).
