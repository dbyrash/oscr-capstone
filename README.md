# OSCR (Online Streaming Churn Rating): Streaming Content Popularity & Sentiment Pipeline

## what am I trying to achieve

Track Netflix and Disney+ popularity from three angles that don't normally
talk to each other: app-store rank, Wall Street sentiment on the parent
stock, and what viewers actually write in reviews and news. The first two
are numbers you can average into a score. The third is free text, and
that's where the RAG layer comes in. Data comes from Finnhub, Financial
Modeling Prep, TMDB, and the Apple App Store RSS feed.

Runs entirely on Databricks: Unity Catalog, Delta Lake, Mosaic AI Vector
Search. No Docker, nothing to run locally end-to-end.

## Architecture

```
  STRUCTURED SIDE                           UNSTRUCTURED SIDE
  Finnhub, FMP, App Store RSS               TMDB reviews, Finnhub news
              |                                          |
              v                                          v
 +--------------------------+               +--------------------------+
 | BRONZE                   |               | BRONZE                   |
 | raw, append-only,        |               | raw review/news text,    |
 | one table per endpoint   |               | untouched                |
 +------------+-------------+               +------------+-------------+
              |                                          |
              v                                          v
 +--------------------------+               +--------------------------+
 | SILVER                   |               | SILVER                   |
 | dedupe, alias-match,     |               | dedupe, typed, cleaned   |
 | MERGE (current state)    |               | pass-through             |
 +------------+-------------+               +------------+-------------+
              |                                          |
              v                                          v
 +--------------------------+               +--------------------------+
 | GOLD                     |               | GOLD: rag_text_corpus    |
 | symbol_analyst_sentiment |               | (doc_id, text_content)   |
 | symbol_forward_estimates |               +------------+-------------+
 | app_rankings_by_brand    |                            |
 | app_rank_history         |                            v
 | show_engagement          |               +--------------------------+
 +------------+-------------+               | VECTOR SEARCH INDEX      |
              |                             | rag_text_corpus_index    |
              v                             | hybrid, Triggered sync   |
 +--------------------------+               +------------+-------------+
 | NUMERIC SIGNALS          |                            |
 | rank, sentiment score,   |                            v
 | growth, ratings          |               +--------------------------+
 +------------+-------------+               | AGENT / LLM              |
              |                             | hybrid search -> prompt  |
              |                             | -> grounded answer       |
              |                             +------------+-------------+
              +------------------------------------------+
                                   |
                                   v
           one popularity picture: what changed (numbers)
           and why (text)
```

![OSCR architecture](docs/architecture.png)

## Idempotency

Every write in this pipeline is safe to rerun. Bronze only appends, so
replaying an ingestion never duplicates a batch that already landed. Silver
uses `MERGE`: it upserts by key, so it always reflects current state no
matter how many times it runs. The one gold history table
(`app_rank_history`) deletes and reinserts a day's rows instead of blindly
appending, so backfilling a day twice doesn't double it. None of this
depends on remembering what already ran; it's just not possible to
duplicate data by construction.

## why RAG is here

The numeric gold tables already answer "what changed": rank moved, analyst
sentiment shifted, revenue grew. RAG exists purely to answer "why," which
lives in review and news text that can't be averaged into a score.
`rag_text_corpus` holds that text, a Vector Search index embeds it, and at
question time hybrid search pulls the most relevant chunks for an LLM to
read and answer from, grounded in real reviews instead of a guess. It's
additive: if the index or serving endpoint goes down, the rest of the
pipeline doesn't notice.

One thing worth knowing if you build on this: retrieval ranks results, it
doesn't filter for relevance. Asking for 3 results always returns 3, even
if only one is actually on-topic. The score gap between them matters more
than the row count.

Everything above is RAG that answers questions.
[`04_setup_vector_index.py`](src/04_setup_vector_index.py) and
[`05_agent_predictions.py`](src/05_agent_predictions.py) are written to
close that gap with an actual agent: one designed to retrieve grounded
context, ask an LLM for a beat, meet, or miss call on next quarter's
subscribers, and write that call to a new `predictions` table, a real state change the rest of the pipeline
doesn't have. Neither notebook has been run against a live workspace yet.
This is not incorporated functionality, it's code written toward that goal;
treat it as a design on paper until a row actually lands in `predictions`.

## what I learned

- Idempotency has to be designed per layer. Append, MERGE, and
  delete-then-insert are three different answers to "what if this reruns,"
  and each layer needs the one that fits it.
- RAG is for text you can't turn into a number. It doesn't replace the rest
  of the pipeline, and most of the real signal here still comes from
  structured data.
- A Databricks workspace and a git repo drift the moment you stop syncing
  them. Comparing the two turned up a stray notebook in the workspace
  writing bronze with `overwrite` instead of `append`, a quiet bug that only
  showed up when I diffed the two copies.
- Search results come back ranked, not filtered. Ask for 3 and you get 3,
  even if only one is relevant, so I check the score gap instead of
  trusting everything that comes back.

<img width="1507" height="556" alt="Screenshot 2026-09-06 at 4 26 38 PM" src="https://github.com/user-attachments/assets/76ec32cb-b316-46d3-b064-91cd8fe28131" />
<img width="3017" height="732" alt="image" src="https://github.com/user-attachments/assets/209ba8dc-4d81-4e90-8411-f6904cfacbe5" />
<img width="3012" height="1647" alt="image" src="https://github.com/user-attachments/assets/9ccfd398-9767-4f38-9bfd-79faad0f300c" />
