# OSCR: Streaming Content Popularity & Sentiment Pipeline

## What this is trying to achieve

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
                                     changed (numbers) and why (text)
```

## Idempotency

Every write in this pipeline is safe to rerun. Bronze only appends, so
replaying an ingestion never duplicates a batch that already landed. Silver
uses `MERGE`: it upserts by key, so it always reflects current state no
matter how many times it runs. The one gold history table
(`app_rank_history`) deletes and reinserts a day's rows instead of blindly
appending, so backfilling a day twice doesn't double it. None of this
depends on remembering what already ran; it's just not possible to
duplicate data by construction.

## Why RAG is here

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

## What I learned

- Idempotency has to be designed per layer, not bolted on. Append, MERGE,
  and delete-then-insert are three different answers to "what happens if
  this reruns," each fitted to what that layer actually needs.
- RAG is a narrow tool. It's for text you can't reduce to a number, not a
  replacement for the rest of the pipeline; most of the real signal here
  still comes from plain structured data.
- A Databricks workspace and a git repo drift the moment you're not
  deliberate about syncing them. Comparing the two turned up a stray
  notebook in the workspace writing bronze with `overwrite` instead of
  `append`, a quiet bug that only surfaces when you actually diff the two
  copies against each other.
- A retrieved chunk isn't automatically a relevant one. The ranking score
  is the thing to check, not the fact that something came back at all.
