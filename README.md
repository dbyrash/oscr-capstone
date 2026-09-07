# OSCR: Streaming Content Popularity & Sentiment Pipeline

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

Everything above was RAG that answers questions. `src/04_setup_vector_index.py`
and `src/05_agent_predictions.py` are the piece that acts on them: the
agent retrieves grounded context, asks an LLM for a beat, meet, or miss call
on next quarter's subscribers, and writes that call to a new `predictions`
table. That write is the actual state change. Both notebooks are new and
untested against a live run as of this commit, so treat them as a first
pass, not verified working code.

## what I learned

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

<img width="1507" height="556" alt="Screenshot 2026-09-06 at 4 26 38 PM" src="https://github.com/user-attachments/assets/76ec32cb-b316-46d3-b064-91cd8fe28131" />
<img width="3017" height="732" alt="image" src="https://github.com/user-attachments/assets/209ba8dc-4d81-4e90-8411-f6904cfacbe5" />
<img width="3012" height="1647" alt="image" src="https://github.com/user-attachments/assets/9ccfd398-9767-4f38-9bfd-79faad0f300c" />
