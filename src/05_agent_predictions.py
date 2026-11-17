# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-vectorsearch",
# ]
# ///
# The agent: retrieves grounded context from rag_text_corpus_index,
# combines it with the structured gold signals, asks an LLM for a concrete
# rise/hold/fall call on a brand's near-term app-store popularity, and writes
# that call as a row to bootcamp_students.oscr_gold.predictions. This is the
# one place in the pipeline where an LLM's judgment produces a real state
# change, not just a descriptive aggregate.
#
# IDEMPOTENT + BACKFILLABLE. Each run targets one "as of" date (default today)
# and writes with delete-then-insert on that date, so re-running a date never
# duplicates it -- the same pattern gold.app_rank_history uses. Set the
# backfill_start widget to replay the agent weekly across history, which fills
# the prediction_scorecard with real hit-rates today instead of a week from now.
#
# Backfill honesty: popularity signals are as-of correct (snapshot_date <=
# as_of_date). Analyst sentiment, forward estimates, and the retrieved
# review/news text use the latest available snapshot -- a known backtest
# limitation, documented rather than hidden.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from datetime import date, datetime, timedelta

import mlflow.deployments
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import current_timestamp

CATALOG = "bootcamp_students"
GOLD = f"{CATALOG}.oscr_gold"
ENDPOINT_NAME = "oscr-rag-endpoint"
INDEX_NAME = f"{GOLD}.rag_text_corpus_index"
PREDICTIONS_TABLE = f"{GOLD}.predictions"

# Confirm the exact serving endpoint name in your workspace under Serving in
# the sidebar; Foundation Model API endpoint names vary by workspace/region.
LLM_ENDPOINT = "databricks-meta-llama-3-1-8b-instruct"

SYMBOLS = ["NFLX", "DIS"]
# Disney's apps all roll up to one "Disney+" brand, matching the dashboard.
BRAND_OF = {"NFLX": "Netflix", "DIS": "Disney+"}
NAMES_OF = {
    "Netflix": ["Netflix"],
    "Disney+": ["Disney+", "Disney Plus", "Hotstar", "JioHotstar"],
}

vsc = VectorSearchClient()
index = vsc.get_index(ENDPOINT_NAME, INDEX_NAME)
llm = mlflow.deployments.get_deploy_client("databricks")

# COMMAND ----------

# Parameters. The scheduled job passes none, so it runs a single idempotent
# prediction for today. For a historical backtest, set backfill_start.
dbutils.widgets.text("as_of_date", date.today().isoformat())
dbutils.widgets.text("backfill_start", "")          # e.g. 2026-08-25 (empty = just as_of_date)
dbutils.widgets.text("backfill_step_days", "7")     # weekly, to match the 7-day scoring horizon

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PREDICTIONS_TABLE} (
  symbol STRING,
  prediction_date DATE,
  call STRING,
  confidence STRING,
  reasoning STRING,
  retrieved_doc_ids ARRAY<STRING>,
  model_endpoint STRING,
  created_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

def retrieve_context(symbol, num_results=5):
    results = index.similarity_search(
        query_text=f"viewer and analyst sentiment toward {symbol}",
        columns=["doc_id", "text_content"],
        num_results=num_results,
        query_type="HYBRID",
    )
    cols = [c["name"] for c in results["manifest"]["columns"]]
    rows = results["result"]["data_array"]
    return [dict(zip(cols, row)) for row in rows]


def structured_signals(symbol, as_of):
    brand = BRAND_OF.get(symbol, symbol)
    names = NAMES_OF.get(brand, [brand])
    in_list = ", ".join(f"'{n}'" for n in names)
    # Popularity is as-of correct: only snapshots on or before as_of, newest 7.
    popularity = spark.sql(f"""
        SELECT snapshot_date, ROUND(AVG(101 - rank), 1) AS popularity_score
        FROM {GOLD}.app_rank_history
        WHERE name IN ({in_list})
          AND snapshot_date <= DATE('{as_of}')
        GROUP BY snapshot_date
        ORDER BY snapshot_date DESC
        LIMIT 7
    """).collect()
    # Sentiment / estimates use latest available snapshot (documented caveat).
    sentiment = spark.sql(f"""
        SELECT * FROM {GOLD}.symbol_analyst_sentiment
        WHERE symbol = '{symbol}' ORDER BY period DESC LIMIT 1
    """).collect()
    estimates = spark.sql(f"""
        SELECT * FROM {GOLD}.symbol_forward_estimates
        WHERE symbol = '{symbol}' ORDER BY date DESC LIMIT 1
    """).collect()
    return {
        "brand": brand,
        "as_of_date": str(as_of),
        "recent_popularity": [r.asDict() for r in popularity],
        "sentiment": sentiment[0].asDict() if sentiment else None,
        "estimates": estimates[0].asDict() if estimates else None,
    }


def build_prompt(symbol, signals, chunks):
    brand = BRAND_OF.get(symbol, symbol)
    context_text = "\n\n".join(
        f"[{c['doc_id']}] {c['text_content'][:500]}" for c in chunks
    )
    return f"""You are assessing the near-term popularity trajectory of {brand}
on streaming, using only the signals below. Call whether {brand}'s app-store
popularity is likely to RISE, HOLD, or FALL next period, and explain why.

Structured signals (recent app-store popularity trend, analyst sentiment,
revenue/EPS estimates):
{json.dumps(signals, default=str, indent=2)}

Retrieved viewer reviews and news:
{context_text}

Ground your explanation in the retrieved text where you can. Respond with
strict JSON only, no other text:
{{"call": "rise" | "hold" | "fall", "confidence": "low" | "medium" | "high", "reasoning": "one or two sentences, citing the specific signals and retrieved text that drove the call"}}
"""

# COMMAND ----------

def call_llm(prompt):
    response = llm.predict(
        endpoint=LLM_ENDPOINT,
        inputs={"messages": [{"role": "user", "content": prompt}], "max_tokens": 300},
    )
    return response["choices"][0]["message"]["content"]


def parse_call(raw_text):
    try:
        return json.loads(raw_text)
    except (json.JSONDecodeError, TypeError):
        return {"call": "unknown", "confidence": "low", "reasoning": raw_text}

# COMMAND ----------

def run_for_date(as_of):
    """Generate one prediction per symbol for a single as-of date and write
    them idempotently: clear that date's rows, then insert. Re-running a date
    replaces it rather than duplicating."""
    rows = []
    for symbol in SYMBOLS:
        chunks = retrieve_context(symbol)
        signals = structured_signals(symbol, as_of)
        parsed = parse_call(call_llm(build_prompt(symbol, signals, chunks)))
        rows.append({
            "symbol": symbol,
            "prediction_date": as_of,
            "call": parsed.get("call", "unknown"),
            "confidence": parsed.get("confidence", "low"),
            "reasoning": parsed.get("reasoning", ""),
            "retrieved_doc_ids": [c["doc_id"] for c in chunks],
            "model_endpoint": LLM_ENDPOINT,
        })
        print(f"{as_of} {symbol}: {parsed.get('call')} ({parsed.get('confidence')})")

    df = (spark.createDataFrame(rows)
          .withColumn("prediction_date", spark.createDataFrame(rows).prediction_date.cast("date"))
          .withColumn("created_at", current_timestamp()))
    df = df.withColumn("prediction_date", df["prediction_date"].cast("date"))
    spark.sql(f"DELETE FROM {PREDICTIONS_TABLE} WHERE prediction_date = DATE('{as_of}')")
    df.write.format("delta").mode("append").saveAsTable(PREDICTIONS_TABLE)


# Build the list of dates to run: a single as-of date, or a weekly backfill.
as_of_date = dbutils.widgets.get("as_of_date") or date.today().isoformat()
backfill_start = dbutils.widgets.get("backfill_start").strip()
step = int(dbutils.widgets.get("backfill_step_days") or "7")

end = datetime.fromisoformat(as_of_date).date()
if backfill_start:
    d, dates = datetime.fromisoformat(backfill_start).date(), []
    while d <= end:
        dates.append(d.isoformat())
        d += timedelta(days=step)
else:
    dates = [end.isoformat()]

print(f"Running agent for {len(dates)} date(s): {dates}")
for ds in dates:
    run_for_date(ds)

spark.table(PREDICTIONS_TABLE).orderBy("created_at", ascending=False).show(truncate=False)
