# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-vectorsearch",
# ]
# ///
# DBTITLE 1,Agent overview
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
# No look-ahead: popularity, sentiment, estimates and retrieved text are all
# limited to rows dated on or before as_of_date.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports and constants
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
BRAND_OF = {"NFLX": "Netflix", "DIS": "Disney+"}
NAMES_OF = {
    "Netflix": ["Netflix"],
    "Disney+": ["Disney+"],  # flagship only; JioHotstar is a separate India product
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

# DBTITLE 1,Retrieval, signals and prompt
def retrieve_context(symbol, as_of, num_results=5):
    results = index.similarity_search(
        query_text=f"viewer and analyst sentiment toward {symbol}",
        columns=["doc_id", "text_content"],
        num_results=num_results,
        query_type="HYBRID",
        filters={"published_date <=": str(as_of)},
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
    sentiment = spark.sql(f"""
        SELECT * FROM {GOLD}.symbol_analyst_sentiment
        WHERE symbol = '{symbol}' AND to_date(period) <= DATE('{as_of}')
        ORDER BY period DESC LIMIT 1
    """).collect()
    # Estimates carry no publish date, so ingestion time is the as-of guard.
    estimates = spark.sql(f"""
        SELECT * FROM {GOLD}.symbol_forward_estimates
        WHERE symbol = '{symbol}' AND _ingested_at < date_add(DATE('{as_of}'), 1)
        ORDER BY date DESC LIMIT 1
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

# DBTITLE 1,LLM call and parsing
import re


def call_llm(prompt):
    response = llm.predict(
        endpoint=LLM_ENDPOINT,
        inputs={"messages": [{"role": "user", "content": prompt}], "max_tokens": 300},
    )
    return response["choices"][0]["message"]["content"]


VALID_CALLS = {"rise", "hold", "fall"}


def parse_call(raw_text):
    """Return the first {...} JSON block with a valid call, or None."""
    if not isinstance(raw_text, str):
        return None
    for pattern in (r"\{.*?\}", r"\{.*\}"):
        match = re.search(pattern, raw_text, re.DOTALL)
        if not match:
            continue
        try:
            obj = json.loads(match.group(0))
        except json.JSONDecodeError:
            continue
        call = str(obj.get("call", "")).strip().lower()
        if call in VALID_CALLS:
            obj["call"] = call
            return obj
    return None


def get_call(prompt, attempts=2):
    """Ask the LLM, retrying once if the reply does not parse."""
    for _ in range(attempts):
        parsed = parse_call(call_llm(prompt))
        if parsed:
            return parsed
    return None

# COMMAND ----------

# DBTITLE 1,Run agent and write predictions
def run_for_date(as_of):
    """Generate one prediction per symbol for a single as-of date and write
    them idempotently: delete each (symbol, prediction_date), then insert.
    Unparseable replies are skipped, never written as 'unknown'."""
    rows = []
    for symbol in SYMBOLS:
        chunks = retrieve_context(symbol, as_of)
        signals = structured_signals(symbol, as_of)
        parsed = get_call(build_prompt(symbol, signals, chunks))
        if parsed is None:
            SKIPPED.append((as_of, symbol))
            print(f"{as_of} {symbol}: SKIPPED (unparseable after retry)")
            continue
        rows.append({
            "symbol": symbol,
            "prediction_date": as_of,
            "call": parsed["call"],
            "confidence": parsed.get("confidence", "low"),
            "reasoning": parsed.get("reasoning", ""),
            "retrieved_doc_ids": [c["doc_id"] for c in chunks],
            "model_endpoint": LLM_ENDPOINT,
        })
        print(f"{as_of} {symbol}: {parsed.get('call')} ({parsed.get('confidence')})")

    if not rows:
        return
    df = (spark.createDataFrame(rows)
          .withColumn("created_at", current_timestamp()))
    df = df.withColumn("prediction_date", df["prediction_date"].cast("date"))
    symbols = ", ".join(f"'{r['symbol']}'" for r in rows)
    spark.sql(f"""DELETE FROM {PREDICTIONS_TABLE}
                  WHERE prediction_date = DATE('{as_of}') AND symbol IN ({symbols})""")
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

SKIPPED = []
print(f"Running agent for {len(dates)} date(s): {dates}")
for ds in dates:
    run_for_date(ds)
print(f"Skipped (unparseable): {len(SKIPPED)} {SKIPPED}")

spark.table(PREDICTIONS_TABLE).orderBy("created_at", ascending=False).show(truncate=False)