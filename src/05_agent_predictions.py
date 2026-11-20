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
SKIPS_TABLE = f"{GOLD}.prediction_skips"
KEY = ["symbol", "prediction_date", "source", "variant"]

# Confirm the exact serving endpoint name in your workspace under Serving in
# the sidebar; Foundation Model API endpoint names vary by workspace/region.
LLM_ENDPOINT = "databricks-meta-llama-3-1-8b-instruct"

SYMBOLS = ["NFLX", "DIS"]
BRAND_OF = {"NFLX": "Netflix", "DIS": "Disney+"}

vsc = VectorSearchClient()
index = vsc.get_index(ENDPOINT_NAME, INDEX_NAME)
llm = mlflow.deployments.get_deploy_client("databricks")

# COMMAND ----------

# DBTITLE 1,Parameters
# The scheduled job passes nothing: one live run for today, both variants.
dbutils.widgets.text("as_of_date", date.today().isoformat())
dbutils.widgets.text("backfill_start", "")  # set to backfill every scorable date from here to as_of_date
dbutils.widgets.text("use_rag", "")         # empty = mode default; "true"/"false" = one variant (live only)

# COMMAND ----------

# DBTITLE 1,Tables and frozen cutoffs
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PREDICTIONS_TABLE} (
  symbol STRING,
  prediction_date DATE,
  source STRING,
  variant STRING,
  call STRING,
  confidence STRING,
  reasoning STRING,
  retrieved_doc_ids ARRAY<STRING>,
  model_endpoint STRING,
  created_at TIMESTAMP
) USING DELTA
""")
if "source" not in spark.table(PREDICTIONS_TABLE).columns:
    spark.sql(f"ALTER TABLE {PREDICTIONS_TABLE} ADD COLUMNS (source STRING, variant STRING)")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SKIPS_TABLE} (
  symbol STRING,
  prediction_date DATE,
  source STRING,
  variant STRING,
  created_at TIMESTAMP
) USING DELTA
""")

# Frozen by src/ddl_move_definition.sql; 06 scores with the same row.
CUTOFFS = spark.table(f"{GOLD}.move_cutoffs").first()
print(f"cutoffs: fall < {CUTOFFS.fall_below:.2f}, rise > {CUTOFFS.rise_above:.2f}, horizon {CUTOFFS.horizon_days}d")

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
    # Same popularity view 06 scores against; newest 7 snapshots on or before as_of.
    popularity = spark.sql(f"""
        SELECT d AS snapshot_date, popularity AS popularity_score
        FROM {GOLD}.brand_popularity
        WHERE brand = '{brand}' AND d <= DATE('{as_of}')
        ORDER BY d DESC
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
        # Oldest first: the model read a newest-first list as chronological and inverted every trend.
        "recent_popularity_oldest_to_newest": [r.asDict() for r in reversed(popularity)],
        "sentiment": sentiment[0].asDict() if sentiment else None,
        "estimates": estimates[0].asDict() if estimates else None,
    }


def build_prompt(symbol, signals, chunks):
    brand = BRAND_OF.get(symbol, symbol)
    context_text = "\n\n".join(
        f"[{c['doc_id']}] {c['text_content'][:500]}" for c in chunks
    )
    as_of = signals["as_of_date"]
    h = CUTOFFS.horizon_days
    retrieved = (f"\nRetrieved viewer reviews and news:\n{context_text}\n\n"
                 "Ground your explanation in the retrieved text where you can.") if chunks else ""
    return f"""You are assessing the near-term popularity trajectory of {brand}
on streaming, using only the signals below.

Definitions (fixed):
- popularity_score = 101 - rank of the {brand} app in the US App Store top-free chart (higher is better).
- change = popularity_score {h} days after {as_of} minus popularity_score on {as_of}.
- RISE if change > {CUTOFFS.rise_above:.2f}; FALL if change < {CUTOFFS.fall_below:.2f}; HOLD otherwise.

Call RISE, HOLD, or FALL for {brand} over the next {h} days, and explain why.

Structured signals (recent popularity_score by snapshot_date, listed oldest to newest;
analyst sentiment; revenue/EPS estimates):
{json.dumps(signals, default=str, indent=2)}
{retrieved}
Respond with strict JSON only, no other text:
{{"call": "rise" | "hold" | "fall", "confidence": "low" | "medium" | "high", "reasoning": "one or two sentences, citing the specific evidence that drove the call"}}
"""

# COMMAND ----------

# DBTITLE 1,LLM call and parsing
import re


def call_llm(prompt):
    response = llm.predict(
        endpoint=LLM_ENDPOINT,
        inputs={"messages": [{"role": "user", "content": prompt}], "max_tokens": 300, "temperature": 0},
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
from delta.tables import DeltaTable

PRED_SCHEMA = ("symbol STRING, prediction_date DATE, source STRING, variant STRING, call STRING, "
               "confidence STRING, reasoning STRING, retrieved_doc_ids ARRAY<STRING>, model_endpoint STRING")
KEY_SCHEMA = "symbol STRING, prediction_date DATE, source STRING, variant STRING"
ON = " AND ".join(f"t.{k} = s.{k}" for k in KEY)


def scorable_dates(brand, start, end):
    """Dates with a snapshot and another exactly horizon_days later, so every backfill row can be scored."""
    rows = spark.sql(f"""
        SELECT a.d FROM {GOLD}.brand_popularity a
        JOIN {GOLD}.brand_popularity b ON b.brand = a.brand AND b.d = date_add(a.d, {CUTOFFS.horizon_days})
        WHERE a.brand = '{brand}' AND a.d BETWEEN DATE('{start}') AND DATE('{end}')
        ORDER BY a.d
    """).collect()
    return [r.d.isoformat() for r in rows]


def predict(symbol, as_of, source, use_rag):
    """Return (row, key); row is None when the reply never parsed."""
    key = {"symbol": symbol, "prediction_date": date.fromisoformat(as_of),
           "source": source, "variant": "rag" if use_rag else "no_rag"}
    chunks = retrieve_context(symbol, as_of) if use_rag else []
    parsed = get_call(build_prompt(symbol, structured_signals(symbol, as_of), chunks))
    if parsed is None:
        print(f"{as_of} {symbol} {key['variant']}: SKIPPED (unparseable after retry)")
        return None, key
    print(f"{as_of} {symbol} {key['variant']}: {parsed['call']} ({parsed.get('confidence')})")
    return {**key, "call": parsed["call"], "confidence": parsed.get("confidence", "low"),
            "reasoning": parsed.get("reasoning", ""), "retrieved_doc_ids": [c["doc_id"] for c in chunks],
            "model_endpoint": LLM_ENDPOINT}, key


def write_results(done, skipped):
    """Upsert on KEY; each key ends up in exactly one of predictions or prediction_skips."""
    preds, skips = DeltaTable.forName(spark, PREDICTIONS_TABLE), DeltaTable.forName(spark, SKIPS_TABLE)
    if done:
        df = spark.createDataFrame(done, PRED_SCHEMA).withColumn("created_at", current_timestamp())
        preds.alias("t").merge(df.alias("s"), ON).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        skips.alias("t").merge(df.select(*KEY).alias("s"), ON).whenMatchedDelete().execute()
    if skipped:
        df = spark.createDataFrame(skipped, KEY_SCHEMA).withColumn("created_at", current_timestamp())
        skips.alias("t").merge(df.alias("s"), ON).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        preds.alias("t").merge(df.select(*KEY).alias("s"), ON).whenMatchedDelete().execute()


as_of_date = dbutils.widgets.get("as_of_date") or date.today().isoformat()
backfill_start = dbutils.widgets.get("backfill_start").strip()
use_rag_flag = dbutils.widgets.get("use_rag").strip().lower()
if use_rag_flag not in ("", "true", "false"):
    raise ValueError(f"use_rag must be empty, true or false, got {use_rag_flag!r}")

if backfill_start:
    # No news exists before 11-10, so a backfill only runs the no-RAG variant.
    if use_rag_flag == "true":
        raise ValueError("Backfill runs no_rag only; leave use_rag empty or set it to false")
    source, variants = "backfill", [False]
    pairs = [(s, d) for s in SYMBOLS for d in scorable_dates(BRAND_OF[s], backfill_start, as_of_date)]
else:
    source = "live"
    variants = {"": [True, False], "true": [True], "false": [False]}[use_rag_flag]
    pairs = [(s, as_of_date) for s in SYMBOLS]

print(f"{source}: {len(pairs)} symbol-date pair(s) x {len(variants)} variant(s)")
done, skipped = [], []
for symbol, d in pairs:
    for use_rag in variants:
        row, key = predict(symbol, d, source, use_rag)
        if row:
            done.append(row)
        else:
            skipped.append(key)
write_results(done, skipped)
print(f"written: {len(done)} | skipped (unparseable): {len(skipped)} "
      f"{[(k['symbol'], str(k['prediction_date']), k['variant']) for k in skipped]}")