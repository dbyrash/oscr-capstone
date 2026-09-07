# Databricks notebook source
# The agent: retrieves grounded context from rag_text_corpus_index,
# combines it with the structured gold signals, asks an LLM for a concrete
# beat/meet/miss call on next quarter's subscriber numbers, and writes that
# call as a new row to bootcamp_students.oscr_gold.predictions. This is the
# one place in the pipeline where an LLM's judgment produces a real state
# change, not just a descriptive aggregate.
#
# Run this after 04_setup_vector_index.py. Requires the source table's
# rows to actually have doc_ids that mention the symbol/show you're asking
# about; with a 60-row corpus, expect thin coverage for symbols that don't
# have matching TMDB shows.

# COMMAND ----------

import json
from datetime import date

import mlflow.deployments
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import current_timestamp

CATALOG = "bootcamp_students"
GOLD = f"{CATALOG}.oscr_gold"
ENDPOINT_NAME = "oscr-rag-endpoint"
INDEX_NAME = f"{GOLD}.rag_text_corpus_index"
PREDICTIONS_TABLE = f"{GOLD}.predictions"

# Confirm the exact serving endpoint name in your workspace under
# Serving in the sidebar; Foundation Model API endpoint names vary by
# workspace and region.
LLM_ENDPOINT = "databricks-meta-llama-3-1-8b-instruct"

SYMBOLS = ["NFLX", "DIS"]

vsc = VectorSearchClient()
index = vsc.get_index(ENDPOINT_NAME, INDEX_NAME)
llm = mlflow.deployments.get_deploy_client("databricks")

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


def structured_signals(symbol):
    sentiment = spark.sql(f"""
        SELECT * FROM {GOLD}.symbol_analyst_sentiment
        WHERE symbol = '{symbol}' ORDER BY period DESC LIMIT 1
    """).collect()
    estimates = spark.sql(f"""
        SELECT * FROM {GOLD}.symbol_forward_estimates
        WHERE symbol = '{symbol}' ORDER BY date DESC LIMIT 1
    """).collect()
    return {
        "sentiment": sentiment[0].asDict() if sentiment else None,
        "estimates": estimates[0].asDict() if estimates else None,
    }


def build_prompt(symbol, signals, chunks):
    context_text = "\n\n".join(
        f"[{c['doc_id']}] {c['text_content'][:500]}" for c in chunks
    )
    return f"""You are assessing whether {symbol} is likely to beat, meet, or
miss next quarter's subscriber estimates.

Structured signals:
{json.dumps(signals, default=str, indent=2)}

Retrieved viewer/news commentary:
{context_text}

Respond with strict JSON only, no other text:
{{"call": "beat" | "meet" | "miss", "confidence": "low" | "medium" | "high", "reasoning": "one or two sentences, citing what actually drove the call"}}
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

for symbol in SYMBOLS:
    chunks = retrieve_context(symbol)
    signals = structured_signals(symbol)
    prompt = build_prompt(symbol, signals, chunks)
    raw = call_llm(prompt)
    parsed = parse_call(raw)

    row = spark.createDataFrame([{
        "symbol": symbol,
        "prediction_date": date.today().isoformat(),
        "call": parsed.get("call", "unknown"),
        "confidence": parsed.get("confidence", "low"),
        "reasoning": parsed.get("reasoning", ""),
        "retrieved_doc_ids": [c["doc_id"] for c in chunks],
        "model_endpoint": LLM_ENDPOINT,
    }])
    row = row.withColumn("prediction_date", row.prediction_date.cast("date")) \
             .withColumn("created_at", current_timestamp())

    row.write.format("delta").mode("append").saveAsTable(PREDICTIONS_TABLE)
    print(f"{symbol}: {parsed}")

spark.table(PREDICTIONS_TABLE).orderBy("created_at", ascending=False).show(truncate=False)
