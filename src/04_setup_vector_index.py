# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Overview
# Creates, repairs or syncs the vector search endpoint and index; safe to rerun.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configure Vector Search client
from databricks.vector_search.client import VectorSearchClient

CATALOG = "bootcamp_students"
SCHEMA = "oscr_gold"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.rag_text_corpus"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.rag_text_corpus_index"
ENDPOINT_NAME = "oscr-rag-endpoint"
EMBEDDING_MODEL_ENDPOINT = "databricks-qwen3-embedding-0-6b"

vsc = VectorSearchClient()

# COMMAND ----------

try:
    vsc.create_endpoint(name=ENDPOINT_NAME, endpoint_type="STANDARD")
    print(f"Created endpoint {ENDPOINT_NAME}")
except Exception as e:
    print(f"Endpoint {ENDPOINT_NAME} probably already exists ({e}); continuing.")

# COMMAND ----------

# DBTITLE 1,Create or repair the index
def describe_index():
    # None when unusable, e.g. a recreated endpoint (same name, new id) orphans the index.
    try:
        return vsc.get_index(ENDPOINT_NAME, INDEX_NAME).describe()
    except Exception:
        return None


import time

if describe_index() is None:
    try:
        vsc.delete_index(ENDPOINT_NAME, INDEX_NAME)
        print(f"Dropped stale index {INDEX_NAME}")
    except Exception as e:
        print(f"Drop attempt failed ({e}); creating anyway")

for attempt in range(60):
    try:
        vsc.create_delta_sync_index(
            endpoint_name=ENDPOINT_NAME,
            source_table_name=SOURCE_TABLE,
            index_name=INDEX_NAME,
            pipeline_type="TRIGGERED",
            primary_key="doc_id",
            embedding_source_column="text_content",
            embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
        )
        print(f"Created index {INDEX_NAME}")
        break
    except Exception as e:
        if describe_index() is not None:
            print(f"Index {INDEX_NAME} exists ({e}); triggering a sync.")
            try:
                vsc.get_index(ENDPOINT_NAME, INDEX_NAME).sync()
            except Exception as se:
                print(f"Sync skipped, index still provisioning: {se}")
            break
        if "pending deletion" in str(e):
            print(f"Old index still deleting (attempt {attempt + 1}); waiting.")
            time.sleep(10)
            continue
        raise
else:
    raise TimeoutError("index deletion never completed")


# COMMAND ----------

# DBTITLE 1,Wait for the sync to reach the latest corpus
import time

TIMEOUT_MIN = 30
# Ready alone is not enough: a stale index still answers queries, so wait for the newest data commit.
target = spark.sql(f"DESCRIBE HISTORY {SOURCE_TABLE}").filter("operation != 'SET TBLPROPERTIES'").agg({"version": "max"}).first()[0]
idx = vsc.get_index(ENDPOINT_NAME, INDEX_NAME)
deadline = time.time() + TIMEOUT_MIN * 60
last = None
while True:
    st = idx.describe().get("status", {})
    done = (st.get("triggered_update_status") or {}).get("last_processed_commit_version", -1)
    if (st.get("detailed_state"), done) != last:
        last = (st.get("detailed_state"), done)
        print(f"state={last[0]} processed_version={done} target_version={target}")
    if st.get("ready") and done >= target:
        break
    if time.time() > deadline:
        raise TimeoutError(f"index at version {done}, not {target}, after {TIMEOUT_MIN} min")
    time.sleep(30)
print(f"Index synced to version {done}: {st.get('indexed_row_count')} rows.")

# COMMAND ----------

import json
status = vsc.get_index(ENDPOINT_NAME, INDEX_NAME).describe().get("status", {})
print(json.dumps(status, indent=2, default=str))