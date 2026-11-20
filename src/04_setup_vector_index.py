# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Codifies the AI Search / Vector Search endpoint and index that were
# originally created by hand in the Databricks UI (Catalog Explorer ->
# rag_text_corpus -> Create -> Vector Search Index). Safe to rerun: catches
# the "already exists" error instead of checking first, since the exact
# shape of list_endpoints()/list_indexes() responses isn't worth depending
# on here.
#
# Note on naming: the workspace UI already shows "AI Search endpoint" and
# MCP URLs under /api/2.0/mcp/ai-search/..., which suggests Databricks is
# mid-rename from "Vector Search" to "AI Search". This script targets the
# long-established `databricks-vectorsearch` package (`VectorSearchClient`),
# which is what actually built rag_text_corpus_index. If your workspace has
# moved to a newer `databricks-ai-search` package, the class is
# `AISearchClient` and the method names below are unchanged.

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
    # None when the index is unusable, e.g. its endpoint was recreated
    # under the same name with a new id, which orphans the index.
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

import time
from databricks.vector_search.exceptions import BadRequest

TIMEOUT_MIN = 20
idx = vsc.get_index(ENDPOINT_NAME, INDEX_NAME)
deadline = time.time() + TIMEOUT_MIN * 60
while True:
    try:
        idx.similarity_search(query_text="test", columns=["doc_id"],
                              num_results=1, query_type="HYBRID")
        break
    except BadRequest as e:
        if time.time() > deadline:
            raise TimeoutError(f"index not ready after {TIMEOUT_MIN} min: {e}")
        time.sleep(15)
print("Index ready.")

# COMMAND ----------

import json
status = vsc.get_index(ENDPOINT_NAME, INDEX_NAME).describe().get("status", {})
print(json.dumps(status, indent=2, default=str))