# Databricks notebook source
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
except Exception as e:
    print(f"Index {INDEX_NAME} probably already exists ({e}); triggering a sync instead.")
    vsc.get_index(ENDPOINT_NAME, INDEX_NAME).sync()

# COMMAND ----------

# Sanity check: confirm the index is queryable before anything downstream
# (the agent notebook) depends on it.
index = vsc.get_index(ENDPOINT_NAME, INDEX_NAME)
sample = index.similarity_search(
    query_text="viewer reaction to a show",
    columns=["doc_id", "text_content"],
    num_results=1,
    query_type="HYBRID",
)
print(sample)
