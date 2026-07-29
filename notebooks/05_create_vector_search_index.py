# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Chunk + Build Vector Search Index
# MAGIC Chunks `silver_parsed_documents.full_text` into
# MAGIC `gold_document_chunks`, then creates/refreshes a Databricks Vector
# MAGIC Search Delta Sync index over it — equivalent to Snowflake's
# MAGIC CORTEX_SEARCH service.

# COMMAND ----------
# MAGIC %pip install -q databricks-vectorsearch
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402
from doc_intelligence.parsing import chunk_text  # noqa: E402
from doc_intelligence.search import get_or_create_endpoint, get_or_create_delta_sync_index  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
dbutils.widgets.text("ai_schema", CONFIG.ai_schema)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ai_schema = dbutils.widgets.get("ai_schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

parsed_table = f"{catalog}.{schema}.silver_parsed_documents"
classified_table = f"{catalog}.{schema}.silver_classified_documents"
chunks_table = f"{catalog}.{schema}.gold_document_chunks"

# COMMAND ----------
import hashlib
from pyspark.sql import functions as F, types as T

parsed = (
    spark.table(parsed_table)
    .filter("parse_status = 'SUCCESS' AND full_text IS NOT NULL")
    .join(spark.table(classified_table), on="doc_id", how="left")
)

already_chunked = (
    spark.table(chunks_table).select("doc_id").distinct()
    if spark.catalog.tableExists(chunks_table)
    else None
)
to_chunk = parsed
if already_chunked is not None:
    to_chunk = parsed.join(already_chunked, on="doc_id", how="left_anti")

pending_count = to_chunk.count()
print(f"{pending_count} document(s) pending chunking.")

# COMMAND ----------
chunk_schema = T.ArrayType(T.StringType())
chunk_udf = F.udf(
    lambda text: chunk_text(
        text, CONFIG.chunk_size_tokens, CONFIG.chunk_overlap_tokens
    ),
    chunk_schema,
)

if pending_count > 0:
    exploded = (
        to_chunk.withColumn("_chunks", chunk_udf(F.col("full_text")))
        .withColumn("chunk_text", F.explode("_chunks"))
        .withColumn("chunk_index", F.expr("row_number() over (partition by doc_id order by chunk_text) - 1"))
        .withColumn(
            "chunk_id",
            F.sha2(F.concat_ws("::", F.col("doc_id"), F.col("chunk_index").cast("string")), 256),
        )
        .withColumn("chunk_tokens", F.size(F.split(F.col("chunk_text"), r"\s+")))
        .withColumn("created_at", F.current_timestamp())
        .select(
            "chunk_id",
            "doc_id",
            "file_name",
            "document_type",
            "chunk_index",
            "chunk_text",
            "chunk_tokens",
            "created_at",
        )
    )
    (
        exploded.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(chunks_table)
    )
    print(f"Wrote {exploded.count()} chunks to {chunks_table}.")
else:
    print("Nothing new to chunk.")

# Change Data Feed must be enabled for the Delta Sync index to track updates
spark.sql(f"ALTER TABLE {chunks_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
endpoint_name = CONFIG.vector_search_endpoint
index_fullname = f"{catalog}.{ai_schema}.{CONFIG.vector_index_name}"

get_or_create_endpoint(vsc, endpoint_name)
index = get_or_create_delta_sync_index(
    client=vsc,
    endpoint_name=endpoint_name,
    source_table_fullname=chunks_table,
    index_fullname=index_fullname,
    primary_key="chunk_id",
    embedding_source_column="chunk_text",
    embedding_model_endpoint=CONFIG.embedding_endpoint,
)
print(f"Vector Search index ready: {index_fullname}")
