# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Duplicate Detection
# MAGIC Flags exact duplicates (normalized-text hash match) and near
# MAGIC duplicates (embedding similarity via the Vector Search index),
# MAGIC writing results to `gold_duplicate_documents`.

# COMMAND ----------
# MAGIC %pip install -q databricks-vectorsearch
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402
from doc_intelligence.dedup import find_exact_duplicates, find_near_duplicates_from_search  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
dbutils.widgets.text("ai_schema", CONFIG.ai_schema)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ai_schema = dbutils.widgets.get("ai_schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

parsed_table = f"{catalog}.{schema}.silver_parsed_documents"
chunks_table = f"{catalog}.{schema}.gold_document_chunks"
dupes_table = f"{catalog}.{schema}.gold_duplicate_documents"
index_fullname = f"{catalog}.{ai_schema}.{CONFIG.vector_index_name}"

# COMMAND ----------
parsed = spark.table(parsed_table).filter("parse_status = 'SUCCESS' AND full_text IS NOT NULL")
exact_dupes = find_exact_duplicates(parsed, text_col="full_text")
print(f"Found {exact_dupes.count()} exact-duplicate pair(s).")

# COMMAND ----------
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
chunks_df = spark.table(chunks_table)

near_dupes = find_near_duplicates_from_search(
    spark=spark,
    vsc=vsc,
    endpoint_name=CONFIG.vector_search_endpoint,
    index_fullname=index_fullname,
    chunks_df=chunks_df,
    similarity_threshold=CONFIG.near_duplicate_similarity_threshold,
)
print(f"Found {near_dupes.count()} near-duplicate pair(s).")

# COMMAND ----------
combined = exact_dupes.unionByName(near_dupes)
if combined.count() > 0:
    (
        combined.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(dupes_table)
    )
    print(f"Wrote {combined.count()} duplicate pair(s) to {dupes_table}.")
else:
    print("No duplicates found in this run.")

display(spark.table(dupes_table).orderBy("detected_at", ascending=False).limit(20) if spark.catalog.tableExists(dupes_table) else combined)
