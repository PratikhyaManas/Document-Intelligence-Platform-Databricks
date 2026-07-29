# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Classify Documents
# MAGIC Zero-shot classification of each parsed document into a document
# MAGIC type (invoice, contract, resume, ...) using `ai_query()` against a
# MAGIC Foundation Model Serving endpoint. Equivalent to Snowflake's
# MAGIC AI_CLASSIFY.

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402
from doc_intelligence.classification import classify_documents  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
dbutils.widgets.text("llm_endpoint", CONFIG.llm_endpoint)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
llm_endpoint = dbutils.widgets.get("llm_endpoint")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

silver_parsed = f"{catalog}.{schema}.silver_parsed_documents"
silver_classified = f"{catalog}.{schema}.silver_classified_documents"

# COMMAND ----------
parsed_df = spark.table(silver_parsed).filter("parse_status = 'SUCCESS' AND full_text IS NOT NULL")

already_classified = (
    spark.table(silver_classified).select("doc_id")
    if spark.catalog.tableExists(silver_classified)
    else None
)
to_classify = parsed_df
if already_classified is not None:
    to_classify = parsed_df.join(already_classified, on="doc_id", how="left_anti")

pending_count = to_classify.count()
print(f"{pending_count} document(s) pending classification.")

# COMMAND ----------
if pending_count > 0:
    classified_df = classify_documents(
        to_classify,
        text_col="full_text",
        llm_endpoint=llm_endpoint,
        categories=CONFIG.document_types,
    )
    (
        classified_df.write.format("delta")
        .mode("append")
        .saveAsTable(silver_classified)
    )
    print(f"Classified {classified_df.count()} document(s).")
else:
    print("Nothing new to classify.")

# COMMAND ----------
from pyspark.sql import functions as F

display(
    spark.table(silver_classified)
    .groupBy("document_type")
    .count()
    .orderBy(F.desc("count"))
)
