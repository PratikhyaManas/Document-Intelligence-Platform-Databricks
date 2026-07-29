# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Extract Structured Data
# MAGIC For each classified document, extracts a type-specific set of
# MAGIC structured fields (vendor, totals, dates, parties, skills, ...) via
# MAGIC `ai_query()` with a JSON-schema instruction, writing to
# MAGIC `gold_extracted_fields`. Equivalent to Snowflake's schema-based
# MAGIC structured extraction functions.

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402
from doc_intelligence.extraction import extract_fields  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
dbutils.widgets.text("llm_endpoint", CONFIG.llm_endpoint)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
llm_endpoint = dbutils.widgets.get("llm_endpoint")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

parsed_table = f"{catalog}.{schema}.silver_parsed_documents"
classified_table = f"{catalog}.{schema}.silver_classified_documents"
gold_table = f"{catalog}.{schema}.gold_extracted_fields"

# COMMAND ----------
joined = (
    spark.table(parsed_table)
    .filter("parse_status = 'SUCCESS'")
    .join(spark.table(classified_table), on="doc_id", how="inner")
)

already_extracted = (
    spark.table(gold_table).select("doc_id")
    if spark.catalog.tableExists(gold_table)
    else None
)
to_extract = joined
if already_extracted is not None:
    to_extract = joined.join(already_extracted, on="doc_id", how="left_anti")

pending_count = to_extract.count()
print(f"{pending_count} document(s) pending extraction.")

# COMMAND ----------
if pending_count > 0:
    extracted_df = extract_fields(
        to_extract,
        text_col="full_text",
        doc_type_col="document_type",
        llm_endpoint=llm_endpoint,
    )
    (
        extracted_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(gold_table)
    )
    print(f"Extracted fields for {extracted_df.count()} document(s).")
else:
    print("Nothing new to extract.")

# COMMAND ----------
display(spark.table(gold_table).orderBy("extracted_at", ascending=False).limit(20))
