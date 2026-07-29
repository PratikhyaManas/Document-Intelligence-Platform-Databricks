# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Parse Documents
# MAGIC Runs `ai_parse_document()` (LAYOUT mode) over any bronze rows that
# MAGIC haven't been parsed yet, writing structured text + layout JSON to
# MAGIC `silver_parsed_documents`. Equivalent to Snowflake's AI_PARSE_DOCUMENT
# MAGIC step in the reference architecture.

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402
from doc_intelligence.parsing import parse_documents  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

bronze_table = f"{catalog}.{schema}.bronze_raw_documents"
silver_table = f"{catalog}.{schema}.silver_parsed_documents"

# COMMAND ----------
bronze_df = spark.table(bronze_table)
already_parsed = spark.table(silver_table).select("doc_id") if spark.catalog.tableExists(silver_table) else None

to_parse = bronze_df
if already_parsed is not None:
    to_parse = bronze_df.join(already_parsed, on="doc_id", how="left_anti")

pending_count = to_parse.count()
print(f"{pending_count} document(s) pending parse.")

# COMMAND ----------
if pending_count > 0:
    parsed_df = parse_documents(to_parse, content_col="content")
    (
        parsed_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(silver_table)
    )
    print(f"Wrote {parsed_df.count()} rows to {silver_table}.")
else:
    print("Nothing new to parse.")

# COMMAND ----------
# Quarantine anything that failed parsing so it doesn't block downstream stages
failures = spark.table(silver_table).filter("parse_status = 'FAILED'")
fail_count = failures.count()
if fail_count > 0:
    print(f"⚠️  {fail_count} document(s) failed to parse — see silver_parsed_documents where parse_status='FAILED'.")
