# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Summarize Documents
# MAGIC Generates an executive summary + key points + sentiment per
# MAGIC document via `ai_query()`, written to `gold_document_summaries`.
# MAGIC Surfaced in the Explore tab of the app.

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402
from doc_intelligence.summarization import summarize_documents  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
dbutils.widgets.text("llm_endpoint", CONFIG.llm_endpoint)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
llm_endpoint = dbutils.widgets.get("llm_endpoint")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

parsed_table = f"{catalog}.{schema}.silver_parsed_documents"
summaries_table = f"{catalog}.{schema}.gold_document_summaries"

# COMMAND ----------
parsed = spark.table(parsed_table).filter("parse_status = 'SUCCESS' AND full_text IS NOT NULL")
already_summarized = (
    spark.table(summaries_table).select("doc_id")
    if spark.catalog.tableExists(summaries_table)
    else None
)
to_summarize = parsed
if already_summarized is not None:
    to_summarize = parsed.join(already_summarized, on="doc_id", how="left_anti")

pending_count = to_summarize.count()
print(f"{pending_count} document(s) pending summarization.")

# COMMAND ----------
if pending_count > 0:
    summaries_df = summarize_documents(to_summarize, text_col="full_text", llm_endpoint=llm_endpoint)
    (
        summaries_df.write.format("delta")
        .mode("append")
        .saveAsTable(summaries_table)
    )
    print(f"Summarized {summaries_df.count()} document(s).")
else:
    print("Nothing new to summarize.")

# COMMAND ----------
display(spark.table(summaries_table).orderBy("summarized_at", ascending=False).limit(10))
