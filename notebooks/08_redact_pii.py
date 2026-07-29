# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Redact PII
# MAGIC Runs regex + LLM-based PII detection over parsed documents and
# MAGIC writes a redacted copy safe for wider sharing to
# MAGIC `gold_redacted_documents`. Also enqueues any PII-positive document
# MAGIC into the `review_queue` and fires a Slack alert.

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402
from doc_intelligence.redaction import redact_documents  # noqa: E402
from doc_intelligence.alerts import alert_on_pii_found  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
dbutils.widgets.text("llm_endpoint", CONFIG.llm_endpoint)
dbutils.widgets.text("slack_webhook_url", CONFIG.slack_webhook_url)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
llm_endpoint = dbutils.widgets.get("llm_endpoint")
slack_webhook_url = dbutils.widgets.get("slack_webhook_url")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

parsed_table = f"{catalog}.{schema}.silver_parsed_documents"
redacted_table = f"{catalog}.{schema}.gold_redacted_documents"
review_table = f"{catalog}.{schema}.review_queue"
audit_table = f"{catalog}.{schema}.audit_log"

# COMMAND ----------
parsed = spark.table(parsed_table).filter("parse_status = 'SUCCESS' AND full_text IS NOT NULL")
already_redacted = (
    spark.table(redacted_table).select("doc_id")
    if spark.catalog.tableExists(redacted_table)
    else None
)
to_redact = parsed
if already_redacted is not None:
    to_redact = parsed.join(already_redacted, on="doc_id", how="left_anti")

pending_count = to_redact.count()
print(f"{pending_count} document(s) pending redaction.")

# COMMAND ----------
if pending_count > 0:
    redacted_df = redact_documents(to_redact, text_col="full_text", llm_endpoint=llm_endpoint)
    redacted_df.cache()
    (
        redacted_df.write.format("delta")
        .mode("append")
        .saveAsTable(redacted_table)
    )
    print(f"Redacted {redacted_df.count()} document(s).")

    # Enqueue PII-positive docs for human review
    from pyspark.sql import functions as F
    import uuid

    flagged = redacted_df.filter("contains_pii = true")
    review_rows = flagged.select(
        F.expr("uuid()").alias("review_id"),
        "doc_id",
        F.lit("PII_FOUND").alias("reason"),
        F.lit("PENDING").alias("status"),
        F.col("pii_entities_json").alias("original_payload"),
        F.lit(None).cast("string").alias("corrected_payload"),
        F.lit(None).cast("string").alias("reviewer"),
        F.current_timestamp().alias("created_at"),
        F.lit(None).cast("timestamp").alias("reviewed_at"),
    )
    review_rows.write.format("delta").mode("append").saveAsTable(review_table)

    n_alerted = alert_on_pii_found(redacted_df, slack_webhook_url)
    print(f"Alerted on {n_alerted} PII-positive document(s).")

    audit_rows = redacted_df.select(
        F.expr("uuid()").alias("event_id"),
        "doc_id",
        F.lit("REDACTED").alias("event_type"),
        F.lit("job:redact_pii").alias("actor"),
        F.lit(None).cast("string").alias("detail"),
        F.current_timestamp().alias("event_time"),
    )
    audit_rows.write.format("delta").mode("append").saveAsTable(audit_table)
else:
    print("Nothing new to redact.")
