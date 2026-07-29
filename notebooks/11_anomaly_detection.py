# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Anomaly Detection
# MAGIC Runs statistical (z-score) and rule-based checks over
# MAGIC `gold_extracted_fields` — amount outliers, duplicate invoice
# MAGIC numbers, future-dated documents, missing required fields — writing
# MAGIC results to `gold_document_anomalies`, enqueueing medium/high
# MAGIC severity findings for review, and alerting on high severity.

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402
from doc_intelligence.anomaly import run_all_checks  # noqa: E402
from doc_intelligence.alerts import alert_on_high_severity_anomalies  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
dbutils.widgets.text("slack_webhook_url", CONFIG.slack_webhook_url)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
slack_webhook_url = dbutils.widgets.get("slack_webhook_url")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

gold_table = f"{catalog}.{schema}.gold_extracted_fields"
anomalies_table = f"{catalog}.{schema}.gold_document_anomalies"
review_table = f"{catalog}.{schema}.review_queue"

# COMMAND ----------
extracted = spark.table(gold_table)
anomalies_df = run_all_checks(extracted)
anomalies_df.cache()
n_anomalies = anomalies_df.count()
print(f"Detected {n_anomalies} anomaly finding(s) this run.")

# COMMAND ----------
if n_anomalies > 0:
    (
        anomalies_df.write.format("delta")
        .mode("append")
        .saveAsTable(anomalies_table)
    )

    from pyspark.sql import functions as F

    severity_rank = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
    threshold = severity_rank.get(CONFIG.anomaly_severity_for_review, 1)
    severity_map = F.create_map(*[x for kv in severity_rank.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])

    to_review = anomalies_df.withColumn("_sev_rank", severity_map[F.col("severity")]).filter(
        F.col("_sev_rank") >= threshold
    )

    review_rows = to_review.select(
        F.expr("uuid()").alias("review_id"),
        "doc_id",
        F.lit("ANOMALY").alias("reason"),
        F.lit("PENDING").alias("status"),
        F.to_json(F.struct("anomaly_type", "metric_name", "metric_value", "details")).alias("original_payload"),
        F.lit(None).cast("string").alias("corrected_payload"),
        F.lit(None).cast("string").alias("reviewer"),
        F.current_timestamp().alias("created_at"),
        F.lit(None).cast("timestamp").alias("reviewed_at"),
    )
    review_rows.write.format("delta").mode("append").saveAsTable(review_table)
    print(f"Enqueued {review_rows.count()} anomaly finding(s) for review.")

    n_alerted = alert_on_high_severity_anomalies(
        anomalies_df, slack_webhook_url, min_severity=CONFIG.alert_min_severity
    )
    print(f"Alerted on {n_alerted} high-severity anomaly finding(s).")

display(anomalies_df.orderBy("severity", ascending=False))
