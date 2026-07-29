# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · Data Quality Checks & Low-Confidence Review Queue
# MAGIC Runs explicit DQ checks over `gold_extracted_fields`, writes
# MAGIC results to `data_quality_results`, and separately enqueues any
# MAGIC document whose classification confidence fell below
# MAGIC `CONFIG.classification_confidence_threshold` into `review_queue`.

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402
from doc_intelligence.quality import run_quality_checks  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

gold_table = f"{catalog}.{schema}.gold_extracted_fields"
classified_table = f"{catalog}.{schema}.silver_classified_documents"
dq_table = f"{catalog}.{schema}.data_quality_results"
review_table = f"{catalog}.{schema}.review_queue"

# COMMAND ----------
import uuid

run_id = str(uuid.uuid4())
extracted = spark.table(gold_table)
dq_results = run_quality_checks(extracted, run_id=run_id)
(
    dq_results.write.format("delta")
    .mode("append")
    .saveAsTable(dq_table)
)

n_failed = dq_results.filter("passed = false").count()
n_total = dq_results.count()
print(f"Ran {n_total} check(s) this pass; {n_failed} failed.")

# COMMAND ----------
from pyspark.sql import functions as F

low_conf = spark.table(classified_table).filter(
    F.col("confidence") < CONFIG.classification_confidence_threshold
)

already_reviewed = (
    spark.table(review_table).filter("reason = 'LOW_CONFIDENCE_CLASSIFICATION'").select("doc_id")
    if spark.catalog.tableExists(review_table)
    else None
)
to_enqueue = low_conf
if already_reviewed is not None:
    to_enqueue = low_conf.join(already_reviewed, on="doc_id", how="left_anti")

n_enqueued = to_enqueue.count()
if n_enqueued > 0:
    review_rows = to_enqueue.select(
        F.expr("uuid()").alias("review_id"),
        "doc_id",
        F.lit("LOW_CONFIDENCE_CLASSIFICATION").alias("reason"),
        F.lit("PENDING").alias("status"),
        F.to_json(F.struct("document_type", "confidence")).alias("original_payload"),
        F.lit(None).cast("string").alias("corrected_payload"),
        F.lit(None).cast("string").alias("reviewer"),
        F.current_timestamp().alias("created_at"),
        F.lit(None).cast("timestamp").alias("reviewed_at"),
    )
    review_rows.write.format("delta").mode("append").saveAsTable(review_table)
print(f"Enqueued {n_enqueued} low-confidence classification(s) for review.")

# COMMAND ----------
display(
    dq_results.groupBy("check_name", "passed").count().orderBy("check_name")
)
