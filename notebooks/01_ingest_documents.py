# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingest Documents
# MAGIC Streams new files landed in the `raw_docs` Volume into
# MAGIC `bronze_raw_documents` using Auto Loader (`cloudFiles`), computing a
# MAGIC stable `doc_id` from the file path. This is the Databricks analogue of
# MAGIC a Snowflake Stream + Serverless Task watching an external stage.

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

raw_path = f"/Volumes/{catalog}/{schema}/{CONFIG.volume_raw_docs}"
checkpoint_path = f"/Volumes/{catalog}/{schema}/{CONFIG.volume_checkpoints}/ingest"
target_table = f"{catalog}.{schema}.bronze_raw_documents"

print(f"Source volume: {raw_path}")
print(f"Checkpoint:    {checkpoint_path}")
print(f"Target table:  {target_table}")

# COMMAND ----------
from pyspark.sql import functions as F

stream_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
    .option("pathGlobFilter", "*.{pdf,png,jpg,jpeg,tif,tiff,docx}")
    .load(raw_path)
)

bronze_df = (
    stream_df.withColumn("doc_id", F.sha2(F.col("path"), 256))
    .withColumn("file_name", F.element_at(F.split(F.col("path"), "/"), -1))
    .withColumn(
        "file_extension",
        F.lower(F.element_at(F.split(F.col("file_name"), "\\."), -1)),
    )
    .withColumnRenamed("length", "file_size_bytes")
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "doc_id",
        "path",
        "file_name",
        "file_extension",
        "file_size_bytes",
        "content",
        "ingested_at",
        "modificationTime",
    )
    .withColumnRenamed("modificationTime", "modification_time")
)

# COMMAND ----------
query = (
    bronze_df.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(target_table)
)
query.awaitTermination()

# COMMAND ----------
result_count = spark.table(target_table).count()
print(f"bronze_raw_documents now has {result_count} rows.")

dbutils.jobs.taskValues.set(key="rows_ingested", value=result_count) if "dbutils" in dir() else None
