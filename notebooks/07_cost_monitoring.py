# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Cost Monitoring
# MAGIC Aggregates spend for this pipeline using Databricks system tables —
# MAGIC the equivalent of the Snowflake article's "comprehensive cost
# MAGIC monitoring dashboard that tracks every credit spent."
# MAGIC
# MAGIC Requires `system` catalog access (enabled by an account admin via
# MAGIC Unity Catalog system schemas: `system.billing.usage`,
# MAGIC `system.billing.list_prices`).

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
dbutils.widgets.text("lookback_days", "30")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
lookback_days = int(dbutils.widgets.get("lookback_days"))

# COMMAND ----------
usage_df = spark.sql(
    f"""
    SELECT
      u.usage_date,
      u.sku_name,
      u.usage_unit,
      u.usage_quantity,
      p.pricing.default AS price_per_unit,
      u.usage_quantity * p.pricing.default AS estimated_cost_usd
    FROM system.billing.usage u
    JOIN system.billing.list_prices p
      ON u.sku_name = p.sku_name
      AND u.usage_end_time >= p.price_start_time
      AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
    WHERE u.usage_date >= date_sub(current_date(), {lookback_days})
      -- Narrow to this workload via tags set on the job/warehouse/serving
      -- endpoint, e.g. tag the Vector Search endpoint & job cluster with
      -- {{"project": "doc_intelligence"}} and filter on it here:
      AND (u.custom_tags['project'] = 'doc_intelligence' OR u.custom_tags IS NULL)
    """
)
display(usage_df)

# COMMAND ----------
daily_cost = usage_df.groupBy("usage_date").sum("estimated_cost_usd").orderBy("usage_date")
display(daily_cost)

# COMMAND ----------
by_sku = (
    usage_df.groupBy("sku_name")
    .sum("estimated_cost_usd")
    .withColumnRenamed("sum(estimated_cost_usd)", "total_cost_usd")
    .orderBy("total_cost_usd", ascending=False)
)
display(by_sku)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Pipeline throughput (documents processed vs. cost)
# MAGIC Joins `pipeline_run_log` against the cost aggregation above so you
# MAGIC can see cost-per-document trends over time in the dashboard app.

# COMMAND ----------
run_log = spark.table(f"{catalog}.{schema}.pipeline_run_log")
display(
    run_log.groupBy("stage")
    .agg({"rows_processed": "sum"})
    .withColumnRenamed("sum(rows_processed)", "total_rows_processed")
)
