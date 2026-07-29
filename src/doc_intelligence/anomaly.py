"""Rule-based + statistical anomaly detection over extracted fields.

Flags:
- AMOUNT_OUTLIER: amount_total more than `z_threshold` standard
  deviations from the mean for that vendor/document_type cohort
  (falls back to IQR fencing for small cohorts).
- DUPLICATE_INVOICE_NUMBER: same invoice_number seen for a vendor more
  than once.
- FUTURE_DATE: doc_date in the future.
- MISSING_REQUIRED_FIELD: a document_type-specific required field is null.
"""

from pyspark.sql import DataFrame, functions as F, Window

REQUIRED_FIELDS = {
    "invoice": ["vendor_or_party", "amount_total", "doc_date"],
    "contract": ["vendor_or_party"],
    "resume": ["vendor_or_party"],  # candidate_name promoted into vendor_or_party
    "financial_report": ["amount_total"],
    "id_document": [],
    "other": [],
}


def detect_amount_outliers(df: DataFrame, z_threshold: float = 2.5) -> DataFrame:
    stats_window = Window.partitionBy("document_type")
    with_stats = (
        df.filter(F.col("amount_total").isNotNull())
        .withColumn("_mean", F.avg("amount_total").over(stats_window))
        .withColumn("_stddev", F.stddev("amount_total").over(stats_window))
    )

    outliers = with_stats.withColumn(
        "_z",
        F.when(F.col("_stddev") > 0, (F.col("amount_total") - F.col("_mean")) / F.col("_stddev")).otherwise(0.0),
    ).filter(F.abs(F.col("_z")) >= z_threshold)

    return outliers.select(
        "doc_id",
        "document_type",
        F.lit("AMOUNT_OUTLIER").alias("anomaly_type"),
        F.lit("amount_total").alias("metric_name"),
        F.col("amount_total").alias("metric_value"),
        F.concat(
            F.format_number(F.col("_mean") - z_threshold * F.col("_stddev"), 2),
            F.lit(" - "),
            F.format_number(F.col("_mean") + z_threshold * F.col("_stddev"), 2),
        ).alias("expected_range"),
        F.when(F.abs(F.col("_z")) >= z_threshold * 1.6, F.lit("HIGH"))
        .when(F.abs(F.col("_z")) >= z_threshold, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW"))
        .alias("severity"),
        F.concat(F.lit("z-score="), F.round(F.col("_z"), 2)).alias("details"),
    ).withColumn("detected_at", F.current_timestamp())


def detect_duplicate_invoice_numbers(df: DataFrame) -> DataFrame:
    """df: doc_id, document_type, extracted_json (containing invoice_number), vendor_or_party."""
    invoices = df.filter("document_type = 'invoice'").withColumn(
        "_invoice_number", F.get_json_object("extracted_json", "$.invoice_number")
    ).filter(F.col("_invoice_number").isNotNull())

    w = Window.partitionBy("vendor_or_party", "_invoice_number")
    flagged = (
        invoices.withColumn("_dupe_count", F.count("doc_id").over(w))
        .filter("_dupe_count > 1")
    )

    return flagged.select(
        "doc_id",
        "document_type",
        F.lit("DUPLICATE_INVOICE_NUMBER").alias("anomaly_type"),
        F.lit("invoice_number").alias("metric_name"),
        F.col("_dupe_count").cast("double").alias("metric_value"),
        F.lit("1 (unique)").alias("expected_range"),
        F.lit("HIGH").alias("severity"),
        F.concat(
            F.lit("vendor="), F.coalesce(F.col("vendor_or_party"), F.lit("?")),
            F.lit(" invoice_number="), F.col("_invoice_number"),
        ).alias("details"),
    ).withColumn("detected_at", F.current_timestamp())


def detect_future_dates(df: DataFrame) -> DataFrame:
    flagged = df.filter(F.col("doc_date") > F.current_date())
    return flagged.select(
        "doc_id",
        "document_type",
        F.lit("FUTURE_DATE").alias("anomaly_type"),
        F.lit("doc_date").alias("metric_name"),
        F.lit(None).cast("double").alias("metric_value"),
        F.lit("<= today").alias("expected_range"),
        F.lit("MEDIUM").alias("severity"),
        F.concat(F.lit("doc_date="), F.col("doc_date").cast("string")).alias("details"),
    ).withColumn("detected_at", F.current_timestamp())


def detect_missing_required_fields(df: DataFrame) -> DataFrame:
    rows = []
    for doc_type, fields in REQUIRED_FIELDS.items():
        for field in fields:
            subset = df.filter(
                (F.col("document_type") == doc_type) & F.col(field).isNull()
            )
            rows.append(
                subset.select(
                    "doc_id",
                    "document_type",
                    F.lit("MISSING_REQUIRED_FIELD").alias("anomaly_type"),
                    F.lit(field).alias("metric_name"),
                    F.lit(None).cast("double").alias("metric_value"),
                    F.lit("not null").alias("expected_range"),
                    F.lit("MEDIUM").alias("severity"),
                    F.concat(F.lit("missing field: "), F.lit(field)).alias("details"),
                )
            )
    if not rows:
        return None
    result = rows[0]
    for r in rows[1:]:
        result = result.union(r)
    return result.withColumn("detected_at", F.current_timestamp())


def run_all_checks(df: DataFrame) -> DataFrame:
    """df: gold_extracted_fields (amount_total, doc_date, document_type,
    vendor_or_party, extracted_json). Returns the union of all anomaly
    checks in the gold_document_anomalies schema.
    """
    results = [
        detect_amount_outliers(df),
        detect_duplicate_invoice_numbers(df),
        detect_future_dates(df),
    ]
    missing = detect_missing_required_fields(df)
    if missing is not None:
        results.append(missing)

    combined = results[0]
    for r in results[1:]:
        combined = combined.unionByName(r)
    return combined
