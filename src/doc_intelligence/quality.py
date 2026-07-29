"""Lightweight data-quality checks over gold_extracted_fields, written
as explicit rows to data_quality_results (rather than relying solely on
Delta Live Tables expectations, so this also runs fine from a plain job
notebook).
"""

import uuid

from pyspark.sql import DataFrame, functions as F

VALID_CURRENCY_CODES = {
    "USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR", "MXN",
}


def _check(df: DataFrame, name: str, passed_expr, severity: str, detail_expr=None) -> DataFrame:
    detail_expr = detail_expr if detail_expr is not None else F.lit(None).cast("string")
    return df.select(
        F.lit(name).alias("check_name"),
        "doc_id",
        passed_expr.alias("passed"),
        F.lit(severity).alias("severity"),
        detail_expr.alias("details"),
    )


def run_quality_checks(df: DataFrame, run_id: str | None = None) -> DataFrame:
    """df: gold_extracted_fields. Returns rows matching data_quality_results
    (minus check_id/run_id/checked_at, added here).
    """
    run_id = run_id or str(uuid.uuid4())

    checks = [
        _check(
            df, "non_null_vendor_or_party",
            F.col("vendor_or_party").isNotNull(),
            "WARN",
        ),
        _check(
            df, "amount_non_negative",
            F.col("amount_total").isNull() | (F.col("amount_total") >= 0),
            "ERROR",
            F.concat(F.lit("amount_total="), F.col("amount_total").cast("string")),
        ),
        _check(
            df, "valid_currency_code",
            F.col("currency").isNull()
            | F.col("currency").isin(*sorted(VALID_CURRENCY_CODES)),
            "WARN",
            F.concat(F.lit("currency="), F.coalesce(F.col("currency"), F.lit("NULL"))),
        ),
        _check(
            df, "extracted_json_is_valid_json",
            F.col("extracted_json").isNotNull()
            & (F.get_json_object(F.col("extracted_json"), "$") != F.lit(None)),
            "ERROR",
        ),
        _check(
            df, "doc_date_not_null_for_invoice",
            (F.col("document_type") != "invoice") | F.col("doc_date").isNotNull(),
            "WARN",
        ),
    ]

    combined = checks[0]
    for c in checks[1:]:
        combined = combined.unionByName(c)

    return (
        combined.withColumn("check_id", F.expr("uuid()"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("checked_at", F.current_timestamp())
        .select("check_id", "doc_id", "check_name", "passed", "severity", "details", "run_id", "checked_at")
    )
