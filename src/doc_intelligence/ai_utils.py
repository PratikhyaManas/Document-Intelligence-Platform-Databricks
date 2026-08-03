"""Shared helpers for LLM prompt assembly and JSON-mode ai_query calls."""

from pyspark.sql import DataFrame, Column, functions as F


def add_truncated_text_column(
    df: DataFrame,
    source_col: str,
    max_chars: int,
    target_col: str = "_truncated_text",
) -> DataFrame:
    """Adds a consistently truncated text column used by prompt builders."""
    return df.withColumn(target_col, F.substring(F.col(source_col), 1, max_chars))


def add_prompt_column(
    df: DataFrame,
    prompt_prefix: str | Column,
    text_col: str = "_truncated_text",
    target_col: str = "_prompt",
) -> DataFrame:
    """Builds the final model prompt from a prefix and truncated text."""
    prefix_expr = prompt_prefix if isinstance(prompt_prefix, Column) else F.lit(prompt_prefix)
    return df.withColumn(target_col, F.concat(prefix_expr, F.col(text_col), F.lit("\n---")))


def ai_query_json_expr(endpoint_name: str, prompt_col: str = "_prompt") -> Column:
    """Returns a Spark SQL expression that enforces JSON object responses."""
    return F.expr(build_ai_query_json_sql(endpoint_name, prompt_col))


def build_ai_query_json_sql(endpoint_name: str, prompt_col: str = "_prompt") -> str:
    """Builds the SQL expression string for ai_query in JSON mode."""
    escaped_endpoint = endpoint_name.replace("'", "''")
    return (
        f"ai_query('{escaped_endpoint}', {prompt_col}, responseFormat => "
        "'{\"type\": \"json_object\"}')"
    )
