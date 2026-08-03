"""Zero-shot document classification via `ai_query()` against a
Databricks Foundation Model Serving endpoint — equivalent to
Snowflake's AI_CLASSIFY.
"""

from pyspark.sql import DataFrame, functions as F

from doc_intelligence.ai_utils import (
    add_prompt_column,
    add_truncated_text_column,
    ai_query_json_expr,
)

CLASSIFY_PROMPT_TEMPLATE = """You are a document classification engine.
Classify the document below into exactly one of these categories:
{categories}

Respond with ONLY a compact JSON object of the form:
{{"document_type": "<category>", "confidence": <0.0-1.0>}}

Document text (may be truncated):
---
{text}
---
"""


def classify_documents(
    df: DataFrame,
    text_col: str,
    llm_endpoint: str,
    categories: tuple,
    max_chars: int = 6000,
) -> DataFrame:
    """Adds document_type / confidence columns by prompting the LLM
    endpoint with each document's (truncated) text via ai_query().
    """
    categories_str = ", ".join(categories)

    truncated = add_truncated_text_column(df, source_col=text_col, max_chars=max_chars)

    prompt_prefix = (
        "You are a document classification engine.\n"
        f"Classify the document below into exactly one of these categories: {categories_str}.\n"
        'Respond with ONLY compact JSON: {"document_type": "<category>", "confidence": <0.0-1.0>}\n'
        "Document text (may be truncated):\n---\n"
    )
    prompted = add_prompt_column(truncated, prompt_prefix=prompt_prefix)

    # ai_query() takes the endpoint name and a column expression for the
    # prompt; responseFormat enforces JSON-mode decoding on supported models.
    classified = prompted.withColumn(
        "_response_json",
        ai_query_json_expr(llm_endpoint),
    )

    parsed = apply_classification_response(classified, categories=categories, response_col="_response_json")

    return (
        parsed.withColumn("classifier_model", F.lit(llm_endpoint))
        .withColumn("classified_at", F.current_timestamp())
        .select(
            "doc_id",
            "document_type",
            "confidence",
            "classifier_model",
            "classified_at",
        )
    )


def apply_classification_response(df: DataFrame, categories: tuple, response_col: str = "_response_json") -> DataFrame:
    """Parses and normalizes classification JSON response columns."""
    allowed_categories = [c.lower() for c in categories]
    return df.withColumn(
        "_document_type_raw",
        F.lower(F.trim(F.get_json_object(response_col, "$.document_type"))),
    ).withColumn(
        "document_type",
        F.when(F.col("_document_type_raw").isin(*allowed_categories), F.col("_document_type_raw")).otherwise(
            F.lit("other")
        ),
    ).withColumn(
        "confidence",
        F.coalesce(
            F.get_json_object(response_col, "$.confidence").cast("double"),
            F.lit(0.0),
        ),
    )
