"""Zero-shot document classification via `ai_query()` against a
Databricks Foundation Model Serving endpoint — equivalent to
Snowflake's AI_CLASSIFY.
"""

from pyspark.sql import DataFrame, functions as F

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

    truncated = df.withColumn(
        "_truncated_text", F.substring(F.col(text_col), 1, max_chars)
    )

    prompt_expr = F.concat(
        F.lit(
            "You are a document classification engine.\n"
            f"Classify the document below into exactly one of these categories: {categories_str}.\n"
            'Respond with ONLY compact JSON: {"document_type": "<category>", "confidence": <0.0-1.0>}\n'
            "Document text (may be truncated):\n---\n"
        ),
        F.col("_truncated_text"),
        F.lit("\n---"),
    )

    # ai_query() takes the endpoint name and a column expression for the
    # prompt; responseFormat enforces JSON-mode decoding on supported models.
    classified = truncated.withColumn("_prompt", prompt_expr).withColumn(
        "_response_json",
        F.expr(
            f"ai_query('{llm_endpoint}', _prompt, responseFormat => "
            "'{\"type\": \"json_object\"}')"
        ),
    )

    parsed = classified.withColumn(
        "document_type",
        F.coalesce(
            F.get_json_object("_response_json", "$.document_type"), F.lit("other")
        ),
    ).withColumn(
        "confidence",
        F.coalesce(
            F.get_json_object("_response_json", "$.confidence").cast("double"),
            F.lit(0.0),
        ),
    )

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
