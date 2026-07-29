"""Document summarization via `ai_query()`."""

from pyspark.sql import DataFrame, functions as F

SUMMARY_PROMPT = (
    "Summarize the document below in 2-4 sentences for a busy executive, "
    "then list up to 5 key points. Respond with ONLY compact JSON: "
    '{"summary": "...", "key_points": ["...", "..."], '
    '"sentiment": "positive|neutral|negative|n/a"}. '
    "Use sentiment only if the document expresses an opinion or review; "
    "otherwise use \"n/a\".\n\nDocument text (may be truncated):\n---\n"
)


def summarize_documents(
    df: DataFrame,
    text_col: str,
    llm_endpoint: str,
    max_chars: int = 10000,
) -> DataFrame:
    truncated = df.withColumn("_truncated_text", F.substring(F.col(text_col), 1, max_chars))
    prompted = truncated.withColumn(
        "_prompt", F.concat(F.lit(SUMMARY_PROMPT), F.col("_truncated_text"), F.lit("\n---"))
    )
    responded = prompted.withColumn(
        "_response",
        F.expr(
            f"ai_query('{llm_endpoint}', _prompt, responseFormat => "
            "'{\"type\": \"json_object\"}')"
        ),
    )

    result = (
        responded.withColumn("summary", F.get_json_object("_response", "$.summary"))
        .withColumn(
            "key_points",
            F.from_json(F.get_json_object("_response", "$.key_points"), "ARRAY<STRING>"),
        )
        .withColumn("sentiment", F.coalesce(F.get_json_object("_response", "$.sentiment"), F.lit("n/a")))
        .withColumn("summary_model", F.lit(llm_endpoint))
        .withColumn("summarized_at", F.current_timestamp())
    )

    return result.select(
        "doc_id", "summary", "key_points", "sentiment", "summary_model", "summarized_at"
    )
