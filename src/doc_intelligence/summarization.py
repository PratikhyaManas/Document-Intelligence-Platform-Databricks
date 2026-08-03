"""Document summarization via `ai_query()`."""

from pyspark.sql import DataFrame, functions as F

from doc_intelligence.ai_utils import (
    add_prompt_column,
    add_truncated_text_column,
    ai_query_json_expr,
)

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
    truncated = add_truncated_text_column(df, source_col=text_col, max_chars=max_chars)
    prompted = add_prompt_column(truncated, prompt_prefix=SUMMARY_PROMPT)
    responded = prompted.withColumn(
        "_response",
        ai_query_json_expr(llm_endpoint),
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
