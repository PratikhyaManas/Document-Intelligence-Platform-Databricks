"""PII detection & redaction via `ai_query()` — equivalent to Snowflake's
AI_REDACT. Combines a fast regex pre-pass (emails, phone numbers, SSNs,
credit cards) with an LLM pass for context-dependent PII (names,
addresses, DOB) that regex can't reliably catch.
"""

from pyspark.sql import DataFrame, functions as F

REGEX_PATTERNS = {
    "EMAIL": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    "PHONE": r"\b(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b",
    "SSN": r"\b\d{3}-\d{2}-\d{4}\b",
    "CREDIT_CARD": r"\b(?:\d[ -]*?){13,16}\b",
}

REDACT_PROMPT = (
    "Identify personally identifiable information (PII) in the text below: "
    "full names, home addresses, dates of birth, government ID numbers, "
    "and bank account numbers. Respond with ONLY compact JSON of the form "
    '{"entities": [{"type": "<TYPE>", "value": "<the exact text found>"}]}. '
    "Types must be one of: PERSON_NAME, ADDRESS, DATE_OF_BIRTH, GOVERNMENT_ID, "
    "BANK_ACCOUNT. If none found, return {\"entities\": []}.\n\nText:\n---\n"
)


def _apply_regex_redactions(df: DataFrame, text_col: str) -> DataFrame:
    result = df.withColumn("_redacted_regex", F.col(text_col))
    for pii_type, pattern in REGEX_PATTERNS.items():
        result = result.withColumn(
            "_redacted_regex",
            F.regexp_replace(F.col("_redacted_regex"), pattern, f"[REDACTED:{pii_type}]"),
        )
    return result


def redact_documents(
    df: DataFrame,
    text_col: str,
    llm_endpoint: str,
    max_chars: int = 8000,
) -> DataFrame:
    """Returns doc_id, redacted_text, pii_entities_json, pii_types_found,
    contains_pii, redaction_model, redacted_at.
    """
    regex_pass = _apply_regex_redactions(df, text_col)

    truncated = regex_pass.withColumn(
        "_truncated_text", F.substring(F.col(text_col), 1, max_chars)
    )
    prompted = truncated.withColumn(
        "_prompt", F.concat(F.lit(REDACT_PROMPT), F.col("_truncated_text"), F.lit("\n---"))
    )
    llm_pass = prompted.withColumn(
        "_pii_response",
        F.expr(
            f"ai_query('{llm_endpoint}', _prompt, responseFormat => "
            "'{\"type\": \"json_object\"}')"
        ),
    )

    # Redact each LLM-identified entity value out of the regex-redacted text.
    with_entities = llm_pass.withColumn(
        "_entity_values",
        F.expr(
            "transform(from_json(_pii_response, 'entities ARRAY<STRUCT<type:STRING,value:STRING>>').entities, "
            "e -> e.value)"
        ),
    ).withColumn(
        "_entity_types",
        F.expr(
            "transform(from_json(_pii_response, 'entities ARRAY<STRUCT<type:STRING,value:STRING>>').entities, "
            "e -> e.type)"
        ),
    )

    final_redacted = with_entities.withColumn(
        "redacted_text",
        F.expr(
            "aggregate(_entity_values, _redacted_regex, "
            "(acc, v) -> replace(acc, v, '[REDACTED:PII]'))"
        ),
    )

    result = (
        final_redacted.withColumn("pii_entities_json", F.col("_pii_response"))
        .withColumn(
            "pii_types_found",
            F.array_union(F.coalesce(F.col("_entity_types"), F.array()), F.array()),
        )
        .withColumn(
            "contains_pii",
            (F.size(F.coalesce(F.col("_entity_types"), F.array())) > 0)
            | F.col("_redacted_regex").contains("[REDACTED:"),
        )
        .withColumn("redaction_model", F.lit(llm_endpoint))
        .withColumn("redacted_at", F.current_timestamp())
    )

    return result.select(
        "doc_id",
        "redacted_text",
        "pii_entities_json",
        "pii_types_found",
        "contains_pii",
        "redaction_model",
        "redacted_at",
    )
