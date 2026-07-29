"""Structured field extraction via `ai_query()`, with a per-document-type
JSON schema — equivalent to Snowflake's schema-based extraction functions.

Add/edit entries in EXTRACTION_SCHEMAS to support new document types.
"""

from pyspark.sql import DataFrame, functions as F

EXTRACTION_SCHEMAS = {
    "invoice": {
        "vendor_name": "string",
        "invoice_number": "string",
        "invoice_date": "string (YYYY-MM-DD)",
        "due_date": "string (YYYY-MM-DD)",
        "total_amount": "number",
        "currency": "string (ISO 4217 code)",
        "line_items": "array of {description, quantity, unit_price, amount}",
    },
    "contract": {
        "parties": "array of strings",
        "effective_date": "string (YYYY-MM-DD)",
        "termination_date": "string (YYYY-MM-DD) or null",
        "contract_value": "number or null",
        "currency": "string or null",
        "governing_law": "string or null",
        "key_obligations": "array of strings, max 5",
    },
    "resume": {
        "candidate_name": "string",
        "email": "string or null",
        "phone": "string or null",
        "total_years_experience": "number",
        "skills": "array of strings",
        "most_recent_title": "string or null",
        "most_recent_employer": "string or null",
    },
    "financial_report": {
        "reporting_period": "string",
        "company_name": "string or null",
        "total_revenue": "number or null",
        "net_income": "number or null",
        "currency": "string or null",
    },
    "id_document": {
        "document_number": "string",
        "full_name": "string",
        "date_of_birth": "string (YYYY-MM-DD) or null",
        "expiry_date": "string (YYYY-MM-DD) or null",
        "issuing_country": "string or null",
    },
    "other": {
        "summary": "string, max 3 sentences",
        "key_entities": "array of strings",
    },
}


def _schema_to_prompt_fragment(schema: dict) -> str:
    lines = [f'  "{k}": {v}' for k, v in schema.items()]
    return "{\n" + ",\n".join(lines) + "\n}"


def extract_fields(
    df: DataFrame,
    text_col: str,
    doc_type_col: str,
    llm_endpoint: str,
    max_chars: int = 12000,
) -> DataFrame:
    """Adds an `extracted_json` column plus a few promoted columns
    (vendor_or_party, amount_total, currency, doc_date) by prompting
    the LLM with a schema tailored to each row's document_type.
    """
    when_expr = None
    for doc_type, schema in EXTRACTION_SCHEMAS.items():
        fragment = _schema_to_prompt_fragment(schema)
        instruction = (
            "Extract the following fields from the document text as a single "
            f"JSON object matching this exact shape (use null when a field is "
            f"not present):\n{fragment}\n\nDocument text (may be truncated):\n---\n"
        )
        branch = F.when(F.col(doc_type_col) == doc_type, F.lit(instruction))
        when_expr = branch if when_expr is None else when_expr.when(
            F.col(doc_type_col) == doc_type, F.lit(instruction)
        )
    when_expr = when_expr.otherwise(F.lit(_schema_to_prompt_fragment(EXTRACTION_SCHEMAS["other"])))

    truncated = df.withColumn("_truncated_text", F.substring(F.col(text_col), 1, max_chars))
    prompted = truncated.withColumn(
        "_prompt",
        F.concat(when_expr, F.col("_truncated_text"), F.lit("\n---")),
    )

    extracted = prompted.withColumn(
        "_response_json",
        F.expr(
            f"ai_query('{llm_endpoint}', _prompt, responseFormat => "
            "'{\"type\": \"json_object\"}')"
        ),
    )

    result = (
        extracted.withColumn("extracted_json", F.col("_response_json"))
        .withColumn(
            "vendor_or_party",
            F.coalesce(
                F.get_json_object("_response_json", "$.vendor_name"),
                F.get_json_object("_response_json", "$.parties[0]"),
                F.get_json_object("_response_json", "$.candidate_name"),
                F.get_json_object("_response_json", "$.company_name"),
            ),
        )
        .withColumn(
            "amount_total",
            F.coalesce(
                F.get_json_object("_response_json", "$.total_amount").cast("double"),
                F.get_json_object("_response_json", "$.contract_value").cast("double"),
                F.get_json_object("_response_json", "$.total_revenue").cast("double"),
            ),
        )
        .withColumn("currency", F.get_json_object("_response_json", "$.currency"))
        .withColumn(
            "doc_date",
            F.coalesce(
                F.get_json_object("_response_json", "$.invoice_date"),
                F.get_json_object("_response_json", "$.effective_date"),
            ).cast("date"),
        )
        .withColumn("extraction_model", F.lit(llm_endpoint))
        .withColumn("extracted_at", F.current_timestamp())
    )

    return result.select(
        "doc_id",
        doc_type_col,
        "extracted_json",
        "vendor_or_party",
        "amount_total",
        "currency",
        "doc_date",
        "extraction_model",
        "extracted_at",
    ).withColumnRenamed(doc_type_col, "document_type")
