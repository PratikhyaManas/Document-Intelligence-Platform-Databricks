"""Document parsing helpers built on Databricks' native `ai_parse_document`
SQL function (the direct equivalent of Snowflake's AI_PARSE_DOCUMENT).

`ai_parse_document` runs on serverless compute inside a SQL/DataFrame
expression, taking file bytes and returning a STRUCT with layout-aware
text, pages, tables and bounding boxes as JSON — no external OCR service
or model serving endpoint required for the parse step itself.
"""

from pyspark.sql import DataFrame, functions as F


def parse_documents(df: DataFrame, content_col: str = "content") -> DataFrame:
    """Run ai_parse_document over a DataFrame of raw file bytes.

    Expects `df` to contain at least: doc_id, file_name, content (BINARY).
    Returns doc_id, file_name, page_count, full_text, layout_json,
    parse_status, parse_error.
    """
    parsed = df.withColumn(
        "_parsed",
        F.expr(f"try_ai_parse_document({content_col}, map('mode', 'LAYOUT'))"),
    )

    result = parsed.select(
        "doc_id",
        "file_name",
        F.col("_parsed.document.pages").alias("_pages"),
        F.to_json(F.col("_parsed")).alias("layout_json"),
        F.when(F.col("_parsed").isNotNull(), F.lit("SUCCESS"))
        .otherwise(F.lit("FAILED"))
        .alias("parse_status"),
        F.col("_parsed.error").alias("parse_error"),
    )

    result = result.withColumn(
        "page_count", F.when(F.col("_pages").isNotNull(), F.size("_pages")).otherwise(0)
    ).withColumn(
        "full_text",
        F.when(
            F.col("_pages").isNotNull(),
            F.array_join(F.expr("transform(_pages, p -> p.text)"), "\n\n"),
        ).otherwise(F.lit(None)),
    ).drop("_pages")

    return result.withColumn("parsed_at", F.current_timestamp())


def chunk_text(text: str, chunk_size_tokens: int = 512, overlap_tokens: int = 64):
    """Simple whitespace-token chunker with overlap, used to build the
    corpus for Vector Search. Good enough for most business documents;
    swap in a tokenizer-aware splitter (e.g. tiktoken/langchain) for
    tighter token budgeting.
    """
    if not text:
        return []

    words = text.split()
    step = max(chunk_size_tokens - overlap_tokens, 1)
    chunks = []
    for start in range(0, len(words), step):
        piece = words[start : start + chunk_size_tokens]
        if not piece:
            continue
        chunks.append(" ".join(piece))
        if start + chunk_size_tokens >= len(words):
            break
    return chunks
