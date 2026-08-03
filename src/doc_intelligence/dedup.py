"""Duplicate / near-duplicate document detection.

Two passes:
1. EXACT_HASH — sha256 of normalized full_text, cheap and catches
   re-uploads of the identical file/content.
2. NEAR_DUPLICATE_EMBEDDING — pairwise cosine similarity over Vector
   Search embeddings for documents of the same type, catches things
   like a resubmitted invoice with a changed date/amount.
"""

from pyspark.sql import DataFrame, functions as F, Window


def find_exact_duplicates(df: DataFrame, text_col: str = "full_text") -> DataFrame:
    """df must contain doc_id, full_text. Returns pairs of doc_id /
    duplicate_of_doc_id for documents whose normalized text hash matches,
    keeping the earliest-seen document as the canonical one.
    """
    hashed = df.withColumn(
        "_norm_hash",
        F.sha2(F.lower(F.regexp_replace(F.col(text_col), r"\s+", " ")), 256),
    )

    w = Window.partitionBy("_norm_hash").orderBy("doc_id")
    ranked = hashed.withColumn("_rank", F.row_number().over(w))

    canonical = ranked.filter("_rank = 1").select(
        F.col("_norm_hash"), F.col("doc_id").alias("_canonical_doc_id")
    )
    dupes = (
        ranked.filter("_rank > 1")
        .join(canonical, on="_norm_hash")
        .select(
            F.col("doc_id"),
            F.col("_canonical_doc_id").alias("duplicate_of_doc_id"),
            F.lit(1.0).alias("similarity_score"),
            F.lit("EXACT_HASH").alias("match_type"),
        )
        .withColumn("detected_at", F.current_timestamp())
    )
    return dupes


def find_near_duplicates_from_search(
    spark,
    vsc,
    endpoint_name: str,
    index_fullname: str,
    chunks_df: DataFrame,
    similarity_threshold: float = 0.92,
    sample_per_type: int = 200,
) -> DataFrame:
    """For a sample of first-chunk text per document, queries the Vector
    Search index for near-duplicates within the same document_type and
    flags pairs above `similarity_threshold`. Returns a Spark DataFrame
    matching the gold_duplicate_documents schema (minus canonicalization
    beyond simple pairwise reporting).
    """
    first_chunks = chunks_df.filter("chunk_index = 0").select("doc_id", "document_type", "chunk_text")
    sample_window = Window.partitionBy("document_type").orderBy("doc_id")
    sampled = first_chunks.withColumn("_sample_rank", F.row_number().over(sample_window)).filter(
        F.col("_sample_rank") <= sample_per_type
    )

    index = vsc.get_index(endpoint_name=endpoint_name, index_name=index_fullname)
    rows = []
    seen_pairs = set()
    for row in sampled.collect():
        results = index.similarity_search(
            query_text=row["chunk_text"],
            columns=["doc_id", "document_type"],
            num_results=5,
            filters={"document_type": row["document_type"]} if row["document_type"] else None,
        )
        for match in results.get("result", {}).get("data_array", []):
            match_doc_id, _match_type, score = str(match[0]), match[1], float(match[-1])
            source_doc_id = str(row["doc_id"])
            if match_doc_id != source_doc_id and score >= similarity_threshold:
                pair = tuple(sorted([source_doc_id, match_doc_id]))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                rows.append(
                    (source_doc_id, match_doc_id, score, "NEAR_DUPLICATE_EMBEDDING")
                )

    schema = "doc_id STRING, duplicate_of_doc_id STRING, similarity_score DOUBLE, match_type STRING"
    result_df = spark.createDataFrame(rows, schema=schema) if rows else spark.createDataFrame([], schema=schema)
    return result_df.withColumn("detected_at", F.current_timestamp())
