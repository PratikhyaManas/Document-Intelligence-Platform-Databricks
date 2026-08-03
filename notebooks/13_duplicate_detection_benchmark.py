# Databricks notebook source
# MAGIC %md
# MAGIC # 13 · Duplicate Detection Benchmark
# MAGIC
# MAGIC Compares a baseline near-duplicate search loop versus the optimized
# MAGIC implementation from `src/doc_intelligence/dedup.py`.
# MAGIC
# MAGIC The benchmark uses synthetic chunk rows and a mock Vector Search index
# MAGIC to focus on control-plane overhead (index lookup frequency, pair de-dup,
# MAGIC and per-type sampling behavior).

# COMMAND ----------
import random
import string
import time

from pyspark.sql import functions as F

import sys
sys.path.append("../src")

from doc_intelligence.dedup import find_near_duplicates_from_search  # type: ignore  # noqa: E402

spark = globals().get("spark")
if spark is None:
    raise RuntimeError("This notebook must run in a Databricks notebook session.")

# COMMAND ----------
# Synthetic chunk data: many docs across multiple types.
doc_types = ["invoice", "contract", "resume"]
n_per_type = 250

rows = []
for dt in doc_types:
    for i in range(n_per_type):
        doc_id = f"{dt}-{i:04d}"
        base = " ".join(random.choices(string.ascii_lowercase, k=300))
        chunk_text = f"{dt} {base}"
        rows.append((doc_id, dt, 0, chunk_text))

chunks_df = spark.createDataFrame(rows, ["doc_id", "document_type", "chunk_index", "chunk_text"])
print(f"Synthetic docs: {chunks_df.count()}")

# COMMAND ----------
class MockIndex:
    def similarity_search(self, query_text, columns, num_results=5, filters=None):
        # Return deterministic pseudo-matches where many pairs overlap,
        # highlighting pair de-dup overhead in the baseline flow.
        qhash = abs(hash(query_text)) % 500
        dt = (filters or {}).get("document_type", "invoice")
        data = []
        for k in range(num_results):
            match_doc = f"{dt}-{(qhash + k) % n_per_type:04d}"
            score = 0.97 - (k * 0.01)
            data.append([match_doc, dt, score])
        return {"result": {"data_array": data}}


class MockVectorSearchClient:
    def __init__(self):
        self.index_calls = 0
        self._index = MockIndex()

    def get_index(self, endpoint_name, index_name):
        self.index_calls += 1
        return self._index

# COMMAND ----------
def baseline_near_duplicates(
    spark,
    vsc,
    endpoint_name,
    index_fullname,
    chunks_df,
    similarity_threshold=0.92,
    sample_per_type=200,
):
    # Mirrors the previous baseline approach.
    first_chunks = (
        chunks_df.filter("chunk_index = 0")
        .select("doc_id", "document_type", "chunk_text")
        .limit(sample_per_type * 10)
    )

    rows = []
    for row in first_chunks.collect():
        index = vsc.get_index(endpoint_name=endpoint_name, index_name=index_fullname)
        results = index.similarity_search(
            query_text=row["chunk_text"],
            columns=["doc_id", "document_type"],
            num_results=5,
            filters={"document_type": row["document_type"]} if row["document_type"] else None,
        )
        for match in results.get("result", {}).get("data_array", []):
            match_doc_id, _match_type, score = match[0], match[1], match[-1]
            if match_doc_id != row["doc_id"] and score >= similarity_threshold:
                rows.append((row["doc_id"], match_doc_id, float(score), "NEAR_DUPLICATE_EMBEDDING"))

    schema = "doc_id STRING, duplicate_of_doc_id STRING, similarity_score DOUBLE, match_type STRING"
    result_df = spark.createDataFrame(rows, schema=schema) if rows else spark.createDataFrame([], schema=schema)
    return result_df.withColumn("detected_at", F.current_timestamp())

# COMMAND ----------
endpoint_name = "mock-endpoint"
index_name = "mock-index"

# Baseline
vsc_baseline = MockVectorSearchClient()
t0 = time.perf_counter()
baseline_df = baseline_near_duplicates(
    spark,
    vsc_baseline,
    endpoint_name,
    index_name,
    chunks_df,
    similarity_threshold=0.92,
    sample_per_type=200,
)
baseline_count = baseline_df.count()
baseline_sec = time.perf_counter() - t0

# Optimized
vsc_opt = MockVectorSearchClient()
t1 = time.perf_counter()
optimized_df = find_near_duplicates_from_search(
    spark,
    vsc_opt,
    endpoint_name,
    index_name,
    chunks_df,
    similarity_threshold=0.92,
    sample_per_type=200,
)
optimized_count = optimized_df.count()
optimized_sec = time.perf_counter() - t1

# COMMAND ----------
summary = spark.createDataFrame(
    [
        ("baseline", baseline_sec, vsc_baseline.index_calls, baseline_count),
        ("optimized", optimized_sec, vsc_opt.index_calls, optimized_count),
    ],
    ["variant", "elapsed_seconds", "index_get_calls", "result_rows"],
)

summary.orderBy("variant").show(truncate=False)

print("\nExpected trend:")
print("- Optimized should use far fewer get_index calls (typically 1).")
print("- Optimized result_rows are often lower due to pair de-duplication.")
print("- Elapsed time should improve as input volume grows.")
