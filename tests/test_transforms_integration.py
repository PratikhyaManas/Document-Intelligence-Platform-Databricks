import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from doc_intelligence.classification import apply_classification_response  # type: ignore  # noqa: E402
from doc_intelligence.redaction import apply_redaction_response  # type: ignore  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    try:
        session = (
            pyspark.sql.SparkSession.builder.master("local[1]")
            .appName("doc-intel-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Spark unavailable: {exc}")
    yield session
    session.stop()


def test_classification_response_normalizes_and_falls_back(spark):
    rows = [
        ("{\"document_type\":\"Invoice\",\"confidence\":0.91}",),
        ("{\"document_type\":\"Memo\",\"confidence\":0.72}",),
        ("{\"confidence\":0.33}",),
    ]
    df = spark.createDataFrame(rows, ["_response_json"])

    out = apply_classification_response(df, categories=("invoice", "contract", "other"))
    collected = out.select("document_type", "confidence").collect()

    assert collected[0]["document_type"] == "invoice"
    assert collected[1]["document_type"] == "other"
    assert collected[2]["document_type"] == "other"
    assert collected[2]["confidence"] == pytest.approx(0.33)


def test_redaction_response_merges_llm_and_regex_types(spark):
    rows = [
        (
            "Contact [REDACTED:EMAIL] for Jane Doe",
            "{\"entities\":[{\"type\":\"PERSON_NAME\",\"value\":\"Jane Doe\"}]}",
        )
    ]
    df = spark.createDataFrame(rows, ["_redacted_regex", "_pii_response"])

    out = apply_redaction_response(df)
    row = out.select("redacted_text", "pii_types_found", "contains_pii").collect()[0]

    assert row["redacted_text"] == "Contact [REDACTED:EMAIL] for [REDACTED:PII]"
    assert "EMAIL" in row["pii_types_found"]
    assert "PERSON_NAME" in row["pii_types_found"]
    assert row["contains_pii"] is True
