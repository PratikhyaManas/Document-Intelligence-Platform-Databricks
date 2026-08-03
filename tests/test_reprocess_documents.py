import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.reprocess_documents import build_delete_statements  # noqa: E402


def test_build_delete_statements_batches_regular_table():
    statements = build_delete_statements(
        table_name="gold_extracted_fields",
        table_fullname="doc_intel.pipeline.gold_extracted_fields",
        doc_ids=["a", "b", "c"],
        batch_size=2,
    )

    assert len(statements) == 2
    assert "doc_id IN ('a', 'b')" in statements[0]
    assert "doc_id IN ('c')" in statements[1]


def test_build_delete_statements_handles_duplicate_link_column():
    statements = build_delete_statements(
        table_name="gold_duplicate_documents",
        table_fullname="doc_intel.pipeline.gold_duplicate_documents",
        doc_ids=["doc-1"],
        batch_size=500,
    )

    assert len(statements) == 1
    assert "doc_id IN ('doc-1')" in statements[0]
    assert "duplicate_of_doc_id IN ('doc-1')" in statements[0]


def test_build_delete_statements_escapes_single_quotes():
    statements = build_delete_statements(
        table_name="gold_extracted_fields",
        table_fullname="doc_intel.pipeline.gold_extracted_fields",
        doc_ids=["abc'123"],
        batch_size=500,
    )

    assert len(statements) == 1
    assert "'abc''123'" in statements[0]
