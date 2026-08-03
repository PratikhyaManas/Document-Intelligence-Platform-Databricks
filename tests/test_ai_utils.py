import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from doc_intelligence.ai_utils import build_ai_query_json_sql  # type: ignore  # noqa: E402
from doc_intelligence.redaction import REGEX_PATTERNS, REGEX_PII_TYPES  # type: ignore  # noqa: E402


def test_build_ai_query_json_sql_contains_json_mode():
    sql = build_ai_query_json_sql("endpoint")
    assert "ai_query('endpoint', _prompt" in sql
    assert '"type": "json_object"' in sql


def test_build_ai_query_json_sql_escapes_quotes_in_endpoint_name():
    sql = build_ai_query_json_sql("my'endpoint")
    assert "my''endpoint" in sql


def test_regex_types_tracks_patterns_keys():
    assert REGEX_PII_TYPES == tuple(REGEX_PATTERNS.keys())
