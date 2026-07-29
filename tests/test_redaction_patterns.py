import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from doc_intelligence.redaction import REGEX_PATTERNS  # noqa: E402


def test_email_pattern_matches():
    assert re.search(REGEX_PATTERNS["EMAIL"], "contact us at jane.doe@example.com today")


def test_ssn_pattern_matches():
    assert re.search(REGEX_PATTERNS["SSN"], "SSN: 123-45-6789")


def test_ssn_pattern_does_not_match_plain_number():
    assert re.search(REGEX_PATTERNS["SSN"], "invoice total 1234567") is None


def test_phone_pattern_matches_common_formats():
    for candidate in ["555-123-4567", "(555) 123-4567", "+1 555 123 4567"]:
        assert re.search(REGEX_PATTERNS["PHONE"], candidate), candidate
