import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from doc_intelligence.parsing import chunk_text  # noqa: E402


def test_chunk_text_empty():
    assert chunk_text("") == []
    assert chunk_text(None) == []


def test_chunk_text_single_chunk():
    text = "word " * 10
    chunks = chunk_text(text, chunk_size_tokens=512, overlap_tokens=64)
    assert len(chunks) == 1


def test_chunk_text_multiple_chunks_with_overlap():
    words = [f"w{i}" for i in range(1000)]
    text = " ".join(words)
    chunks = chunk_text(text, chunk_size_tokens=100, overlap_tokens=20)
    assert len(chunks) > 1
    # Overlap: last few words of one chunk should reappear at the start of the next
    first_chunk_words = chunks[0].split()
    second_chunk_words = chunks[1].split()
    assert first_chunk_words[-1] in second_chunk_words[: 20 + 1]
