"""
Local tests for Lambda function
Run with: python -m pytest tests/
"""

import json
import sys
sys.path.insert(0, '.')

# common.py is the canonical source; lambda_function.py keeps inline copies
# (Phase 7 will delete them and make this import canonical everywhere).
from common import validate_sequence, parse_fasta


def test_validate_sequence():
    """Test sequence validation"""
    # Valid sequence
    seq = validate_sequence("MKLLIVLLALAVAALHA")
    assert seq == "MKLLIVLLALAVAALHA"

    # With whitespace
    seq = validate_sequence("MKL LIV LLAC")
    assert seq == "MKLLIVLLAC"

    # Stop codon stripped silently
    seq = validate_sequence("MKLLIVLLACC*")
    assert seq == "MKLLIVLLACC"

    # Gap stripped silently
    seq = validate_sequence("MKL-LIV-LLAC")
    assert seq == "MKLLIVLLAC"

    # IUPAC codes accepted
    seq = validate_sequence("MKLLIVLLAXBZJUO")
    assert "X" in seq and "B" in seq

    # Too short
    try:
        validate_sequence("MKL")
        assert False, "Should raise error"
    except ValueError:
        pass

    # Too short after stop-codon stripping
    try:
        validate_sequence("MK*")
        assert False, "Should raise error after stripping"
    except ValueError:
        pass


def test_parse_fasta_bare_sequence_header():
    """Bare sequence should get 'query' as header (not None)."""
    header, seq = parse_fasta("MKLLIVLLALAVAALHA")
    assert header == "query"


def test_parse_fasta_multi_fasta_rejected():
    """Multi-FASTA input should raise ValueError."""
    try:
        parse_fasta(">seq1\nMKLLIVLLA\n>seq2\nACDEFGHIKL")
        assert False, "Should raise ValueError"
    except ValueError as e:
        assert "Multi-FASTA" in str(e)


# The handler test requires boto3 (Lambda environment) — skipped in local dev.
# handler integration is validated by the CI deploy + example-regen step.
