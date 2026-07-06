"""
Tests for common.py — the authoritative FASTA parsing and sequence validation
pipeline shared by the DIAMOND orchestrator and legacy Lambda.

Run with: python -m pytest tests/
"""

import pytest
import sys
sys.path.insert(0, ".")

from common import parse_fasta, validate_sequence, VALID_AA, MIN_LEN, MAX_LEN, SEARCH_VERSION


# ── parse_fasta ──────────────────────────────────────────────────────────────

class TestParseFasta:
    def test_bare_sequence_gets_query_header(self):
        header, seq = parse_fasta("MKLLIVLLALAVAALHA")
        assert header == "query"
        assert seq == "MKLLIVLLALAVAALHA"

    def test_fasta_with_header(self):
        header, seq = parse_fasta(">sp|P12345|AAAA_HUMAN\nMKLLIVLLA")
        assert header == "sp|P12345|AAAA_HUMAN"
        assert seq == "MKLLIVLLA"

    def test_fasta_multiline_body(self):
        header, seq = parse_fasta(">myseq\nMKLLI\nVLLAL\nAVAAL")
        assert header == "myseq"
        assert seq == "MKLLIVLLALAVAAL"

    def test_strips_leading_trailing_whitespace(self):
        header, seq = parse_fasta("  MKLLIVLLALAVAALHA  ")
        assert header == "query"

    def test_multi_fasta_rejected(self):
        with pytest.raises(ValueError, match="Multi-FASTA"):
            parse_fasta(">seq1\nMKLLIVLLA\n>seq2\nACDEFGHIKL")

    def test_multi_fasta_three_records_rejected(self):
        with pytest.raises(ValueError, match="Multi-FASTA"):
            parse_fasta(">a\nACDE\n>b\nFGHI\n>c\nKLMN")

    def test_single_fasta_not_rejected(self):
        # One > should not trigger the multi-FASTA guard
        header, seq = parse_fasta(">only_one\nACDEFGHIKLMNPQRSTVWY")
        assert header == "only_one"

    def test_bare_sequence_with_newlines_in_body(self):
        # Bare sequence with internal newlines (no header)
        header, seq = parse_fasta("MKLLIVL\nLALAVAAL")
        assert header == "query"
        assert seq == "MKLLIVL\nLALAVAAL"  # raw, validate_sequence strips later


# ── validate_sequence ────────────────────────────────────────────────────────

class TestValidateSequence:
    def test_standard_20_aa(self):
        seq = validate_sequence("MKLLIVLLALAVAALHA")
        assert seq == "MKLLIVLLALAVAALHA"

    def test_returns_uppercase(self):
        seq = validate_sequence("mkllivllalavaalha")
        assert seq == seq.upper()

    def test_whitespace_stripped(self):
        seq = validate_sequence("MKL LIV LLAC")
        assert seq == "MKLLIVLLAC"

    def test_newlines_stripped(self):
        seq = validate_sequence("MKLLIVL\nLALAVAAL")
        assert seq == "MKLLIVLLALAVAAL"

    # Stop codon / gap stripping
    def test_stop_codon_stripped_silently(self):
        seq = validate_sequence("MKLLIVLLACC*")
        assert seq == "MKLLIVLLACC"
        assert "*" not in seq

    def test_gap_char_stripped_silently(self):
        seq = validate_sequence("MKLLIVLLACC-")
        assert seq == "MKLLIVLLACC"
        assert "-" not in seq

    def test_internal_stop_codons_stripped(self):
        seq = validate_sequence("MKL*LIV*LLAC")
        assert seq == "MKLLIVLLAC"

    def test_sequence_too_short_after_stripping(self):
        # 4 real residues + 6 stop codons — still too short after strip
        with pytest.raises(ValueError, match="too short"):
            validate_sequence("MKLL******")

    def test_only_stops_and_gaps_becomes_empty_then_too_short(self):
        with pytest.raises(ValueError, match="too short"):
            validate_sequence("*-*-*-*-*-*-")

    # IUPAC ambiguity codes
    def test_iupac_x_accepted(self):
        seq = validate_sequence("MKLLIVLLAXAVAALHA")
        assert "X" in seq

    def test_iupac_b_accepted(self):
        seq = validate_sequence("MKLLIVLLABAVAALHA")
        assert "B" in seq

    def test_iupac_z_accepted(self):
        seq = validate_sequence("MKLLIVLLAZAVAALHA")
        assert "Z" in seq

    def test_iupac_j_accepted(self):
        seq = validate_sequence("MKLLIVLLAJVAALHA")
        assert "J" in seq

    def test_iupac_u_accepted(self):
        seq = validate_sequence("MKLLIVLLAUAVAALHA")
        assert "U" in seq

    def test_iupac_o_accepted(self):
        seq = validate_sequence("MKLLIVLLAOAVAALHA")
        assert "O" in seq

    # Invalid characters
    def test_digit_rejected(self):
        with pytest.raises(ValueError, match="Invalid amino acid"):
            validate_sequence("MKLLIVLLA1AVAALHA")

    def test_special_char_rejected(self):
        with pytest.raises(ValueError, match="Invalid amino acid"):
            validate_sequence("MKLLIVLLA@AVAALHA")

    # Length bounds
    def test_too_short(self):
        with pytest.raises(ValueError, match="too short"):
            validate_sequence("MKLLIVL")  # 7 residues

    def test_exactly_min_length(self):
        seq = validate_sequence("ACDEFGHIKL")  # exactly 10
        assert len(seq) == MIN_LEN

    def test_too_long(self):
        with pytest.raises(ValueError, match="too long"):
            validate_sequence("A" * (MAX_LEN + 1))

    def test_exactly_max_length(self):
        seq = validate_sequence("A" * MAX_LEN)
        assert len(seq) == MAX_LEN


# ── VALID_AA constant ─────────────────────────────────────────────────────────

class TestValidAAConstant:
    def test_contains_all_standard_20(self):
        for aa in "ACDEFGHIKLMNPQRSTVWY":
            assert aa in VALID_AA, f"{aa} missing from VALID_AA"

    def test_contains_iupac_ambiguity_codes(self):
        for aa in "BJZXUO":
            assert aa in VALID_AA, f"IUPAC code {aa} missing from VALID_AA"

    def test_stop_codon_not_in_valid_aa(self):
        assert "*" not in VALID_AA

    def test_gap_not_in_valid_aa(self):
        assert "-" not in VALID_AA


# ── SEARCH_VERSION ────────────────────────────────────────────────────────────

def test_search_version_is_string():
    assert isinstance(SEARCH_VERSION, str)

def test_search_version_format():
    parts = SEARCH_VERSION.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)

def test_search_version_at_least_1_2_0():
    major, minor, patch = (int(p) for p in SEARCH_VERSION.split("."))
    assert (major, minor, patch) >= (1, 2, 0), \
        f"SEARCH_VERSION should be >= 1.2.0 after IUPAC/strip expansion; got {SEARCH_VERSION}"
