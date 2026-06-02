#!/usr/bin/env python3
"""
Shared, side-effect-free helpers for the PETadex search path.

These are the query-contract functions (FASTA parsing + validation) that BOTH
the orchestrator and the legacy Lambda must apply identically. They live here —
rather than being imported from `lambda_function.py` — because that module
eager-downloads the 3.2 GB MMseqs2 database at import time, which the DIAMOND
orchestrator/worker must never trigger.

During the transition `lambda_function.py` keeps its own inline copies (leaving
the live path untouched); at Phase 7 cutover that module is deleted and this
becomes the single source of truth. Keep the two in sync until then.
"""


def parse_fasta(raw):
    """Parse a FASTA string into (header, sequence). Also accepts bare sequence."""
    raw = raw.strip()
    if raw.startswith(">"):
        lines = raw.splitlines()
        header = lines[0][1:].strip()  # strip leading '>'
        sequence = "".join(lines[1:])
    else:
        header = None
        sequence = raw
    return header, sequence


def validate_sequence(sequence):
    """Validate input protein sequence; return the cleaned uppercase sequence."""
    # Remove whitespace and newlines
    sequence = "".join(sequence.split())

    # Check valid amino acids
    valid_aa = set("ACDEFGHIKLMNPQRSTVWY")
    if not all(c in valid_aa for c in sequence.upper()):
        raise ValueError("Invalid amino acid characters in sequence")

    # Check length
    if len(sequence) < 10:
        raise ValueError("Sequence too short (minimum 10 amino acids)")

    if len(sequence) > 10000:
        raise ValueError("Sequence too long (maximum 10,000 amino acids)")

    return sequence.upper()
