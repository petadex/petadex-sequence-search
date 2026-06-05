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

import re

# Version of the SEARCH PIPELINE itself (engine + sensitivity + scoring), as
# distinct from the DATABASE version (the corpus build). Bump this by hand
# whenever a change would alter results for the SAME query against the SAME
# corpus — e.g. the §10.10 sensitivity switch (--very-sensitive → default), a
# block-size/scoring change, or an engine swap. It is stamped into every result
# (`search_version`) and is one of the cache-key inputs, so bumping it correctly
# invalidates stale cached results that a database-version change alone wouldn't.
#
#   1.0.0 — DIAMOND blastp, default (fast) sensitivity, -b 1, full-corpus
#           --dbsize e-value calibration. (Initial versioned release.)
SEARCH_VERSION = "1.0.0"


def parse_database_release(corpus):
    """Extract the semantic corpus release (e.g. "v1.1") from a corpus path/tag.

    The release version lives in the corpus FASTA filename, which is also
    embedded in the build version tag, e.g.:
        s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa  -> "v1.1"
        catalytic_orfs_v1.1_20260602_222538                -> "v1.1"

    Returns the matched "vMAJOR[.MINOR...]" string, or None if absent. This is a
    fallback for manifests built before the builder started writing an explicit
    `database_release` field, so the live (already-published) manifest resolves
    a clean release without a 307M rebuild.
    """
    if not corpus:
        return None
    m = re.search(r"v\d+(?:\.\d+)*", corpus)
    return m.group(0) if m else None


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
