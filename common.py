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
#   1.1.0 — No functional/result change. Version-bump to validate that a
#           code-only deploy lands cleanly on the functions and the
#           search/version stamping flows through end-to-end (post-MMseqs2
#           decouple). Busts the cache (all queries re-run, identical results).
#   1.2.0 — Expanded input acceptance: full IUPAC ambiguity codes (B, J, Z, X,
#           U, O) now accepted; stop codons (*) and gap chars (-) silently
#           stripped before search. Previously-rejected sequences now succeed;
#           cache bust warranted.
#   1.3.0 — Hits now carry the target amino-acid sequence: the worker requests
#           DIAMOND's `full_sseq` column and the aggregator stamps it as
#           `target_sequence` on every hit. The HIT SET is unchanged (same
#           query + corpus -> same targets, scores and ranking) — but the cached
#           payload is not, so this MUST bump: without it the web app would keep
#           serving pre-1.3.0 cached results that have no sequences, which is
#           exactly the staleness the cache key exists to prevent.
SEARCH_VERSION = "1.3.0"

# Valid amino acid characters accepted by the search pipeline.
# Includes the 20 standard AAs plus IUPAC ambiguity codes DIAMOND handles natively.
# Stop codons (*) and gap characters (-) are stripped before this check.
VALID_AA = set("ACDEFGHIKLMNPQRSTVWYBJZXUO")
MIN_LEN = 10
MAX_LEN = 10_000


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
    """Parse a FASTA string into (header, sequence). Also accepts bare sequence.

    Raises ValueError for multi-FASTA input (multiple > records); only single
    sequences are supported by the DIAMOND fan-out architecture.
    """
    raw = raw.strip()
    if raw.startswith(">"):
        lines = raw.splitlines()
        header_count = sum(1 for line in lines if line.startswith(">"))
        if header_count > 1:
            raise ValueError(
                "Multi-FASTA input is not supported — please submit a single sequence."
            )
        header = lines[0][1:].strip()  # strip leading '>'
        sequence = "".join(lines[1:])
    else:
        header = "query"
        sequence = raw
    return header, sequence


def validate_sequence(sequence):
    """Validate input protein sequence; return the cleaned uppercase sequence.

    Strips stop codons (*) and gap characters (-) silently before validation.
    Accepts the full IUPAC ambiguity set (B, J, Z, X, U, O) in addition to
    the 20 standard amino acids.
    """
    # Remove whitespace and newlines
    sequence = "".join(sequence.split())

    # Strip stop codons and gap characters — not residues, but legal in pasted
    # translated ORFs and aligned sequences; DIAMOND should not receive them.
    sequence = sequence.replace("*", "").replace("-", "")

    # Check valid amino acids (standard 20 + IUPAC ambiguity codes)
    if not all(c in VALID_AA for c in sequence.upper()):
        raise ValueError("Invalid amino acid characters in sequence")

    # Check length on the cleaned body (after stop/gap stripping)
    if len(sequence) < MIN_LEN:
        raise ValueError(f"Sequence too short (minimum {MIN_LEN} amino acids)")

    if len(sequence) > MAX_LEN:
        raise ValueError(f"Sequence too long (maximum {MAX_LEN:,} amino acids)")

    return sequence.upper()
