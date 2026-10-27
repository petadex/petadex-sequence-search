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
#   1.4.0 — Result count decoupled from the search: every job now searches to a
#           fixed server-side RESULT_DEPTH (below) and the caller's
#           `max_results` becomes purely presentational (the web app subsets at
#           view time). Same query + corpus -> the same ranked hits, but the
#           stored payload now holds RESULT_DEPTH rows instead of the caller's
#           count, so this MUST bump: without it a cached 50-hit result would
#           keep being served to a request that asks for 200.
SEARCH_VERSION = "1.4.0"

# Depth of the SEARCH itself — how many hits every job computes and stores,
# regardless of how many the caller asks to display. Set once here, injected by
# the orchestrator into the execution input, and used unchanged downstream as
# the worker's per-shard `-k` and the aggregator's final truncation.
#
# WHY A CONSTANT, not the caller's `max_results`: `max_results` used to be both
# a search parameter AND a cache-key input, so changing the displayed count
# re-ran the entire 32-shard fan-out (~46 s, 32 Lambdas) to surface rows the
# previous job had already computed and discarded at the truncation step. The
# fan-out's cost is fixed (shard download + seed-index rebuild) and is
# essentially independent of `-k`, so it is far cheaper to search deep ONCE and
# let the web app slice. See the vault note "12 Variable Result Count".
#
# WHY 500: it is exactly the cap the web app's request schema already allows
# (`max_results` Joi max(500)), so making it the fixed depth means no query
# becomes less capable than it is today. It is a CEILING, not a quota — the
# worker passes no `--evalue`, so DIAMOND's default 1e-3 cutoff usually returns
# fewer hits than this anyway (`num_results < result_depth` in a result means
# the search was exhaustive and ANY display count is servable from it).
#
# Changing this value does NOT require a SEARCH_VERSION bump: every result
# carries its own `result_depth`, so the web app re-runs only those queries
# whose stored depth is too shallow for what is being asked. Never set it to 0
# / unlimited — against a 307M-sequence corpus a promiscuous query would return
# an unbounded payload, and since 1.3.0 every hit carries a full protein.
RESULT_DEPTH = 500

# --- Export of ALL significant hits (on demand, separate from the search) ---
#
# The search above stops at RESULT_DEPTH per shard, so DIAMOND never computes
# the rest. An export re-runs the fan-out with no hit cap (`-k 0`), and the
# aggregator merges the 32 sorted parts into a gzipped TSV of every hit, a
# gzipped FASTA of their full sequences in the same order, and a summary of
# counts. Live (2026-10-27): ~31k hits (SRR10663367) to ~7.9M (a C003 query) at
# e <= 1e-3; with sequences the largest export is ~1.5 GB — why it is opt-in
# rather than part of every search.
#
# EXPORT_DEPTH rides the existing `maxResults` field through the state machine
# (0 = DIAMOND's "no limit"), so exports need no ASL change. The orchestrator
# injects it; nothing else ever produces maxResults == 0.
EXPORT_DEPTH = 0
# Exports run under their own sessionId so a failed export writes its own
# `.error` sentinel instead of failing the search it belongs to. Two characters
# because public execution names (`{tag}_{sessionId}-{jobId}`) must fit in 80.
EXPORT_SESSION_SUFFIX = "_x"
# Version of the export file format (columns, header, summary), stamped into
# every summary. Bump when a column or the summary shape changes.
#   1.0 — hits.tsv.gz + summary.
#   1.1 — adds hits.fasta.gz (full target sequences, rank order); an export
#         without it is stale and is rebuilt (orchestrator.find_export).
EXPORT_FORMAT_VERSION = "1.1"
# DIAMOND columns the worker writes for an export part, in this order. The full
# target sequence is last; it goes to the FASTA, not the TSV.
EXPORT_OUTFMT = ["sseqid", "qstart", "qend", "sstart", "send", "length",
                 "pident", "evalue", "bitscore", "slen", "full_sseq"]
# Columns of the merged file: rank + target id + origin, then the alignment.
EXPORT_COLUMNS = ["rank", "target_id", "origin", "pident", "aln_len", "qstart",
                  "qend", "sstart", "send", "slen", "evalue", "bitscore"]
# E-value bands reported in the summary (hits at or below each).
EXPORT_EVALUE_BANDS = (1e-3, 1e-10, 1e-30, 1e-50)
_ORIGIN_BY_PREFIX = {"PL": "plasticome", "NR": "ncbi_nr", "LG": "logan"}


def export_rank_key(fields):
    """Sort key for export rows: the search's own ranking (bitscore desc, then
    e-value asc, as aggregator.handler ranks), then target id so ties are stable.
    `fields` is an EXPORT_OUTFMT row: bitscore = [8], evalue = [7], sseqid = [0]."""
    return (-float(fields[8]), float(fields[7]), fields[0])


def sort_export_rows(tsv):
    """Sort an export part's outfmt-6 text by export_rank_key; returns text."""
    rows = [line.split("\t") for line in tsv.splitlines() if line.strip()]
    rows.sort(key=export_rank_key)
    return "".join("\t".join(r) + "\n" for r in rows)


def export_session_id(session_id):
    """sessionId an export of `session_id` runs (and is stored) under."""
    return f"{session_id}{EXPORT_SESSION_SUFFIX}"


def hit_origin(target_id):
    """Source of a hit from its v260701 petadex_id prefix (PL/NR/LG + digits).
    v1.1-format IDs (`orf||run|…`) carry no prefix and return "unknown"."""
    prefix, rest = target_id[:2], target_id[2:]
    if prefix in _ORIGIN_BY_PREFIX and rest.isdigit():
        return _ORIGIN_BY_PREFIX[prefix]
    return "unknown"

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
