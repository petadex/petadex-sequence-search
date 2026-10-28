#!/usr/bin/env python3
"""
Tier-A metadata enrichment for search hits, from the v260701 metadata store.

The store (built by scripts/db/build_metadata_store.py) holds one JSON "card"
per searchable sequence and one per cluster, in small gzipped NDJSON blocks with
a sorted index. A search needs only the blocks its hits fall in: group the hit
ids by block, fetch those blocks in parallel, and keep the lines whose id was
asked for. Every line starts with `{"id":"<key>",`, so unwanted lines are
skipped without being parsed.

    METADATA_STORE  s3://petadex/metadata/v260701/<tag>  (or a local directory)
                    Unset -> enrichment is "unavailable" and hits keep
                    `metadata: null`, exactly as before.

`enrich` never raises: a failed lookup must not fail the search, so it returns
status "failed" and the hits stay valid (the web app tells "no metadata" from
"lookup failed" by `enrichment_status`).

Status values: ok (every hit has a card) | partial (some ids not in the store)
| failed (the lookup raised) | unavailable (no store, or a store built for a
different database_release than the search).
"""

import bisect
import gzip
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3

ORIGIN = {"PL": "plasticome", "NR": "ncbi_nr", "LG": "logan"}
LEVELS = ("c90", "c60", "c30")
FETCH_THREADS = 32

_s3 = None
_stores = {}
_lock = threading.Lock()


def _client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", region_name="us-east-1")
    return _s3


class Store:
    """An opened store: index, background and manifest are read once per container."""

    def __init__(self, uri):
        self.uri = uri.rstrip("/")
        self.manifest = json.loads(self._read("manifest.json"))
        self.background = json.loads(self._read("background.json"))
        self.index = json.loads(self._read("index.json"))
        try:   # Plasticome members per 90 / 60 % cluster; optional (older stores lack it)
            self.pl_members = json.loads(self._read("pl_members.json"))
        except Exception:
            self.pl_members = {}

    @property
    def tag(self):
        return self.manifest.get("tag")

    def _read(self, rel):
        if self.uri.startswith("s3://"):
            bucket, _, prefix = self.uri[5:].partition("/")
            return _client().get_object(Bucket=bucket, Key=f"{prefix}/{rel}")["Body"].read()
        with open(os.path.join(self.uri, rel), "rb") as f:
            return f.read()

    def block_of(self, ns, key):
        """Block holding `key` in namespace `ns`, or None if it sorts before the first block."""
        ix = self.index.get(ns)
        if not ix or not ix["keys"]:
            return None
        i = bisect.bisect_right(ix["keys"], key) - 1
        return ix["blocks"][i] if i >= 0 else None

    def _block_cards(self, ns, block, wanted):
        """Parse only the lines of one block whose id is in `wanted` (a set of id strings)."""
        text = gzip.decompress(self._read(f"blocks/{ns}/block={block}/data_0.json.gz")).decode()
        out = {}
        for line in text.splitlines():
            # `{"id":"` is 7 characters; ids contain no quotes or backslashes
            end = line.find('"', 7)
            if end > 7 and line[7:end] in wanted:
                out[line[7:end]] = json.loads(line)
        return out

    def lookup(self, requests):
        """requests: [(ns, key, id_string)] -> ({id_string: card}, blocks fetched)."""
        by_block = {}
        for ns, key, sid in requests:
            blk = self.block_of(ns, key)
            if blk is not None:
                by_block.setdefault((ns, blk), set()).add(sid)
        cards = {}
        with ThreadPoolExecutor(max_workers=FETCH_THREADS) as pool:
            for got in pool.map(lambda kv: self._block_cards(kv[0][0], kv[0][1], kv[1]), by_block.items()):
                cards.update(got)
        return cards, len(by_block)


def open_store(uri):
    with _lock:
        if uri not in _stores:
            _stores[uri] = Store(uri)
        return _stores[uri]


def parse_target(target_id):
    """`LG123` -> ("LG", 123); anything else (v1.1 pipe headers) -> None."""
    ns = target_id[:2]
    if ns in ORIGIN and target_id[2:].isdigit():
        return ns, int(target_id[2:])
    return None


def rep(domain_id):
    """A cluster's display id: the representative sequence of its domain_id (`PL228|C001|...` -> `PL228`)."""
    return domain_id.split("|", 1)[0] if domain_id else None


def summarize(results, cards, background):
    """Hit-set summary: counts by origin, per-sequence environments against a random
    corpus sequence, and the clusters the hits fall into. Pure (no I/O)."""
    by_origin = {}
    for r in results:
        o = ORIGIN.get(r["target_id"][:2], "other")
        by_origin[o] = by_origin.get(o, 0) + 1

    # Environments: each Logan hit weighs 1, split over the profiles of its profiled
    # BioSamples (the same unit as the store's background), so `x_expected` is the
    # share among these hits over the share for a random PETadex Logan sequence.
    weight, n_seq = {}, 0
    for r in results:
        hist = (cards.get(r["target_id"]) or {}).get("prof_hist") or {}
        tot = sum(hist.values())
        if not tot:
            continue
        n_seq += 1
        for p, k in hist.items():
            weight[p] = weight.get(p, 0.0) + k / tot
    bg_n, bg_w = background.get("n_sequences") or 0, background.get("weight") or {}
    envs = []
    for p, w in sorted(weight.items(), key=lambda kv: -kv[1]):
        share = w / n_seq
        bshare = bg_w.get(p, 0.0) / bg_n if bg_n else None
        envs.append({"profile": p, "sequences": round(w, 3), "share": round(share, 4),
                     "background_share": round(bshare, 6) if bshare is not None else None,
                     "x_expected": round(share / bshare, 2) if bshare else None})

    clusters = {}
    for lvl in LEVELS:
        agg = {}
        for r in results:
            cid = (cards.get(r["target_id"]) or {}).get(lvl)
            if not cid:
                continue
            a = agg.setdefault(cid, {"id": rep(cid), "domain_id": cid, "hits": 0, "by_origin": {}, "best_pident": 0.0})
            a["hits"] += 1
            o = ORIGIN.get(r["target_id"][:2], "other")
            a["by_origin"][o] = a["by_origin"].get(o, 0) + 1
            a["best_pident"] = max(a["best_pident"], r["percent_identity"])
        clusters[lvl] = {"n": len(agg), "top": sorted(agg.values(), key=lambda a: (-a["hits"], -a["best_pident"]))[:50]}

    return {"n_hits": len(results), "by_origin": by_origin,
            "environments": {"unit": "logan sequence", "n_sequences": n_seq,
                             "background_sequences": bg_n, "profiles": envs},
            "clusters": clusters}


def enrich(results, database_release=None, store_uri=None):
    """Attach a card to every hit (`metadata`) and build the hit-set summary.

    Returns {status, store, cards, clusters, summary, timing}; never raises."""
    t0 = time.monotonic()
    uri = store_uri if store_uri is not None else os.environ.get("METADATA_STORE")
    out = {"status": "unavailable", "store": None, "cards": {}, "clusters": None, "summary": None,
           "timing": {}}
    if not uri:
        return out
    try:
        store = open_store(uri)
        out["store"] = store.tag
        if database_release and store.manifest.get("database_release") != database_release:
            return out   # e.g. a search pinned to v1.1: its ids are not in a v260701 store
        reqs = [(p[0], p[1], r["target_id"]) for r in results if (p := parse_target(r["target_id"]))]
        cards, n_blocks = store.lookup(reqs)
        t1 = time.monotonic()
        creq = sorted({(lvl, c[lvl], c[lvl]) for c in cards.values() for lvl in LEVELS if c.get(lvl)})
        ccards, n_cblocks = store.lookup(creq)
        clusters = {lvl: {} for lvl in LEVELS}
        for lvl, cid, _ in creq:
            if cid in ccards:
                card = dict(ccards[cid])
                if lvl in store.pl_members:
                    card["pl_members"] = store.pl_members[lvl].get(cid, [])
                clusters[lvl][cid] = card
        out.update(cards={sid: c for sid, c in cards.items()}, clusters=clusters,
                   summary=summarize(results, cards, store.background),
                   status="ok" if len(cards) == len(results) else "partial",
                   timing={"cards_ms": round((t1 - t0) * 1000, 1),
                           "clusters_ms": round((time.monotonic() - t1) * 1000, 1),
                           "blocks": n_blocks, "cluster_blocks": n_cblocks})
    except Exception as e:   # enrichment must never fail the search
        print(f"WARN metadata enrichment failed: {type(e).__name__}: {e}")
        out.update(status="failed", cards={}, clusters=None, summary=None)
    out["timing"]["total_ms"] = round((time.monotonic() - t0) * 1000, 1)
    return out


# ---- "Found in Samples": the BioSamples one Logan sequence was assembled in ----
# Recorded-text keywords: a literal match on a BioSample's own record and study title, nothing inferred.
KEYWORDS = {
    "saline": re.compile(r"hypersaline|saltern|salt (lake|marsh|pan)|brine|halophil|soda lake|saline", re.I),
    "hot": re.compile(r"hot spring|hydrothermal|geotherm|thermophil|compost|hot water|geyser|solfatar", re.I),
    "waste": re.compile(r"landfill|\bdump\b|waste ?water|sewage|activated sludge|compost|garbage|refuse|wwtp", re.I),
    "plastic": re.compile(r"plastic|polyethylene|polyester|polyurethane|polystyrene|polypropylene"
                          r"|\bpet (film|bottle|flake|powder|plastic)|plastisphere|\bpbat\b", re.I),
}
KEYWORD_FIELDS = ["isolation_source", "env_broad_scale", "env_medium", "host"]
BATCH_MAX_IDS = 500
BATCH_DEFAULT_LIMIT, BATCH_MAX_LIMIT = 50, 200
SAMPLE_FIELDS = ["bioproject", "profile", "profile_evidence", "isolation_source", "env_broad_scale", "env_medium",
                 "host", "country", "latitude", "longitude", "temperature_c", "ph"]


def _num(v, cast):
    try:
        return cast(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def samples(petadex_id, store_uri=None):
    """One sequence's samples from the samples store (SAMPLES_STORE, built by
    scripts/db/build_samples_store.py).

    Returns {id, status, n_biosamples, n_bioprojects, samples: [...]}: the first
    `sample_cap` (200) BioSamples by run count, each with its study title, its
    record and the sample's most abundant species by read classification.
    Status: ok | not_found (a Logan id with no linked BioSample) | not_applicable
    (not a Logan id) | unavailable (no store). Raises ValueError on a malformed id."""
    out = {"id": petadex_id, "status": "unavailable", "n_biosamples": 0, "n_bioprojects": 0, "samples": []}
    p = parse_target(petadex_id or "")
    if p is None:
        raise ValueError(f"not a petadex_id: {petadex_id!r}")
    if p[0] != "LG":
        out["status"] = "not_applicable"
        return out
    uri = store_uri if store_uri is not None else os.environ.get("SAMPLES_STORE")
    if not uri:
        return out
    store = open_store(uri)
    out["store"] = store.tag
    lgs, _ = store.lookup([("LGS", p[1], petadex_id)])
    card = lgs.get(petadex_id)
    if not card:
        out["status"] = "not_found"
        return out
    rows = card.get("samples") or []
    bs, _ = store.lookup([("BS", r[0], r[0]) for r in rows])
    bps = sorted({r[1] for r in rows if r[1]})
    bp, _ = store.lookup([("BP", x, x) for x in bps])
    out.update(status="ok", n_biosamples=card.get("n_biosamples", len(rows)),
               n_bioprojects=card.get("n_bioprojects", len(bps)), sample_cap=store.manifest.get("sample_cap"))
    for biosample, bioproject, n_runs, ka_max in rows:
        rec = bs.get(biosample) or {}
        out["samples"].append({
            "biosample": biosample, "bioproject": bioproject,
            "bioproject_title": (bp.get(bioproject) or {}).get("title"),
            "n_runs": _num(n_runs, int), "ka_max": _num(ka_max, float),
            **{f: rec.get(f) for f in SAMPLE_FIELDS if f != "bioproject"},
            "top_species": [[n, _num(s, float)] for n, s in (rec.get("top_species") or [])],
        })
    return out


def keywords(rec, title=None):
    """The KEYWORDS whose pattern occurs in a BioSample's recorded text or its study title."""
    text = " ".join(str(rec.get(f)) for f in KEYWORD_FIELDS if rec.get(f)) + (f" {title}" if title else "")
    return [k for k, rx in KEYWORDS.items() if rx.search(text)]


def samples_batch(ids, limit=None, store_uri=None):
    """Found in Samples for many sequences at once (a search's Logan hits), in three store reads.

    Returns {status, store, sample_cap, limit, results, biosamples, bioprojects}:
    `results[id]` = {status, n_biosamples, n_bioprojects, samples: [[biosample, n_runs, ka_max], ...]}
    holding each id's first `limit` BioSamples by run count (the true totals stay in n_*);
    `biosamples[bs]` = the record once per BioSample (SAMPLE_FIELDS, top_species, keywords);
    `bioprojects[bp]` = study title. Per-id status: ok | not_found | not_applicable.
    Overall status: ok | unavailable (no store). Raises ValueError on a malformed request."""
    if not isinstance(ids, list) or not ids:
        raise ValueError("ids must be a non-empty list")
    if len(ids) > BATCH_MAX_IDS:
        raise ValueError(f"at most {BATCH_MAX_IDS} ids per request")
    limit = BATCH_DEFAULT_LIMIT if limit is None else limit
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= BATCH_MAX_LIMIT:
        raise ValueError(f"limit must be an integer 1-{BATCH_MAX_LIMIT}")
    parsed = {}
    for i in ids:
        p = parse_target(i) if isinstance(i, str) else None
        if p is None:
            raise ValueError(f"not a petadex_id: {i!r}")
        parsed[i] = p
    out = {"status": "unavailable", "limit": limit, "results": {}, "biosamples": {}, "bioprojects": {}}
    uri = store_uri if store_uri is not None else os.environ.get("SAMPLES_STORE")
    if not uri:
        return out
    store = open_store(uri)
    out.update(status="ok", store=store.tag, sample_cap=store.manifest.get("sample_cap"))
    lg = {i: p for i, p in parsed.items() if p[0] == "LG"}
    lgs, _ = store.lookup([("LGS", p[1], i) for i, p in lg.items()])
    want_bs, want_bp = {}, set()   # biosample -> its study, from the sequence's own sample rows
    for i, p in parsed.items():
        if p[0] != "LG":
            out["results"][i] = {"status": "not_applicable"}
            continue
        card = lgs.get(i)
        if not card:
            out["results"][i] = {"status": "not_found", "n_biosamples": 0, "n_bioprojects": 0, "samples": []}
            continue
        rows = (card.get("samples") or [])[:limit]
        want_bs.update((r[0], r[1]) for r in rows)
        want_bp.update(r[1] for r in rows if r[1])
        out["results"][i] = {"status": "ok", "n_biosamples": card.get("n_biosamples", len(rows)),
                             "n_bioprojects": card.get("n_bioprojects"),
                             "samples": [[b, _num(n, int), _num(k, float)] for b, _, n, k in rows]}
    bs, _ = store.lookup([("BS", b, b) for b in sorted(want_bs)])
    bp, _ = store.lookup([("BP", x, x) for x in sorted(want_bp)])
    out["bioprojects"] = {x: (bp.get(x) or {}).get("title") for x in sorted(want_bp)}
    for b in sorted(want_bs):
        rec = bs.get(b) or {}
        out["biosamples"][b] = {**{f: rec.get(f) for f in SAMPLE_FIELDS}, "bioproject": rec.get("bioproject") or want_bs[b],
                                "top_species": [[n, _num(s, float)] for n, s in (rec.get("top_species") or [])],
                                "keywords": keywords(rec, out["bioprojects"].get(want_bs[b]))}
    return out
