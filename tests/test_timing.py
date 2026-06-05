"""
Local tests for the runtime-timing telemetry (worker sidecars + aggregator
rollup). Stubs S3 — no network. Run with: python -m pytest tests/test_timing.py
"""

import json
import sys

sys.path.insert(0, ".")

import aggregator
import worker


class FakeS3:
    """Minimal in-memory S3 stand-in: put/get/list over a dict of key -> bytes."""

    def __init__(self):
        self.store = {}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.store[Key] = Body if isinstance(Body, bytes) else Body.encode()
        return {}

    def get_object(self, Bucket, Key):
        return {"Body": _Body(self.store[Key])}

    def get_paginator(self, _name):
        store = self.store

        class _Paginator:
            def paginate(self, Bucket, Prefix):
                contents = [{"Key": k} for k in store if k.startswith(Prefix)]
                yield {"Contents": contents}

        return _Paginator()


class _Body:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


# --- worker: write_shard_timing -------------------------------------------------

def test_write_shard_timing_key_and_payload():
    fake = FakeS3()
    worker.s3 = fake
    timings = {"download_ms": 20.0, "search_ms": 170.0, "total_ms": 191.0,
               "shard_size_bytes": 7_065_182_208, "shard_seq_count": 15_357_787,
               "num_hits": 12}
    worker.write_shard_timing("sess", "job", 3, timings, "success")

    key = "results/sess/job/parts/shard_3.meta.json"
    assert key in fake.store
    doc = json.loads(fake.store[key])
    assert doc["shard_index"] == 3
    assert doc["status"] == "success"
    assert doc["error"] is None
    assert doc["search_ms"] == 170.0
    assert doc["shard_seq_count"] == 15_357_787
    assert doc["num_hits"] == 12
    assert "timestamp" in doc


def test_write_shard_timing_never_raises():
    """A telemetry write failure must degrade silently, not crash the worker."""
    class Boom:
        def put_object(self, **kwargs):
            raise RuntimeError("s3 down")
    worker.s3 = Boom()
    # Must not raise.
    worker.write_shard_timing("sess", "job", 0, {"total_ms": 1.0}, "failed", "x")


def test_handler_emits_sidecar_on_failure(monkeypatch=None):
    """fail-fast: the search raises, but the `finally` still leaves a breadcrumb
    with status=failed and the error string."""
    fake = FakeS3()
    worker.s3 = fake
    worker.download_shard = lambda key: "/tmp/shard_0"
    def _boom(*a, **k):
        raise RuntimeError("diamond exploded")
    worker.run_shard_search = _boom

    event = {"sessionId": "s", "jobId": "j", "shardIndex": 0,
             "shardKey": "diamond/v1/shard_0.dmnd", "queryFasta": ">q\nMKLL",
             "maxResults": 50, "shardSeqs": 100}
    try:
        worker.handler(event, None)
        assert False, "handler should re-raise (fail-fast)"
    except RuntimeError:
        pass

    doc = json.loads(fake.store["results/s/j/parts/shard_0.meta.json"])
    assert doc["status"] == "failed"
    assert "diamond exploded" in doc["error"]
    assert doc["total_ms"] is not None


# --- aggregator: write_job_timing rollup ---------------------------------------

def _seed_sidecar(fake, job, idx, status="success", total_ms=100.0):
    key = f"results/sess/{job}/parts/shard_{idx}.meta.json"
    fake.store[key] = json.dumps({
        "shard_index": idx, "status": status, "total_ms": total_ms,
        "download_ms": 20.0, "search_ms": total_ms - 20.0,
    }).encode()


def test_write_job_timing_rollup_and_derived_fields():
    fake = FakeS3()
    aggregator.s3 = fake
    _seed_sidecar(fake, "job", 0, total_ms=100.0)
    _seed_sidecar(fake, "job", 1, total_ms=250.0)
    _seed_sidecar(fake, "job", 2, total_ms=180.0)

    submitted = "2026-06-02T00:00:00+00:00"
    aggregator.write_job_timing("sess", "job", "v1", 3, submitted, "success")

    doc = json.loads(fake.store["results/sess/job/timing.json"])
    assert doc["status"] == "success"
    assert doc["shards_expected"] == 3
    assert doc["shards_completed"] == 3
    assert doc["slowest_shard_ms"] == 250.0
    assert doc["fastest_slowest_spread_ms"] == 150.0  # 250 - 100
    assert doc["total_wall_ms"] is not None and doc["total_wall_ms"] > 0
    assert [s["shard_index"] for s in doc["shards"]] == [0, 1, 2]


def test_write_job_timing_marks_missing_shards():
    """A worker that died before writing a sidecar must show as `missing`, not
    crash the rollup (failure path)."""
    fake = FakeS3()
    aggregator.s3 = fake
    _seed_sidecar(fake, "job", 0, status="failed", total_ms=50.0)
    # shards 1 and 2 never wrote a sidecar.
    aggregator.write_job_timing("sess", "job", "v1", 3, None, "failed")

    doc = json.loads(fake.store["results/sess/job/timing.json"])
    assert doc["status"] == "failed"
    assert doc["shards_completed"] == 0          # the one present shard failed
    assert doc["total_wall_ms"] is None          # no submittedAt supplied
    by_idx = {s["shard_index"]: s for s in doc["shards"]}
    assert by_idx[1]["status"] == "missing"
    assert by_idx[2]["status"] == "missing"
    assert by_idx[0]["status"] == "failed"


def test_aggregator_stamps_database_identity():
    """Success path: the result JSON self-identifies as the DIAMOND/Logan corpus
    (engine/database/version/seq-count), so it can't be confused with nr."""
    fake = FakeS3()
    aggregator.s3 = fake
    aggregator.fetch_metadata = lambda ids: {}  # no RDS
    # One shard part + its sidecar so the part-count cross-check passes.
    fake.store["results/sess/job/parts/shard_0.tsv"] = (
        b"target1\t1\t10\t1\t10\t10\t55.5\t1e-50\t200\n")
    _seed_sidecar(fake, "job", 0)

    event = {
        "sessionId": "sess", "jobId": "job", "version": "catalytic_orfs_v1.1_x",
        "querySequence": "MKLLIVL", "queryHeader": ">q", "maxResults": 50,
        "corpus": "s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa",
        "dbSequenceCount": 307155746,
        "databaseRelease": "v1.1", "searchVersion": "1.0.0",
        "shards": [{"shardIndex": 0}],
    }
    aggregator.handler(event, None)

    doc = json.loads(fake.store["results/sess/job.json"])
    assert doc["engine"] == "diamond"
    assert doc["database"] == "s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa"
    assert doc["database_version"] == "catalytic_orfs_v1.1_x"
    # Two version axes both labelled: semantic release + search-pipeline semver.
    assert doc["database_release"] == "v1.1"
    assert doc["search_version"] == "1.0.0"
    assert doc["db_sequence_count"] == 307155746
    # The contract fields are still present and unchanged.
    assert doc["num_results"] == 1 and "results" in doc


def test_aggregator_records_phase_timing():
    """Success path: timing.json carries an `aggregator` block with the
    post-Map phase durations, so we can see how much of the tail is metadata
    enrichment vs. part-merge vs. ranking vs. the write."""
    fake = FakeS3()
    aggregator.s3 = fake
    aggregator.fetch_metadata = lambda ids: {}  # no RDS
    fake.store["results/sess/job/parts/shard_0.tsv"] = (
        b"target1\t1\t10\t1\t10\t10\t55.5\t1e-50\t200\n")
    _seed_sidecar(fake, "job", 0)

    event = {
        "sessionId": "sess", "jobId": "job", "version": "v1",
        "querySequence": "MKLLIVL", "queryHeader": ">q", "maxResults": 50,
        "shards": [{"shardIndex": 0}],
    }
    aggregator.handler(event, None)

    doc = json.loads(fake.store["results/sess/job/timing.json"])
    agg = doc["aggregator"]
    assert agg is not None
    for k in ("read_parts_ms", "sort_ms", "metadata_ms",
              "write_result_ms", "total_ms"):
        assert k in agg and agg[k] >= 0.0


def test_aggregator_timing_only_mode():
    """timing-only invocation writes a failed rollup and no result JSON."""
    fake = FakeS3()
    aggregator.s3 = fake
    _seed_sidecar(fake, "job", 0)
    event = {"mode": "timing-only", "sessionId": "sess", "jobId": "job",
             "version": "v1", "submittedAt": "2026-06-02T00:00:00+00:00",
             "shards": [{"shardIndex": 0}, {"shardIndex": 1}]}
    out = aggregator.handler(event, None)

    assert out["status"] == "failed"
    assert "results/sess/job/timing.json" in fake.store
    # No user-facing result/index written on the failure path.
    assert "results/sess/job.json" not in fake.store
    assert "results/sess.index" not in fake.store


_ORCH_TIMING = {
    "parse_ms": 1.2, "resolve_version_ms": 30.5,
    "load_shards_ms": 12.0, "total_pre_start_ms": 43.7,
}


def test_aggregator_records_orchestrator_timing_success():
    """Success path: the orchestrator's pre-start phases threaded through the
    execution input land in timing.json under `orchestrator`, symmetric to the
    `aggregator` block — so the wall's orchestrator contribution is visible."""
    fake = FakeS3()
    aggregator.s3 = fake
    aggregator.fetch_metadata = lambda ids: {}  # no RDS
    fake.store["results/sess/job/parts/shard_0.tsv"] = (
        b"target1\t1\t10\t1\t10\t10\t55.5\t1e-50\t200\n")
    _seed_sidecar(fake, "job", 0)

    event = {
        "sessionId": "sess", "jobId": "job", "version": "v1",
        "querySequence": "MKLLIVL", "queryHeader": ">q", "maxResults": 50,
        "shards": [{"shardIndex": 0}],
        "orchestratorTiming": _ORCH_TIMING,
    }
    aggregator.handler(event, None)

    doc = json.loads(fake.store["results/sess/job/timing.json"])
    assert doc["orchestrator"] == _ORCH_TIMING


def test_aggregator_records_orchestrator_timing_failure():
    """Fail-fast path: orchestratorTiming survives the timing-only invocation
    (the ASL Parameters forward it), so failed jobs still record it."""
    fake = FakeS3()
    aggregator.s3 = fake
    _seed_sidecar(fake, "job", 0)
    event = {"mode": "timing-only", "sessionId": "sess", "jobId": "job",
             "version": "v1", "submittedAt": "2026-06-02T00:00:00+00:00",
             "shards": [{"shardIndex": 0}],
             "orchestratorTiming": _ORCH_TIMING}
    aggregator.handler(event, None)

    doc = json.loads(fake.store["results/sess/job/timing.json"])
    assert doc["orchestrator"] == _ORCH_TIMING
