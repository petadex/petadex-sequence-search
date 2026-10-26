"""Shard cache must be keyed by the full S3 key, not the basename.

Two DB versions name their shards identically (`shard_07.fa.zst`); a basename
cache served one version's shard to a search pinned to the other (2026-10-26).

Standalone (no pytest): `python3 tests/test_shard_cache.py`. S3 is stubbed.
"""
import os
import sys
import tempfile
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import worker  # noqa: E402

FAILURES = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


OLD = "diamond/catalytic_orfs_v1.1_20260610_173654/shard_07.fa.zst"
NEW = "diamond/petadex_seq_v260701_20261026_202443/shard_07.fa.zst"

p_old, p_new = worker.shard_local_path(OLD), worker.shard_local_path(NEW)
check("same shard index, different versions -> different cache paths", p_old != p_new)
check("cache path keeps the shard_ prefix (eviction glob)", os.path.basename(p_new).startswith("shard_"))
check("cache path keeps the format suffix", worker.is_fasta_db(p_new))
check(".dmnd path still strips to a DIAMOND basename",
      worker.shard_db_path("diamond/v/shard_00.dmnd", worker.shard_local_path("diamond/v/shard_00.dmnd"))
      == "/tmp/shard_diamond_v_shard_00")

# A warm container that last served OLD must download NEW, not reuse OLD's file.
with tempfile.TemporaryDirectory() as tmp:
    real = worker.shard_local_path
    with mock.patch.object(worker, "shard_local_path",
                           lambda k: os.path.join(tmp, os.path.basename(real(k)))), \
         mock.patch.object(worker.glob, "glob",
                           lambda pat: [os.path.join(tmp, f) for f in os.listdir(tmp) if f.startswith("shard_")]), \
         mock.patch.object(worker.s3, "download_file",
                           side_effect=lambda b, k, path, **kw: open(path, "w").write(k)) as dl:
        worker.download_shard(OLD)
        got = worker.download_shard(NEW)
        check("pinned version is downloaded, not served from the other version's cache",
              dl.call_count == 2 and open(got).read() == NEW, f"calls={dl.call_count}")
        check("the other version's shard was evicted", os.listdir(tmp) == [os.path.basename(got)],
              str(os.listdir(tmp)))
        worker.download_shard(NEW)
        check("same key on a warm container is still a cache hit", dl.call_count == 2)

print(f"\n{'ALL PASS' if not FAILURES else f'{len(FAILURES)} FAILED'}")
sys.exit(1 if FAILURES else 0)
