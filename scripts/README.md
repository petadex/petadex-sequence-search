# Scripts

Building, validating and operating the sharded DIAMOND search database.

## Files

**Database (`db/`)**
- **[build_metadata_store.py](db/build_metadata_store.py)** - builds the v260701 metadata store (hit cards, cluster cards) the aggregator enriches from (runs on EC2)
- **[build_samples_store.py](db/build_samples_store.py)** - builds the Found-in-Samples store behind the `samples` action (runs on EC2)
- **[build_diamond_shards.py](db/build_diamond_shards.py)** - partitions a corpus FASTA into shards, builds each (`.fa.zst` or `.dmnd`), uploads them under `diamond/{version}/`, writes `manifest.json`, and moves `LATEST` last (unless `--no-bump-latest`)
- **[assemble_v260701_corpus.py](db/assemble_v260701_corpus.py)** - assembles the v260701 full-length and catalytic-core corpora (`>petadex_id` headers) from the dataset-generation outputs
- **[verify_v260701_corpus.sh](db/verify_v260701_corpus.sh)** - independent record-by-record check of those corpora against the source files
- **[run_v260701_rebuild.sh](db/run_v260701_rebuild.sh)** - the unattended v260701 run: verify → build + upload (`--no-bump-latest`) → manifest/`LATEST` checks → pinned validation
- **[striped_corpus_sample.py](db/striped_corpus_sample.py)** - pre-build sampler for sizing the shard count
- **[backfill_manifest_letters.py](db/backfill_manifest_letters.py)** - adds `total_letters` to an older manifest
- **[reap_old_diamond_versions.sh](db/reap_old_diamond_versions.sh)** - removes superseded versions under `diamond/`

**Deploy and validation (`deploy/`)**
- **[provision_diamond_infra.sh](deploy/provision_diamond_infra.sh)** - one-time creation of the orchestrator/worker/aggregator and state machine
- **[provision_align.sh](deploy/provision_align.sh)** - one-time creation of the align Lambda, its role, invoke grants and `ALIGN_FUNCTION` env
- **[set_worker_env.py](deploy/set_worker_env.py)** - worker environment changes
- **[smoke_test.sh](deploy/smoke_test.sh)** - one search end to end through the live orchestrator
- **[validate_export.py](deploy/validate_export.py)** - live export check against the deployed functions
- **[validate_pinned_version.py](deploy/validate_pinned_version.py)** - runs the site examples pinned to a version and against `LATEST`; fails on any hit from the wrong version

**Benchmarks (`bench/`)** - `bench_full_vs_cores.sh` (search DB choice), `bench_result_depth.py`, `export_e2e.py` (export vs search on real shards, no AWS), `sensitivity_recall.py` (default vs `--very-sensitive` recall by identity bin), and the Stage 2/3 dev-build runbooks.

**Operations (`ops/`)**
- **[setup_search_alarms.sh](ops/setup_search_alarms.sh)** - creates/updates the search CloudWatch alarms and their SNS → ntfy relay (idempotent; `--test` sends one push)

## Building a database version

Production searches zstd-compressed FASTA shards (`.fa.zst`) on DIAMOND 2.2.2 with the cross-block merge (`DIAMOND_FASTA_CROSSBLOCK_MERGE=1`, `-b1`). A zstd shard needs no `diamond makedb`, so a build runs on any Linux box with `zstd`, Python + `boto3`, enough scratch (about the corpus size), and S3 write on `s3://petadex/diamond/*`. The v260701 build ran on watson.

```bash
# Plan only:
./scripts/db/build_diamond_shards.py --dry-run --format zstd ...

# Build + upload WITHOUT publishing (LATEST untouched; pin it to test):
./scripts/db/build_diamond_shards.py --format zstd --shard-count 32 \
  --local-corpus /path/PETadex.seq.v260701.fa --key diamond/corpus/PETadex.seq.v260701.fa \
  --expected-count 298687346 --expected-letters 88181580142 \
  --release v260701 --version-prefix petadex_seq_v260701 \
  --scratch-dir /scratch --threads 64 --no-bump-latest

# Validate through the live orchestrator, pinned:
AWS_PROFILE=default ./scripts/deploy/validate_pinned_version.py <version> validation.tsv

# Publish (cutover) once validated:
aws s3api put-object --bucket petadex --key diamond/LATEST --body <(printf '%s' <version>)
```

`--expected-count`, `--expected-letters`, `--release` and `--version-prefix` default to v1.1's values. Pass them for any other corpus. Other flags: `--partition {round-robin,contiguous}` (default round-robin), `--shard-count` (default 32), `--threads`, `--version`, `--keep-fasta`, `--skip-upload`, `--max-records` (smoke test only).

Published layout:

```
s3://petadex/diamond/{version}/shard_00.fa.zst … shard_31.fa.zst
s3://petadex/diamond/{version}/manifest.json   # version, database_release, total_letters, per-shard seqs/letters/bytes
s3://petadex/diamond/LATEST                     # → {version}  (written LAST)
```

Workers resolve the version once per job (`LATEST`, or the request's `version`) and cache shards in `/tmp` under the full S3 key, so a new version needs no Lambda redeploy, and two versions never share a cached shard.

**Batch (`batch/`, local-only, gitignored)** - many-protein batch search via 32 direct worker invokes with a multi-query FASTA.

## Sharded database layout

```
s3://petadex/diamond/
├── LATEST                                    # text pointer → current version dir
└── {version}/                                # e.g. petadex_seq_v260701_20261026_202443
    ├── manifest.json                         # version, database_release, total_letters, per-shard key/sequences/letters/bytes
    ├── shard_00.fa.zst  (~779 MB)            # 32 shards, ~9.33M sequences / ~2.756G residues each
    ├── ...
    └── shard_31.fa.zst
```

The corpus FASTA is split round-robin, so shards are even to one sequence and a target lives in exactly one shard. Each shard is zstd level 19; DIAMOND (built with zstd support) reads it directly as its database. Earlier builds (`catalytic_orfs_v1.1_*`: 20 `.dmnd` shards until 2026-06-26, then 32 zstd shards) came from `s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa`.

**Atomic versioning** (prevents mid-rebuild races): write all shards, then `manifest.json`, **then** update `LATEST`. The orchestrator reads `LATEST` once per job and passes the resolved version into the execution, so all workers pin to one version even if `LATEST` flips mid-job. Old versions are removed with `reap_old_diamond_versions.sh` (LATEST-aware, dry-run by default), never by an age rule.

## Removed

The legacy MMseqs2 index builder (`db/update_sequence_index.py`, its `requirements.txt`, `S3_SETUP.md`, `deploy/setup_s3_access.sh` and the `update-database.yml.example` workflow) was removed on 2026-10-26. It rebuilt the ~1M-sequence `enzyme_fastaa` MMseqs2 index, which has not served the web app since the DIAMOND cutover (2026-06-03). It is in git history.

## Support

For issues or questions, see the main [README.md](../README.md) or open an issue.
