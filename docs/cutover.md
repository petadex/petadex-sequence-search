# Search cutovers

Two database cutovers of the sharded DIAMOND search, newest first. A cutover is
a write to `s3://petadex/diamond/LATEST`: the orchestrator reads it once per job
and pins every worker, so no code deploy is needed and rollback is the same
one-line write.

## v260701: 2026-10-27 (DONE)

`LATEST`: `catalytic_orfs_v1.1_20260610_173654` → `petadex_seq_v260701_20261026_202443`.

**What changed for users.** Every unpinned search now runs against the v260701
release: 298,687,346 full-length catalytic proteins (PAZy 486, NCBI nr 6,764,617,
Logan 291,922,243), 88,181,580,142 residues, 32 zstd-19 `.fa.zst` shards. Hit IDs
are `petadex_id`s (`PL…` / `NR…` / `LG…`) instead of v1.1's `orf||run|…`; the
petadex.io backend and Gatsby site parse both, per hit. `metadata` is `null`
until tier-A enrichment exists (v260701 is not in RDS yet). The engine and
settings are unchanged: DIAMOND 2.2.2, `default` sensitivity, `-b1` with the
cross-block merge, `--dbsize` = `total_letters`, top 500.

**Order of events.**
1. Built and uploaded with `scripts/db/run_v260701_rebuild.sh` (verify corpus →
   `build_diamond_shards.py --format zstd --shard-count 32 --no-bump-latest` →
   manifest checks → `scripts/deploy/validate_pinned_version.py`), 2026-10-26.
2. Validated pinned: the site examples with a per-hit cross-version leak check,
   a 10-family recall check, a failure-path regression (a pinned search against a
   `diamond/smoketest-failpath-*` manifest with missing shards: `.error` written
   in 5 s, backend reported `failed` in 3 s).
3. Fixed before the flip: the worker's `/tmp` shard cache was keyed by file name,
   so warm Lambdas served the wrong version's shard when two versions were live
   (`a6f6633`, `tests/test_shard_cache.py`).
4. Site examples pinned to v260701 via `data/examples-version` (`6dd5aa8`), then
   **the user wrote `LATEST`** (Claude Code's safety check blocks that write for
   the assistant). Done ahead of the planned `web/` pages, at the user's request.
5. Verified: the orchestrator `version` action reports v260701; an unpinned
   SRR10663367 search returned 500/500 `petadex_id`s, top hit `LG113721682`
   at 100 % (v1.1 had no 100 % hit).
6. `data/examples-version` removed (`5728a45`); the deploy regenerated the
   examples against `LATEST` (all three on v260701).

**Rollback.**
```
printf '%s' catalytic_orfs_v1.1_20260610_173654 > /tmp/LATEST && \
AWS_PROFILE=default aws s3api put-object --bucket petadex --key diamond/LATEST \
  --body /tmp/LATEST --content-type text/plain --server-side-encryption AES256
```
The v1.1 shards are still at `s3://petadex/diamond/catalytic_orfs_v1.1_20260610_173654/`;
don't reap them while a rollback might be needed. Cached results are keyed by
`database_version`, so neither direction serves the other version's cache. After
a rollback the example pages regenerate on the next deploy (or pin them with
`data/examples-version`).

**Left open at the flip.** PL559 (AII11221.1) is absent from v260701 because of
the 2026-10-18 core-selection rule; if that rule changes, rebuild with
`run_v260701_rebuild.sh` and flip again. The `web/` search pages and tier-A
metadata enrichment come later. Details: the Obsidian vault notes
*v260701 Search Corpus Build* and *Search Lambda - TODO*.

---

## v1.1 zstd cutover: 2026-06-26 (DONE, historical runbook)

The runbook below was written before the zstd cutover and is kept as the record
of how it was planned. It happened 2026-06-26 on the DIAMOND **2.2.2** release
(see the vault note *09 - v2.2.2 Release Cutover*); `LATEST` then pointed at
`catalytic_orfs_v1.1_20260610_173654` until the v260701 flip above. The
"UNCOMMITTED" and "waiting on a tagged release" statements below describe that
time, not the current state.

### Lambda zstd Cutover: release-day runbook (as written before 2026-06-26)

**Status:** waiting on a **tagged DIAMOND release** carrying the compressed-FASTA
cross-block-merge fix. Everything below is validated and staged; the cutover is
gated only on a pinnable release tag.

**Why we're cutting over.** Stage 3 (doc *08 Compressed-FASTA Merge Dev Build
Validation*) measured 32-shard **zstd-on-dev** vs the 20-shard `.dmnd` baseline on
real Lambda, 5 cold runs each:

| metric | zstd (32, `-b1`) | `.dmnd` (20, `-b1`) |
|---|---|---|
| e2e | **48.2 s** | 127.1 s |
| download | **7.9 s** | 78.8 s |
| search | **31.4 s** | 40.3 s |
| worker MaxMem | **2.84 GB** | 7.99 GB |
| cost | **11,075 GB-s** | 22,679 GB-s |

Correct (all 10 runs: 50 hits, identical `hitset_sig` across both arms; across-shard
merge cross-validated). Verdict **(A) cutover-ready**.

### Winning config to pin

- **DB:** 32-shard `.fa.zst`, zstd level 19, round-robin partition.
- **Binary:** DIAMOND with the cross-block-merge fix (validated at `dev@4b2ae056`),
  built `WITH_ZSTD=ON`. **At cutover, pin the TAGGED RELEASE, not the dev branch.**
- **Worker `-b`:** `-b1` (lowest RSS; the merge fix lifts the single-block rule).
- **Sensitivity:** `default` (matches today's prod).
- **`--dbsize`:** whole-corpus letters from the manifest (`total_letters`).
- **Lambda:** 10240 MB / 600 s / arm64 (unchanged from today).

### Pre-staged changes (already in the working tree, UNCOMMITTED)

1. **`Dockerfile`**: added `ARG DIAMOND_BUILD_TOOLCHAIN=base` (default = today's
   build byte-for-byte). The DIAMOND build is now an `if`: `base` is unchanged;
   `gcc10` adds the proven toolchain fix (GCC 10.5 + `-include memory_resource` +
   `-static-libstdc++ -static-libgcc`) for when the release source needs C++17
   `std::pmr`. Already builds `WITH_ZSTD=ON` from a tagged tarball; cutover is a
   `DIAMOND_VERSION` bump (+ maybe the toolchain toggle).
2. **`worker.py`**: added `DIAMOND_FASTA_CROSSBLOCK_MERGE`: truthy → worker runs
   `-b1` for any shard size (the principled prod replacement for the dev-only
   `DIAMOND_FASTA_BLOCK_OVERRIDE` benchmark hack). **Default unset → prod behavior
   unchanged** (`-b4` single-block sizing, safe for the stock 2.2.1 binary).

### Cutover steps (when the release tag is published)

1. **Build-test the release binary** on the arm64 box:
   ```
   docker build --build-arg DIAMOND_VERSION=<tag> -f Dockerfile -t diamond-zstd-test .
   ```
   - Compiles clean → toolchain `base` is fine.
   - Fails with `'std::pmr' has not been declared` → add
     `--build-arg DIAMOND_BUILD_TOOLCHAIN=gcc10` (proven fix). Confirm the built
     binary's `ldd` shows **no dynamic libstdc++** before trusting it.
   - Either way confirm `diamond version` and that it opens a `.fa.zst`.
2. **Re-validate on the devtest stack** (no prod risk): rebuild the devtest worker +
   aggregator from the release image (`scripts/bench/stage2_devtest_runbook.sh`), set the
   worker env `DIAMOND_FASTA_CROSSBLOCK_MERGE=1` (instead of the override), and re-run
   `ARM=zstd fanout`; expect **50 hits, `set_match=True`, MaxMem ~2.8 GB**. This
   re-confirms the *release* binary (not just `dev@4b2ae056`) before touching prod.
3. **Build the production 32-shard zstd DB** of the current corpus:
   `scripts/db/build_diamond_shards.py --format zstd --shard-count 32 --no-bump-latest`.
   Keep `--no-bump-latest` until step 6.
4. **Ship the prod image** via the normal pipeline (`deploy.yml`) with the bumped
   `DIAMOND_VERSION` (+ `DIAMOND_BUILD_TOOLCHAIN=gcc10` build-arg if step 1 needed it).
5. **Set prod worker env** `DIAMOND_FASTA_CROSSBLOCK_MERGE=1` (so it runs `-b1`).
   ⚠️ Do NOT set it while the stock 2.2.1 binary is still deployed: it over-reports
   at multi-block `-b1`. Set it only on the merge-fixed image.
6. **Flip `LATEST`** to the new 32-shard zstd version prefix. Orchestrator reads it
   once per job and pins every worker.

### Verifications / open items at cutover

- [ ] **Reserved concurrency** ≥ 32 + margin for the 32-wide fan-out (provision
  default is 160; today's prod is 20-shard). Couldn't read it from the dev box
  (`lambda:GetFunctionConcurrency` denied); check with an admin principal. The
  Stage-3 fan-out saw **no throttling**, but that was the devtest worker on the
  unreserved pool, not prod's reserved setting.
- [ ] **Result-identity** on the production path post-flip: run the example set
  (IsPETase / FAST-PETase / SRR10663367) and diff vs the pre-flip `.dmnd` results.
  (Stage 3 already showed IsPETase identical across arms.)
- [ ] **Corpus freshness**: Stage 3 used the 2026-06-10 zstd build; step 3 rebuilds
  from the current corpus, so confirm `total_letters`/shard count in the new manifest.
- [ ] **Aggregator/orchestrator** need no change (format-agnostic), but redeploy on
  the same release image for consistency.

### Rollback

`LATEST` still has the previous `.dmnd` version prefix: flip it back and unset
`DIAMOND_FASTA_CROSSBLOCK_MERGE`. The orchestrator picks it up on the next job; no
code redeploy required to revert the DB. (Reverting the binary = redeploy the prior
image tag.)

### References

- `obsidian-vault/.../08 Compressed-FASTA Merge Dev Build Validation.md`: full
  validation (Gates 1–2, Stage 3 A/B, verdict).
- `scripts/bench/stage2_devtest_runbook.sh` / `stage3_fanout.py` / `stage3_ab.py`: the
  devtest harness used to validate; reuse it to re-validate the release binary.
- **Open ask to Benjamin:** confirm the tagged release / stable commit that carries
  the compressed-FASTA cross-block-merge fix, to pin in step 1.

## MMseqs2 → DIAMOND: 2026-06-03 (DONE)


The web app's search invocation was repointed from `petadex-mmseqs2-search` to **`petadex-diamond-orchestrator`** (its role granted `lambda:InvokeFunction` on the orchestrator). Because the contract is preserved, no result-parsing changes were needed; latency rose from ~58 s to ~5 min, so the caller applies a poll timeout / failure state. The legacy function, its code and the MMseqs2 binary have since been removed (below).


## Legacy MMseqs2 path

The original implementation was a single ARM64 container Lambda (`petadex-mmseqs2-search`) running `mmseqs easy-search` against the ~1M-sequence `petadex-nr` subset:

1. Downloads the MMseqs2 sequence index from S3 (`s3://petadex/mmseqs2/`), cached in `/tmp` across warm invocations.
2. Runs `mmseqs easy-search` against the index.
3. Enriches hits from RDS and uploads the result JSON to `results/{sessionId}/{job_id}.json` in the same schema.

**Retired.** The cutover to DIAMOND completed 2026-06-03 and the `mmseqs` binary left the image on 2026-06-26. The index builder (`scripts/db/update_sequence_index.py`) was removed 2026-10-26; the `petadex-mmseqs2-search` Lambda was deleted and its handler (`src/lambda_function.py`) removed 2026-10-27 (all in git history; function config backups in `/franklin/rnalab1/dzhu/petadex-v260701-search/lambda_backup_20261026/`). What remains is the `s3://petadex/mmseqs2/` prefix. Old results carry `engine: mmseqs2` / `search_version: legacy-mmseqs2`, so they stay self-identifying if encountered.
