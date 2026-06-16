# Lambda zstd Cutover — release-day runbook

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

## Winning config to pin

- **DB:** 32-shard `.fa.zst`, zstd level 19, round-robin partition.
- **Binary:** DIAMOND with the cross-block-merge fix (validated at `dev@4b2ae056`),
  built `WITH_ZSTD=ON`. **At cutover, pin the TAGGED RELEASE, not the dev branch.**
- **Worker `-b`:** `-b1` (lowest RSS; the merge fix lifts the single-block rule).
- **Sensitivity:** `default` (matches today's prod).
- **`--dbsize`:** whole-corpus letters from the manifest (`total_letters`).
- **Lambda:** 10240 MB / 600 s / arm64 (unchanged from today).

## Pre-staged changes (already in the working tree, UNCOMMITTED)

1. **`Dockerfile`** — added `ARG DIAMOND_BUILD_TOOLCHAIN=base` (default = today's
   build byte-for-byte). The DIAMOND build is now an `if`: `base` is unchanged;
   `gcc10` adds the proven toolchain fix (GCC 10.5 + `-include memory_resource` +
   `-static-libstdc++ -static-libgcc`) for when the release source needs C++17
   `std::pmr`. Already builds `WITH_ZSTD=ON` from a tagged tarball — cutover is a
   `DIAMOND_VERSION` bump (+ maybe the toolchain toggle).
2. **`worker.py`** — added `DIAMOND_FASTA_CROSSBLOCK_MERGE`: truthy → worker runs
   `-b1` for any shard size (the principled prod replacement for the dev-only
   `DIAMOND_FASTA_BLOCK_OVERRIDE` benchmark hack). **Default unset → prod behavior
   unchanged** (`-b4` single-block sizing, safe for the stock 2.2.1 binary).

## Cutover steps (when the release tag is published)

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
   aggregator from the release image (`scripts/stage2_devtest_runbook.sh`), set the
   worker env `DIAMOND_FASTA_CROSSBLOCK_MERGE=1` (instead of the override), and re-run
   `ARM=zstd fanout` — expect **50 hits, `set_match=True`, MaxMem ~2.8 GB**. This
   re-confirms the *release* binary (not just `dev@4b2ae056`) before touching prod.
3. **Build the production 32-shard zstd DB** of the current corpus:
   `scripts/build_diamond_shards.py --format zstd --shard-count 32 --no-bump-latest`.
   Keep `--no-bump-latest` until step 6.
4. **Ship the prod image** via the normal pipeline (`deploy.yml`) with the bumped
   `DIAMOND_VERSION` (+ `DIAMOND_BUILD_TOOLCHAIN=gcc10` build-arg if step 1 needed it).
5. **Set prod worker env** `DIAMOND_FASTA_CROSSBLOCK_MERGE=1` (so it runs `-b1`).
   ⚠️ Do NOT set it while the stock 2.2.1 binary is still deployed — it over-reports
   at multi-block `-b1`. Set it only on the merge-fixed image.
6. **Flip `LATEST`** to the new 32-shard zstd version prefix. Orchestrator reads it
   once per job and pins every worker.

## Verifications / open items at cutover

- [ ] **Reserved concurrency** ≥ 32 + margin for the 32-wide fan-out (provision
  default is 160; today's prod is 20-shard). Couldn't read it from the dev box
  (`lambda:GetFunctionConcurrency` denied) — check with an admin principal. The
  Stage-3 fan-out saw **no throttling**, but that was the devtest worker on the
  unreserved pool, not prod's reserved setting.
- [ ] **Result-identity** on the production path post-flip: run the example set
  (IsPETase / FAST-PETase / SRR10663367) and diff vs the pre-flip `.dmnd` results.
  (Stage 3 already showed IsPETase identical across arms.)
- [ ] **Corpus freshness** — Stage 3 used the 2026-06-10 zstd build; step 3 rebuilds
  from the current corpus, so confirm `total_letters`/shard count in the new manifest.
- [ ] **Aggregator/orchestrator** need no change (format-agnostic), but redeploy on
  the same release image for consistency.

## Rollback

`LATEST` still has the previous `.dmnd` version prefix — flip it back and unset
`DIAMOND_FASTA_CROSSBLOCK_MERGE`. The orchestrator picks it up on the next job; no
code redeploy required to revert the DB. (Reverting the binary = redeploy the prior
image tag.)

## References

- `obsidian-vault/.../08 Compressed-FASTA Merge Dev Build Validation.md` — full
  validation (Gates 1–2, Stage 3 A/B, verdict).
- `scripts/stage2_devtest_runbook.sh` / `stage3_fanout.py` / `stage3_ab.py` — the
  devtest harness used to validate; reuse it to re-validate the release binary.
- **Open ask to Benjamin:** confirm the tagged release / stable commit that carries
  the compressed-FASTA cross-block-merge fix, to pin in step 1.
