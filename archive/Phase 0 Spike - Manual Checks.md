# Phase 0 Spike — Manual Checks

Runbook for the pre-build measurement spike on the sharded DIAMOND search migration. Each check gates the next — run in order and stop if a result invalidates a downstream assumption. Fill in findings as you go.

> Note on the 3.2 GB figure: this was the size of the *current* MMseqs2 index, built from the ~1M `petadex-nr` set, not the full corpus. 3.2 GB for ~1M sequences is plausible; the extrapolation to the full Logan-scale corpus is what the spike must nail down. Check 0 querying the real count is the move that resolves this.

---

## Check 0 — Establish ground truth on corpus size

Cheapest check, reshapes everything downstream. Connect to production RDS and run the count the real extraction will use:

```sql
SELECT COUNT(*) FROM enzyme_fastaa
WHERE translated_sequence IS NOT NULL
  AND translated_sequence != '';
```

This number replaces every "217M" / "300M" figure. If it comes back ~1M, the "expand by 300x" framing refers to a corpus not yet in this table — meaning you must also confirm **where the full Logan-scale set actually lives**, because `update_sequence_index.py`'s extraction query points at `enzyme_fastaa`. If that only holds the nr subset, the build pipeline has a missing input. Resolve before anything else.

**Finding:**
- Corpus count: 307155746
- Full Logan set location (if not in `enzyme_fastaa`): s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa

Please note that enzyme_fastaa is not the same thing as s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa which contains all 307 million logan sequences.

---

## Check 1 — Confirm current index size (baseline ratio)

List the current index objects and sum sizes:

```bash
aws s3 ls s3://petadex/mmseqs2/{version}/ --recursive --summarize
```

Pair the total with the sequence count that index was built from → real bytes-per-sequence for MMseqs2. This is a sanity cross-check only; DIAMOND's ratio (Check 3) drives the design. But if MMseqs2's measured ratio wildly contradicts the 3.2 GB working figure, that flags a problem before any build.

**Finding:**
- Current index total size: 3.49 GiB 
	- .idx MMseqs2 precomputed k-mer search index = 3.13 GiB
	- Rest is sequence data, headers and the metadata component
- Sequences it was built from: 1,048,585 sequences
- Bytes/sequence (MMseqs2): 3KB/seq approximately

---

## Check 1b — Current Lambda runtime baseline

Establishes what DIAMOND has to beat *per shard* end-to-end. Invoke `petadex-mmseqs2-search` once to force a cold start, then several times warm. The Lambda already emits `TIMING database_download` (S3→`/tmp`) and `TIMING mmseqs_search` (load + search bundled) — `mmseqs easy-search` does not separate index load from search, so a true "load time" can't be isolated without a code change (split into `createdb` + `prefilter` + `align`, or warm the page cache with a dummy run).

```bash
# cold (or force cold by updating env/memory before invoking)
aws lambda invoke --function-name petadex-mmseqs2-search \
  --cli-binary-format raw-in-base64-out \
  --payload '{"action":"search","sessionId":"bench","sequence":"<seq>","max_results":50}' \
  --log-type Tail --query LogResult --output text out.json | base64 -d | grep -E "TIMING|REPORT"
```

Lambda config at time of measurement: arm64, **3072 MB memory**, 300s timeout, 4096 MB `/tmp`.

**Finding (measured 2026-06-01):**

Cold invocation (IsPETase, 290 aa):
- S3 → `/tmp` download: **47.95s** for 3.5 GiB (~73 MiB/s)
- `mmseqs easy-search` (load + search): **9.63s**
- Init duration: 400 ms
- End-to-end: **57.9s**
- Max memory used: **3054 / 3072 MB** (saturated)

Warm invocations (DB already on `/tmp`):

| Query | Length | `easy-search` wall | Total |
|---|---|---|---|
| IsPETase | 290 aa | 9.84s | 10.1s |
| FAST-PETase | 290 aa | 13.10s | 13.3s |
| SRR10663367 | 215 aa | 14.01s | 14.2s |
| IsPETase (repeat) | 290 aa | 15.92s | 16.1s |

Observations:
- Max memory is at the ceiling every invocation. Warm search time drifts upward across successive runs on identical input (9.84 → 15.92s) — consistent with the OS page cache for the 3.1 GiB `.idx` mmap being evicted under memory pressure between invocations, forcing re-reads from disk-backed `/tmp`.
- Bumping Lambda memory would likely both speed up and stabilize warm search times; worth re-measuring at 4–6 GB before treating the current 10–16s warm number as the baseline DIAMOND must beat.
- For the per-shard comparison: DIAMOND on one shard must beat ~10–16s warm search **and** the per-shard slice of cold download (currently 48s for 3.5 GiB → ~3.7s if amortized over 13 shards downloading in parallel, but Check 7 is the empirical test of that).

---

## Check 2 — Stand up the measurement box

Launch an **arm64** EC2 instance (`r6g.2xlarge` is a starting point, not a requirement — any arm64 box with enough EBS for a few sample `.dmnd` files and RAM to run DIAMOND comfortably). Install the arm64 DIAMOND2 binary — the **same version and architecture** that will go in the Lambda image, since DB format and search behavior must match.

```bash
diamond version
```

Optionally install MMseqs2 here too if you want Check 8 parity to run head-to-head locally rather than against the live Lambda.

**Finding (2026-05-28):**
- Instance type used: **r6g.2xlarge** (8 vCPU, 64 GiB RAM, Amazon Linux 2023 aarch64; 300 GB EBS scratch at `/scratch`)
- DIAMOND version: **2.1.11** — ⚠️ no prebuilt arm64 binary exists (GitHub ships only `diamond-linux64.tar.gz`, x86-64); **built from source** on the box (`dnf install gcc-c++ cmake make zlib-devel`, ~30s). Native aarch64 binary. Use this same source build in the Lambda arm64 image.
- MMseqs2 installed locally? **Yes** — prebuilt arm64 (`mmseqs-linux-arm64.tar.gz`; MMseqs2 *does* ship arm64, unlike DIAMOND), for local Check 8 head-to-head.

---

## Check 3 — DIAMOND DB size at three sample sizes

Extract samples from RDS using the **exact** query in `update_sequence_index.py` (`translated_sequence`, id = `genbank_accession_id` else `enzyme_{enzyme_id}`), capped with `LIMIT` at 100K, 500K, 1M.

```bash
diamond makedb --in sample_100k.fasta -d sample_100k
diamond makedb --in sample_500k.fasta -d sample_500k
diamond makedb --in sample_1m.fasta   -d sample_1m
ls -l sample_*.dmnd
```

Compute bytes/sequence at each point, confirm roughly flat, multiply the stable ratio by the Check 0 count → projected total `.dmnd` size. This number sets shard count.

Watch for: PETadex sequences skew short (IsPETase is ~290 aa), so bytes/sequence may be well below a general-corpus assumption, reducing shard count. Measure, don't assume.

> **Method deviation (justified by Check 0):** the corpus is the S3 FASTA
> `petadex.catalytic_orfs.v1.1.fa`, NOT `enzyme_fastaa` (which holds only the ~1M
> nr subset). So samples were **streamed directly from the S3 file** with early
> termination (first N sequences), using the real corpus headers — not the RDS
> `update_sequence_index.py` `LIMIT` query (which would synthesize `enzyme_{id}`
> IDs against the wrong table). makedb v2.1.11, 8 threads.

**Finding (2026-05-29):**
- 100K `.dmnd` size: **50,476,078 B**  | bytes/seq: **504.76**
- 500K `.dmnd` size: **252,949,188 B**  | bytes/seq: **505.90**
- 1M `.dmnd` size: **502,448,706 B**    | bytes/seq: **502.45**
- Ratio stable? **Yes** — spread 3.45 B/seq = **0.68%** of mean across a 10× range. (makedb scales linearly: ~9.4 s/M seqs, ~0.7 GB RSS/M seqs, 8 threads.)
- Projected total `.dmnd` size: **154.33 GB (143.73 GiB)** at 307,155,746 seqs × 502.45 B/seq. `.dmnd` ≈ 1.02× the input FASTA bytes.
  - ⚠️ **Caveat:** measured bytes/seq fell to **459.9** on the first 15.36M seqs (shard_0), so the corpus is not uniformly ordered and head-sampling slightly *overestimates*. True total is likely ≤154 GB. A striped/random sample across the whole file would tighten this before the final build.

---

## Check 4 — Set shard count from the `/tmp` ceiling

Arithmetic on measured inputs (no execution):

- `SHARD_COUNT = ceil(projected_total / 8 GB)` — 8 GB leaves headroom in Lambda's 10 GB `/tmp` for query + output
- `SHARD_SIZE = Check_0_count / SHARD_COUNT` (sequences per shard)

Write both down — Checks 5 and 6 build a concrete shard from these.

**Finding (2026-05-30):**
- SHARD_COUNT: **20**
  - `ceil(154.33 GB / 8 GB)` = 20 (decimal-GB cap, chosen). The binary reading `ceil(143.73 GiB / 8 GiB)` = 18 puts each shard at **7.99 GiB** — right at the cap with zero margin, **rejected**. 20 shards = 7.19 GiB projected / **6.58 GiB measured** (shard_0), ~1.4 GiB headroom under the 8 GiB cap.
- SHARD_SIZE (seqs/shard): **15,357,787** (= 307,155,746 / 20)

---

## Check 5 — Peak memory vs `--block-size` on one real shard

Build a single shard at SHARD_SIZE. Run `diamond blastp` with the IsPETase query, sweeping `-b`, capturing peak RSS:

```bash
for b in 0.5 1 2 4; do
  /usr/bin/time -v diamond blastp -q ispetase.fasta -d shard_0 \
    -o /dev/null -b $b --outfmt 6 2>&1 | grep "Maximum resident"
done
```

Looking for: does RSS actually move? If nearly flat, `-b` is irrelevant for the single-query pattern and WORKER_MEMORY is set by shard index size alone — record this, it closes a flagged risk (the `-b × ~6 GB` heuristic comes from large batch-query workloads, not single-sequence search).

**Finding (2026-06-01):** built shard_0 at SHARD_SIZE (15,357,787 seqs, .dmnd = 6.58 GiB). IsPETase query, default sensitivity. (All runs return 0 hits — PETase homologs aren't in the first 15.36M ORFs; RSS is governed by -b + DB size regardless of hits, so the curve is valid. Self-hit test confirmed the pipeline works.)
- RSS at -b 0.5: **0.91 GiB**
- RSS at -b 1: **1.73 GiB**
- RSS at -b 2: **3.36 GiB**
- RSS at -b 4: **6.48 GiB**
- Does `-b` move RSS meaningfully? **YES — strongly, NOT flat.** ~1.6 GiB per unit-b for a 6.58 GiB shard (base ≈0.1 GiB). The flagged `-b × ~6 GB` heuristic **overestimates ~4×**. *However* wall time is flat across -b (~34s) for the single-query pattern → higher -b buys no speed, only costs RAM. So minimize -b.
- Chosen BLOCK_SIZE: **0.5–1**
- WORKER_MEMORY: **~0.9 GiB @ -b 0.5, ~1.7 GiB @ -b 1** → set **~2 GiB** with OS/runtime headroom.

---

## Check 6 — The two wall times that gate the timeout

Same shard. **Download timing** (per-shard cold cost), 3x for variance:

```bash
for i in 1 2 3; do
  time aws s3 cp s3://petadex/diamond/spike/shard_0.dmnd /tmp/shard_0.dmnd
  rm /tmp/shard_0.dmnd
done
```

**Search timing + sensitivity sweep** — all three queries (length and divergence matter; SRR10663367 is the stress case):

```bash
for seq in ispetase fastpetase srr10663367; do
  for sens in --fast "" --sensitive --very-sensitive; do
    echo "=== $seq $sens ==="
    time diamond blastp -q $seq.fasta -d shard_0 -o out.tsv $sens --outfmt 6
    wc -l out.tsv
  done
done
```

`WORKER_TIMEOUT = (download + slowest search) × safety margin`.

**Finding (2026-06-01):**
- Per-shard download time (avg of 3): **~19.9 s** (18.6 / 20.9 / 20.2 s; ~356 MB/s) for the 6.58 GiB shard, to tmpfs `/tmp`.
- Search times by sensitivity (wall s; **all 0 hits on shard_0** — homologs absent from first 15.36M ORFs, see Check 5; timings valid):

  | query | --fast | default | --sensitive | --very-sensitive |
  |---|---|---|---|---|
  | ispetase | 22.4 | 33.7 | 191.9 | 170.0 |
  | fastpetase | 21.7 | 33.1 | 189.2 | 169.8 |
  | srr10663367 | 26.5 | 36.9 | 262.5 | 180.1 |

  Note: **`--sensitive` is SLOWER than `--very-sensitive`** (DIAMOND seeding quirk) while being *less* sensitive — so `--very-sensitive` strictly dominates it here.
- Chosen sensitivity flag: **`--very-sensitive`** (faster than `--sensitive` AND ≥ its recall; parity validated in Check 8).
- WORKER_TIMEOUT: **~300 s** = (20 s download + ~180 s slowest v-sensitive search) × 1.5 safety margin. (Fits Lambda's 900s ceiling with room. Note: bump Lambda `/tmp` from the current 4096 MB → 10240 MB and memory to ~2 GB.)

---

## Check 7 — Parallel download wall time (can break the S3 decision)

**Most important check, easiest to skip.** Single-shard download does NOT predict N concurrent cold downloads — they contend for network and S3 throughput. Fire SHARD_COUNT parallel `aws s3 cp` jobs and measure aggregate wall time:

```bash
time (for i in $(seq 0 $((SHARD_COUNT-1))); do
  aws s3 cp s3://petadex/diamond/spike/shard_$i.dmnd /tmp/shard_$i.dmnd &
done; wait)
```

(Requires all shards uploaded, or simulate with copies of one shard under N keys.)

If N-parallel cold download routinely pushes total query latency past a few minutes — and at low query volume nearly every query is cold — the **S3-over-EFS decision reopens**. This is the empirical test of the central architectural bet, not ceremony.

**Finding (2026-06-01):** staged 20 distinct shard keys in `s3://petadex/diamond/spike/` (uploaded shard_0, server-side copied to shard_1..19 with `--copy-props none` — the role lacks `s3:GetObjectTagging` which the CLI's multipart-copy path calls). Fired 20 parallel `aws s3 cp`, streamed to `/dev/null` (20 × 6.58 GiB = 131.6 GiB won't fit in 32 GiB tmpfs/RAM; streaming isolates the network/S3 contention this check targets).
- N-parallel aggregate download wall time: **125.0 s** for 20 shards (131.6 GiB), **9.04 Gbps**, 0 failures.
- Acceptable given BLAST-timescale tolerance? **Yes — with the right interpretation.** 125 s is the **single-box worst case**: 20 downloads sharing ONE NIC, saturated at the r6g.2xlarge ~10 Gbps ceiling. In the real Lambda fan-out each worker has **independent network**, and 20 *distinct* S3 keys stay well under S3's per-prefix request limits → per-worker latency ≈ the single-shard **~20 s**, not 125 s. Only co-located workers would contend.
- S3 decision holds, or reopen EFS/EC2? **HOLDS.** No EFS/EC2 redesign needed under the fan-out model.

---

## Check 8 — DIAMOND vs MMseqs2 parity (for Artem's sign-off)

Run all three example queries against current MMseqs2 (working Lambda or local) and against the DIAMOND shard at chosen sensitivity. Compare:

- Target-ID overlap (Jaccard)
- Rank correlation on shared hits
- Identity / e-value agreement on shared hits

Explicitly assert DIAMOND `pident` lands in [0,100] with **no `×100` applied** — the current code does `fident*100` for MMseqs2; the merge step must NOT re-scale DIAMOND output. This is the silent-corruption bug to catch.

SRR10663367 is the meaningful comparison (real environmental hit, lower identity, the ~40% AAI regime). The two PETase variants agree trivially and tell you little.

> **Reframe (model (a)):** the production DIAMOND target is the full **307M Logan
> corpus**, a *different database* than the **~1M nr set** the MMseqs2 baseline
> searches (the regen-JSON `target_id`s are nr/GenBank accessions). So raw-ID
> "parity" against that baseline is ill-posed — the corpus expansion is **additive
> recall**, not a parity risk. The well-posed test: run **both engines on the
> SAME `nr_enz.fa`** (1,048,671 seqs) and compare by FASTA-header target ID. Built
> a DIAMOND DB + an MMseqs2 DB from that one FASTA; ran all 3 queries through both
> (DIAMOND `--sensitive`, `--max-target-seqs 300`; `mmseqs easy-search --max-seqs 300`).

**Finding (2026-06-01):**
- Jaccard overlap per query (top-300): IsPETase **0.58**, FAST-PETase **0.58**, SRR10663367 **0.65**. By rank depth (intersection/N): **top-10 90–100%**, top-50 76–88%, top-100 73–80%, top-300 74–79%. → Head agrees strongly; tail (>100) diverges ~25% (normal DIAMOND/MMseqs2 heuristic difference, biologically irrelevant).
- Rank agreement on shared hits: **Spearman 0.82–0.87**. Rank-1 hit identical across both engines for all three queries.
- Identity/e-value agreement: **pident mean Δ < 0.3 pp** on shared hits (near-identical alignments). ⚠️ **e-values differ by 2–4.5 log10 units** (different DB-size / Karlin-Altschul calibration) — **do NOT port the MMseqs2 e-value threshold to DIAMOND; recalibrate.**
- `pident` confirmed in [0,100], no `×100`? **YES — demonstrated head-to-head.** DIAMOND emits [0,100] (100, 78.9); MMseqs2 emits fident [0,1] (1.000, 0.789). The merge step's `fident*100` is correct for MMseqs2 and **must NOT** be applied to DIAMOND. ASSERTION PASS.
- Parity scientifically acceptable? **YES.** Top hits agree strongly, identities near-identical, ranks well-correlated, and **SRR10663367 (the ~40% AAI stress case) agrees AS WELL as the easy queries** (top-10 100%, top-50 84%) → DIAMOND does not degrade in the divergent regime.

---

## Go / No-Go gate

Two findings can force a redesign before any build:

1. **Check 7** — N-parallel cold download wall time unacceptable → S3 decision reopens; reconsider EFS or small EC2.
2. **Check 8** — DIAMOND parity unacceptable at any sensitivity that fits the timeout → reconsider keeping MMseqs2 on an EC2-server model instead of switching engines.

Everything else just fills in the parameter table below.

**VERDICT: ✅ GO** (2026-06-01)
1. **Check 7** — passes under the fan-out model (per-worker ~20 s; the 125 s single-box number is a shared-NIC artifact, not production latency). S3 decision holds; no EFS/EC2 redesign.
2. **Check 8** — parity acceptable at `--very-sensitive`, which fits a ~300 s WORKER_TIMEOUT. Engine swap is safe.

**Watch-items carried into the build:**
- Do **NOT** apply `×100` to DIAMOND `pident` (only MMseqs2 needs it) — silent-corruption bug, confirmed live in Check 8.
- DIAMOND e-values differ from MMseqs2 by 2–4.5 log10 units — **recalibrate** any e-value cutoff.
- Bump Lambda config: `/tmp` 4096 → **10240 MB** (shard is 6.58 GiB), memory → **~2 GB**.
- Size projection (154 GB) is from a head-sample (502 B/seq) that drifts to 460 B/seq by 15M — confirm with a **striped sample** before the final full build (likely ≤154 GB).

---

## Settled parameter table (Phase 0 output)

| Parameter | Source | Value |
|---|---|---|
| Full corpus sequence count | Check 0 | **307,155,746** |
| Total `.dmnd` size | Check 3 | **154.33 GB / 143.73 GiB** (head-sample; likely ≤ this) |
| SHARD_COUNT | Check 4 | **20** |
| SHARD_SIZE (seqs/shard) | Check 4 | **15,357,787** (measured shard .dmnd = 6.58 GiB) |
| BLOCK_SIZE (`-b`) | Check 5 | **0.5–1** |
| WORKER_MEMORY | Check 5 | **~2 GiB** (0.9 @ -b 0.5 / 1.7 @ -b 1 + headroom) |
| Per-shard download time | Check 6 | **~19.9 s** (~356 MB/s) |
| N-parallel download wall time | Check 7 | **125 s** single-box (worst case); **~20 s/worker** in fan-out |
| Per-shard search time | Check 6 | fast ~22s / default ~34s / sens ~190–263s / **v-sens ~170–180s** |
| Sensitivity flag | Check 6 + 8 | **`--very-sensitive`** |
| WORKER_TIMEOUT | Check 6 | **~300 s** |
| WORKER_EPHEMERAL_STORAGE | fixed | 10240 MB |
| DIAMOND↔MMseqs2 parity | Check 8 | **Acceptable** (top hits agree; engine swap safe) |
