# PETadex Sequence Search — Scale-Out Architecture Plan

Migrate the current single-Lambda MMseqs2 search to a sharded, fan-out **DIAMOND** architecture that searches the full Logan-scale protein database in parallel across many worker Lambdas, with all shards resident on **S3** (not EFS).

Companion: [[Phase 0 Spike - Manual Checks]] — pre-build measurement runbook.

---

## 1. Current state (baseline)

| Piece | Today |
| --- | --- |
| Engine | MMseqs2 `easy-search` |
| Compute | One ARM64 container Lambda (`petadex-mmseqs2-search`) |
| DB storage | `s3://petadex/mmseqs2/{version}/`, `LATEST` pointer; downloaded to `/tmp`, cached across warm invocations |
| Index build | `scripts/update_sequence_index.py` extracts `enzyme_fastaa.translated_sequence` from RDS, builds MMseqs2 DB, uploads, bumps `LATEST` |
| Enrichment | `fetch_metadata()` joins hits to RDS `blast_nr_metadata` |
| Output | `results/{sessionId}/{jobId}.json` + `results/{sessionId}.index` (job_id pointer) |
| Consumer | Express web app polls the `.index` file, then the result JSON |
| Actions | `search`, `history` |
| Current scale | ~1M sequences (petadex-nr subset) |
| Target scale | Full Logan corpus (count TBD by Spike Check 0; "~300M" working figure) |

**Measured baseline (warm container, empty `/tmp`):**
- DB download: ~48s (3.2 GB `.idx` file alone is ~44s)
- MMseqs2 search: ~9s
- Total: ~58s — **download dominates, search is cheap**

**Contract to preserve (so the web app needs zero changes):**
- Lambda event shape: `{ action, sessionId, sequence, max_results }`
- Result JSON shape: `{ query_header, query_sequence, query_length, num_results, results[] }` where each result has `target_id, query_start, query_end, target_start, target_end, alignment_length, percent_identity, evalue, bitscore, metadata`
- S3 keys: `results/{sessionId}/{jobId}.json` and `results/{sessionId}.index`

---

## 2. Target architecture

```
client → Orchestrator Lambda
              │  validate + parse FASTA once, mint jobId, pass query inline
              ├── fan out → Worker Lambda  (shard 0)  → parts/shard_0.tsv
              ├── fan out → Worker Lambda  (shard 1)  → parts/shard_1.tsv
              │             ...                          ...
              └── fan out → Worker Lambda  (shard N-1) → parts/shard_{N-1}.tsv
              │
              ▼ (S3 event triggers aggregator when last part lands)
         Aggregator: merge → sort by bitscore/evalue → top-K
                     → enrich from RDS (once)
                     → write results/{sessionId}/{jobId}.json + .index
```

### Decision: S3 not EFS
At low query frequency, EFS costs ~$60/mo flat regardless of use; S3 for ~200 GB is ~$4.60/mo with negligible read fees at this volume. The per-cold-start shard download penalty is acceptable because users already expect BLAST-timescale latency.

**Reopen condition:** if Spike Check 7 (N-parallel cold download wall time) returns an unacceptable aggregate, OR if query volume rises enough that repeated shard downloads dominate cost/latency, revisit EFS or a small persistent EC2 instance.

### Decision: DIAMOND not MMseqs2
Logan itself used DIAMOND2 — the dataset users are searching against was built with DIAMOND, so methodological consistency favors the same engine for the search interface. DIAMOND's database format is more compact than MMseqs2's, and it streams chunks better at scale.

### Decision: query passed inline
Orchestrator validates the query once and passes the FASTA in the worker invocation payload (well under Lambda's 256 KB limit). No shared scratch location, no extra S3 round-trip, no read-after-write consistency thinking.

### Decision: atomic shard versioning
Database rebuilds use a version-pinned convention to avoid mid-upload races:
1. Write all shards under `s3://petadex/diamond/{version}/shard_{i}.dmnd`.
2. Write `s3://petadex/diamond/{version}/manifest.json`.
3. **Then** update `s3://petadex/diamond/LATEST`.
4. Orchestrator reads `LATEST` once per job and passes the resolved version to all workers — workers pin to that version even if `LATEST` flips mid-job.

---

## 3. Hard constraints that drive the design

1. **Lambda `/tmp` ≤ 10 GB.** A worker must hold its `.dmnd` shard on local disk — DIAMOND cannot read the database from S3. So **each shard's `.dmnd` file must fit in `/tmp` with headroom for query + output** (target ≤ ~8–9 GB). This — not RAM — sets the minimum shard count: `total_dmnd_size / ~8 GB`.
2. **Lambda memory ≤ 10 GB (≈6 vCPU at max).** DIAMOND peak RSS *may* scale with `--block-size (-b)` (the `-b × ~6 GB` heuristic comes from large batch-query workloads; for single-query search, RSS may be dominated by the shard's seed index regardless). Spike Check 5 resolves this. `--block-size` bounds **RAM, not disk** — it does *not* remove the `/tmp` shard-size constraint above.
3. **Lambda max timeout 15 min.** A worker must finish (download + search) one shard within this. The orchestrator must NOT block-wait — use event-driven aggregation.
4. **DIAMOND ≠ MMseqs2 in scoring/sensitivity.** Hit sets and identities will differ; parity must be validated, not assumed.
5. **Warm-cache shard affinity is not guaranteed.** Lambda routing does not preserve "container X holds shard X" affinity across invocations. Warm caching will help less than the design hopes. Plan for cold-dominated economics.

---

## 4. Phased plan

### Phase 0 — Spike & finalize parameters
**Status:** runbook drafted in [[Phase 0 Spike - Manual Checks]]. Execute before any build.

Output: a settled parameter table — SHARD_COUNT, SHARD_SIZE, BLOCK_SIZE, WORKER_MEMORY, WORKER_TIMEOUT, sensitivity flag, parity assessment.

**Go/no-go gates** (only two findings can force redesign):
- Check 7 — N-parallel cold download wall time unacceptable → reopen S3 vs EFS/EC2.
- Check 8 — DIAMOND parity scientifically unacceptable at any sensitivity that fits the timeout → reconsider keeping MMseqs2 on an EC2-server model instead of switching engines.

Everything else just fills in numbers.

### Phase 1 — Build the sharded DIAMOND database (offline pipeline)
- [ ] New `scripts/build_diamond_shards.py`:
  - **Source: stream the full corpus FASTA from S3** — `s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa` (307,155,746 seqs, confirmed by Spike Check 0). Do **not** run the old `enzyme_fastaa` RDS extraction query — that table holds only the ~1M nr subset and would defeat the entire scale-out. Read the S3 object as a stream (don't load it into memory); keep each record's native corpus header as the sequence ID.
  - Split sequences into SHARD_COUNT FASTA files (contiguous chunks are simplest; round-robin balances size if lengths vary).
  - `diamond makedb --in shard_i.fasta -d shard_i` → `shard_i.dmnd`.
  - Upload all shards to `s3://petadex/diamond/{version}/shard_{i}.dmnd`.
  - Write `s3://petadex/diamond/{version}/manifest.json` (shard count, per-shard size, sequence counts, version, build date).
  - **Last:** update `s3://petadex/diamond/LATEST`. Atomic versioning per Section 2.
  - **Run the striped-sample corpus-size check (§9.1) before the full build** — it sets SHARD_COUNT and the per-shard size that must fit `/tmp`.
- [ ] Keep `mmseqs2/` prefix and `update_sequence_index.py` fully working during transition.

### Phase 2 — Worker Lambda
- [ ] Add the `diamond` arm64 binary to the `Dockerfile`; keep `mmseqs` during transition. Single image can serve both worker and orchestrator roles (handler dispatched by `action`).
- [ ] Worker entry (`action: "worker"` or separate function):
  - Input: `{ sessionId, jobId, shardIndex, version, queryFasta, max_results }` (query pre-validated, version pinned by orchestrator).
  - Download `s3://petadex/diamond/{version}/shard_{shardIndex}.dmnd` to `/tmp`, cached across warm invocations (same pattern as today's `download_database`, but version-pinned not LATEST-following).
  - `diamond blastp -q /tmp/query.fasta -d shard_i -o /tmp/part.tsv -b BLOCK_SIZE -k max_results --threads <vCPUs> --outfmt 6 sseqid qstart qend sstart send length pident evalue bitscore` plus chosen sensitivity flag.
  - Write partial result to `s3://petadex/results/{sessionId}/{jobId}/parts/shard_{i}.tsv`.
  - **Gotcha:** DIAMOND `pident` is already a percentage (0–100); the current MMseqs2 code does `fident*100`. The merge step must NOT re-scale DIAMOND output. Assert `0 ≤ pident ≤ 100` in the merger.

### Phase 3 — Orchestrator Lambda
- [ ] Reuse existing `parse_fasta` + `validate_sequence`; validate ONCE here.
- [ ] Read `s3://petadex/diamond/LATEST` once, resolve to version; pass to all workers in payload.
- [ ] Mint `jobId`; fan out one async worker invocation per shard (read `manifest.json` for SHARD_COUNT).
- [ ] Return `{ job_id, s3_key }` to caller immediately. Do NOT block-wait.
- [ ] Keep `history` action unchanged.

### Phase 4 — Coordination / aggregation

**Locked: Step Functions Map** (chosen over the S3-event aggregator because fail-fast needs active failed-shard detection — see Phase 5 and §9.2):
- Orchestrator starts a Step Functions execution; a `Map` state runs one worker task per shard (input = manifest shard list), max concurrency = SHARD_COUNT.
- Per-branch `Retry` on transient errors (S3/Lambda throttle) with backoff; per-branch `Catch` → the execution fails the whole job (fail-fast).
- Each worker still writes its `parts/shard_{i}.tsv` to S3; the final aggregator state reads all parts, merges → sort by `bitscore` desc (tiebreak `evalue` asc) → truncate to `max_results` → enrich via existing `fetch_metadata` (one RDS round-trip) → write `results/{sessionId}/{jobId}.json` + `results/{sessionId}.index` in the **unchanged** shape.
- Because the `Map` join only reaches the aggregator state once, after all branches succeed, there is no multi-trigger idempotency problem (the issue that the S3-event pattern would have had).

Documented fallback if Step Functions infra/IAM proves heavier than wanted:

| Option | How | Trade-off |
| --- | --- | --- |
| S3-event aggregator | Worker writes part → `ObjectCreated` triggers aggregator → list+count, fire on last. | No new infra, but fail-fast is awkward (a dead worker writes no part, so "failed" is indistinguishable from "still running" without a separate timeout signal); needs careful multi-trigger idempotency. |
| Async invoke + S3 poll | Orchestrator invokes N workers async, then polls for N parts. | Simple, no new infra; orchestrator pays for wait, fragile on partial failure. |
| DynamoDB counter | Each worker decrements a per-job counter; last worker triggers aggregator. | Event-driven; extra table + careful idempotency. |

### Phase 5 — Partial-failure policy
**Locked: fail-fast** (Artem — best-effort introduces reproducibility problems for a search users cite):

- If any shard fails after the Step Functions per-branch retries, the whole job fails. Result schema **unchanged** — no `incomplete` field, no contract change for the web app.
- The retry/abort burden lives in the Step Functions `Map` retry+catch policy (Phase 4), which is why Step Functions was chosen over the S3-event aggregator.
- Rejected alternative — best-effort (`incomplete: true` flag, return whatever completed): better raw UX, but a partial corpus search is not reproducible and could mislead, so it was ruled out on scientific grounds.

### Phase 6 — Deploy & infra
- [ ] Update `.github/workflows/deploy.yml` to build the diamond-enabled image and deploy orchestrator + worker functions (one image, two function bindings).
- [ ] IAM:
  - Worker: read `s3://petadex/diamond/*`, write `s3://petadex/results/*/parts/*`.
  - Orchestrator: invoke workers, read `s3://petadex/diamond/LATEST` and `manifest.json`.
  - Aggregator: read `s3://petadex/results/*/parts/*`, write `s3://petadex/results/*`, Secrets Manager (`DB_SECRET_ARN`), RDS reach.
- [ ] Provision worker: WORKER_MEMORY, WORKER_TIMEOUT, ephemeral storage 10240 MB.
- [ ] **Concurrency model:** at low query volume, prefer **reserved concurrency** (caps without pre-warming) over **provisioned concurrency** (pre-warmed but pays 24/7). Set reserved concurrency ≥ SHARD_COUNT × expected concurrent queries. Revisit if query volume rises.

### Phase 7 — Cutover & cleanup
- [ ] Run DIAMOND and MMseqs2 side by side on `example-sequences.txt` (IsPETase, FAST-PETase, SRR10663367); confirm parity per Spike Check 8.
- [ ] Update the "Regenerate example searches" step in `deploy.yml` to the new path.
- [ ] Point the web app at the orchestrator (no code change if the contract held).
- [ ] Remove MMseqs2 (`mmseqs` from Dockerfile, old code paths, `mmseqs2/` prefix) once validated. Update `README.md` and this file.

---

## 5. Decisions made (locked unless reopened)

| Decision | Locked value | Reopen if |
|---|---|---|
| Engine | DIAMOND2 (arm64) | Parity unacceptable (Check 8) |
| Storage | S3, sharded | N-parallel download too slow (Check 7) or volume rises |
| Compute | Lambda, fan-out | Query volume needs persistent warm instance |
| Coordination | Step Functions Map | (lock) |
| Query distribution | Inline in worker payload | (unlikely to reopen) |
| Versioning | Atomic: shards → manifest → LATEST | (lock) |
| Partial failures | Fail-fast (any shard fails after retries → whole job fails) | (lock; Artem — reproducibility) |
| Concurrency model | Reserved, not provisioned | Latency complaints at cold-start |

## 6. Open questions / decisions to finalize

- Final **SHARD_COUNT / SHARD_SIZE** (driven by `/tmp` ceiling, measured in Phase 0).
- **BLOCK_SIZE** and **WORKER_MEMORY** (DIAMOND RSS vs `-b`, measured in Phase 0).
- **Sensitivity flag** — **locked: `--very-sensitive`**, exposed as an explicit container flag. Part B ladder benchmark (24 rows, shard_0) shows it strictly dominates `--sensitive`/`--more-sensitive` on all three axes — faster (180s vs 264s slowest-query), higher recall (803 vs 774 nr hits), and 6–12× less memory (1.5 GB vs 9.9 GB RSS), because DIAMOND switches to a low-memory streaming path at this tier. Captures ~98% of `--ultra-sensitive` recall at 1/5 the cost. `--mid-sensitive` (128s, 69% recall) is the only honest cheaper rung if a tiered model is ever wanted.
- **Full Logan corpus location** — if Spike Check 0 confirms `enzyme_fastaa` only holds the nr subset, locate the full set before Phase 1.

## 7. Key risks

- **Cold-start storm at low query volume.** Nearly every query is cold; N-parallel shard downloads dominate wall time. Mitigated by Spike Check 7 measuring the actual aggregate, and by the reopen condition on S3 vs EFS.
- **Warm-cache shard affinity is not guaranteed.** Plan for cold-dominated economics; do not over-rely on warm cache savings.
- **Result divergence DIAMOND vs MMseqs2.** Sensitivity/scoring differ; needs validation (Check 8).
- **`/tmp` vs RAM confusion.** `--block-size` bounds memory, not disk; shards must still fit `/tmp`. Reinforced in Phase 0 Check 5.
- **`pident` scale silent-corruption bug.** DIAMOND returns 0–100 directly; do not re-scale in the merger. Assert in code.
- **Mid-upload version race.** Atomic versioning convention (shards → manifest → LATEST, workers pin version) prevents this.
- **Orchestrator idle cost.** Avoided by S3-event-driven aggregation; orchestrator returns immediately.
- **Cost at scale.** N invocations + N downloads per query — fine at low volume, basis of the S3 decision; revisit if volume grows.

---

## 8. References

- [[Phase 0 Spike - Manual Checks]] — measurement runbook to execute before Phase 1.
- `petadex-sequence-search-main/lambda_function.py` — current single-Lambda implementation.
- `petadex-sequence-search-main/scripts/update_sequence_index.py` — current MMseqs2 index build.
- `example-sequences.txt` — IsPETase, FAST-PETase, SRR10663367 — used for parity and sensitivity stress tests.

---

## 9. Open decisions / to reconcile

Most architecture decisions are locked in Section 5. The items below are the ones still genuinely open — either flagged as needing confirmation, carried forward from the spike as watch-items, or contradicted by a spike finding the plan hasn't yet absorbed. Grouped by type.

### 9.1 Explicitly open (Section 6 carry-overs)

- **Final SHARD_COUNT / SHARD_SIZE.** Provisionally 20 shards / 15,357,787 seqs each. The spike's 154 GB projection came from a *head-sample* whose ratio drifts (502 → 460 B/seq by the first 15M seqs), so true total is likely ≤154 GB and the shard count could shift. **Confirm with a striped/random sample across the whole corpus before locking** — the entire shard arithmetic keys off this number.

> The thinking here is that by scaling the number of shards, each search becomes more expensive but also the overall search time becomes faster.

- **BLOCK_SIZE / WORKER_MEMORY.** Effectively settled by Spike Check 5 (`-b 0.5–1`, ~2 GiB). Open only as a formality: the measured values need to be written into the build/deploy config, not re-decided.

- **Sensitivity flag.** **Resolved: `--very-sensitive`**, as an explicit container flag. The Part B ladder benchmark settled it: `--very-sensitive` strictly dominates `--sensitive`/`--more-sensitive` on speed, recall, *and* memory (the tier switches DIAMOND to a low-memory streaming path — 1.5 GB vs 9.9 GB RSS), and captures ~98% of `--ultra-sensitive`'s recall at 1/5 the cost. Note: the earlier instinct to pick `--sensitive` would have selected a strictly dominated option (slower *and* less sensitive *and* 6–12× more memory). Flag stays explicit so `--mid-sensitive` remains available as the one honest cheaper rung (128s, 69% recall) if a tiered model is ever wanted.

### 9.2 Resolved

- **Partial-failure policy.** **Resolved: fail-fast** (Artem — best-effort creates reproducibility problems). Section 5 table updated to match. Consequence: result schema stays unchanged (no `incomplete` field), so there is no contract change for the web app. The cost moves to coordination — the layer must actively detect a failed/timed-out shard and abort the whole job, which is one reason Step Functions (below) is the better fit than the S3-event aggregator.

- **Coordination mechanism.** **Resolved: Step Functions Map.** The `Map` state fans out one worker per shard with declarative per-branch retry/catch, and a failed branch aborts the execution — which is exactly what fail-fast needs (a dead worker writes no part, so the S3-event "count the parts" pattern can't distinguish still-running from failed without a separate timeout signal). Step Functions gives the abort + retry semantics for free and makes the failure visible in the execution graph. Section 5 table updated.

### 9.3 Plan text not yet reconciled with a spike finding

- **Phase 1 build-pipeline data source.** Phase 1 (Section 4) still says `build_diamond_shards.py` should *"reuse the `enzyme_fastaa` extraction query."* But Spike Check 0 established that `enzyme_fastaa` holds only the ~1M nr subset, and the real corpus is the S3 FASTA `s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa` (307,155,746 seqs). The spike already worked around this by streaming samples directly from the S3 file with the real corpus headers, *not* the RDS `LIMIT` query (which would synthesize `enzyme_{id}` IDs against the wrong table). **The shard-build pipeline must read from the S3 corpus FASTA, not run the RDS extraction query — Phase 1 text contradicts this and needs correcting.** Left inline in Phase 1 as-is for now; flagged here for reconciliation.

### 9.4 Already settled (noted to avoid re-litigating)

- **Full Logan corpus location** — resolved by Check 0 (the S3 FASTA above). Answered; the only residue is the Phase 1 contradiction in 9.3.
- **Concurrency model** — reserved (not provisioned), per Section 5 and the cold-start cost analysis. Treat as settled.
- **Engine / storage / query distribution / versioning** — locked in Section 5; reopen only on the stated conditions.
