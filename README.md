# PETadex Sequence Search

> **Note: This is an internal component, not a standalone tool. It is designed to be invoked as an AWS Lambda function by the PETadex web application.**

MMseqs2-powered protein sequence similarity search against 217M+ plastic-degrading enzyme sequences from the PETadex database. Packaged as a Docker container that runs as either an AWS Lambda function or a standalone CLI.

---

## How It Works (legacy single-Lambda MMseqs2 path)

1. On invocation, downloads the MMseqs2 sequence index from S3 (`s3://petadex/mmseqs2/`) — cached in `/tmp` across warm Lambda invocations
2. Runs `mmseqs easy-search` against the index
3. Uploads results as JSON to `s3://petadex/results/{sessionId}/{job_id}.json`
4. Returns the `job_id` to the caller; the web app fetches results from S3

This single-container path searches the ~1M-sequence `petadex-nr` subset and is **still the live path** served to the web app. It is being replaced by the DIAMOND scale-out architecture below, which searches the full ~307M Logan corpus.

---

## Architecture — DIAMOND scale-out search

The scale-out architecture replaces the single MMseqs2 Lambda with a **sharded, fan-out DIAMOND search** over the full Logan-scale protein corpus (**307,155,746 sequences**). The database is split into 20 shards, each searched in parallel by its own worker Lambda, with all shards resident on **S3** (not EFS). The web-app contract (event shape, result JSON, S3 keys) is preserved, so the cutover is a function-name swap on the caller's side.

> **Status:** the DIAMOND infra is deployed and the full 307M sharded DB is built and published (`diamond/LATEST`), but the web app has not yet been cut over — it still calls the legacy MMseqs2 function. See "Cutover" below.

### Why these choices

| Decision | Choice | Rationale |
|---|---|---|
| Engine | **DIAMOND2** (`blastp --very-sensitive`) | The Logan corpus was built with DIAMOND2 — methodological consistency. More compact DB format; streams better at scale. Parity with MMseqs2 validated (top-10 90–100% overlap, near-identical identities). |
| Storage | **S3, sharded** (not EFS) | At low query volume EFS costs ~$60/mo flat; the ~120 GB sharded DB on S3 is ~$2.80/mo. Per-cold-start shard download (~78s) is acceptable at BLAST-timescale latency. |
| Compute | **Lambda fan-out** | No idle cost; scales to N shards in parallel. |
| Coordination | **Step Functions Map** | Declarative per-shard retry/catch; a failed branch aborts the whole job (fail-fast). |
| Query distribution | **Inline in worker payload** | Orchestrator validates once and passes the FASTA in each worker invocation (well under Lambda's 256 KB limit) — no shared scratch. |
| Partial failures | **Fail-fast** | A partial corpus search is not reproducible; any shard failing (after retries) fails the whole job. Result schema unchanged. |

### Components

| Component | Function | Role |
|---|---|---|
| **Orchestrator** | `petadex-diamond-orchestrator` | Validates + parses FASTA once, resolves the DB version, reads the shard manifest, mints `jobId`, returns `{job_id, s3_key}` immediately, and starts the Step Functions execution. Never block-waits. |
| **State machine** | `petadex-diamond-search` (Step Functions) | A `Map` state fans out one worker per shard (`MaxConcurrency = SHARD_COUNT`). Per-branch retry on transient `Lambda.*` errors; `Catch: States.ALL` → fail-fast. Final state invokes the aggregator. |
| **Worker** (×20 in parallel) | `petadex-diamond-worker` | Downloads its assigned shard `.dmnd` to `/tmp` (cached across warm invocations, evicting any other shard first), runs `diamond blastp`, writes a raw outfmt-6 TSV part to S3. One worker per shard. |
| **Aggregator** | `petadex-diamond-aggregator` | Merges all parts → sorts by bitscore desc / evalue asc → top-K → enriches from RDS once → writes the final result JSON + `.index` pointer in the unchanged schema. |

All four share **one Docker image** (ARM64, DIAMOND built from source — no arm64 release binary exists); the Lambdas are distinguished by their `ImageConfig.Command` (`orchestrator.handler` / `worker.handler` / `aggregator.handler`). The legacy `mmseqs` binary remains in the image during the transition.

### Data flow

```mermaid
flowchart TD
    WA["🌐 PETadex web app"]

    subgraph ORCH["⚙️ Orchestrator Lambda"]
        direction TB
        O1["validate + parse FASTA once"] --> O2["read diamond/LATEST → version"] --> O3["read manifest.json → shard list"] --> O4["mint jobId · StartExecution"]
    end

    S3DB[("🗄️ S3 · diamond/{version}/<br/>20 × shard_NN.dmnd + manifest")]

    subgraph SFN["🔀 Step Functions · Map (MaxConcurrency = 20)"]
        direction LR
        W0["Worker<br/>shard 0"]
        W1["Worker<br/>shard 1"]
        WDOTS["…"]
        W19["Worker<br/>shard 19"]
    end

    PARTS[("🗄️ S3 · results/{sid}/{jid}/parts/<br/>shard_N.tsv + shard_N.meta.json")]

    subgraph AGG["🧬 Aggregator Lambda · final Map state"]
        direction TB
        A1["merge parts · assert 0≤pident≤100"] --> A2["sort bitscore↓ / evalue↑ · top-K"] --> A3["fetch_metadata (once)"] --> A4["write result + index + timing"]
    end

    RDS[("🛢️ RDS<br/>blast_nr_metadata")]
    RESULT[("🗄️ S3 · results/{sid}/{jid}.json<br/>+ results/{sid}.index + timing.json")]

    WA -- "{ action, sessionId, sequence, max_results }" --> ORCH
    ORCH -. "returns { job_id, s3_key } immediately" .-> WA
    O2 <-->|read| S3DB
    O3 <-->|read| S3DB
    O4 --> SFN
    S3DB -- "download shard → /tmp (evict prior)" --> SFN
    SFN -- "diamond blastp --very-sensitive" --> PARTS
    PARTS --> AGG
    RDS <-->|enrich| A3
    AGG --> RESULT
    WA -. "poll .index → fetch result JSON" .-> RESULT

    classDef store fill:#1f2937,stroke:#60a5fa,color:#e5e7eb;
    classDef lambda fill:#3b2f12,stroke:#f59e0b,color:#fde68a;
    class S3DB,PARTS,RESULT,RDS store;
```

> **Fail-fast:** each Map branch retries transient `Lambda.*` errors; if any shard still fails, the whole execution fails (`Catch: States.ALL`) — no result JSON or `.index` is written, so the poller simply never sees a completion.

**Temporal view** — note the orchestrator returns *before* the search runs; the aggregator publishes the result minutes later, and the web app bridges the gap by polling:

```mermaid
sequenceDiagram
    autonumber
    participant WA as Web app
    participant O as Orchestrator
    participant SF as Step Functions
    participant W as Workers ×20
    participant AG as Aggregator
    participant S3 as S3

    WA->>O: search { sessionId, sequence }
    O->>S3: read diamond/LATEST + manifest.json
    O->>SF: StartExecution(version, shards[])
    O-->>WA: { job_id, s3_key }  (immediate, ~1s)
    Note over WA,S3: web app starts polling results/{sid}.index

    par fan-out — all 20 shards in parallel (~5 min)
        SF->>W: invoke worker(shard N)
        W->>S3: download shard_N.dmnd → /tmp
        W->>W: diamond blastp --very-sensitive
        W->>S3: write parts/shard_N.tsv + meta
    end

    SF->>AG: aggregate (only after every part lands)
    AG->>S3: read all 20 parts
    AG->>AG: merge → sort → top-K → enrich (RDS)
    AG->>S3: write {jobId}.json + .index + timing.json
    WA->>S3: poll finds .index → fetch result
```

### Sharded database on S3

The corpus FASTA (`s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa`) is streamed and split round-robin into 20 shards; each is built with `diamond makedb` into a `.dmnd` and uploaded. Built offline by `scripts/build_diamond_shards.py` (an arm64 box with ~300 GB scratch — not a laptop job).

```
s3://petadex/diamond/
├── LATEST                                    # text pointer → current version dir
└── {version}/                                # e.g. catalytic_orfs_v1.1_20260602_222538
    ├── manifest.json                         # version, corpus, shard_count, per-shard key/sequences/bytes
    ├── shard_00.dmnd   (~5.6 GiB)            # 20 shards, ~15.36M sequences each
    ├── shard_01.dmnd
    ├── ...
    └── shard_19.dmnd
```

**Atomic versioning** (prevents mid-rebuild races): write all shards → write `manifest.json` → **then** update `LATEST`. The orchestrator reads `LATEST` once per job and passes the resolved version into the execution, so all workers pin to one version even if `LATEST` flips mid-job.

### Key runtime parameters

| Parameter | Value | Notes |
|---|---|---|
| Corpus size | 307,155,746 sequences | full Logan catalytic-ORF corpus |
| Total `.dmnd` | ~112 GiB / 120 GB | 20 shards, ~5.6 GiB each |
| `SHARD_COUNT` | **20** | bounded by `/tmp` (a shard must fit in 10 GB with headroom) |
| Worker memory | **10240 MB** (Lambda max, ≈6 vCPU) | DIAMOND `--very-sensitive` is CPU-bound; vCPU count sets search wall time |
| Worker `/tmp` | **10240 MB** | holds one ~6 GB shard + query + output |
| Worker timeout | **600 s** | ~78 s download + ~190 s search ≈ ~290 s/shard, with headroom |
| Worker reserved concurrency | **100** | must exceed peak burst (3 concurrent jobs × 20 = 60), not equal it |
| Sensitivity | **`--very-sensitive`** | strictly dominates `--sensitive` (faster *and* higher recall); low-memory streaming path |
| `-b` (block size) | **1** | bounds RAM, not disk |
| Per-shard timing | download ~78 s, search ~190 s, total ~290 s | job wall ~5 min |

### Result S3 layout & contract

The **contract is unchanged** from the legacy path, so the web app needs only a function-name swap:

```
results/{sessionId}.index                          # 36-byte pointer: the jobId
results/{sessionId}/{jobId}.json                   # final result (schema below)
results/{sessionId}/{jobId}/parts/shard_N.tsv      # raw worker outputs (aggregator input)
results/{sessionId}/{jobId}/parts/shard_N.meta.json# per-shard timing sidecar
results/{sessionId}/{jobId}/timing.json            # job-level timing rollup
```

The result JSON keeps `{ query_header, query_sequence, query_length, num_results, results[] }` (each hit: `target_id, query_start, query_end, target_start, target_end, alignment_length, percent_identity, evalue, bitscore, metadata`), plus **additive identity stamps** the web app may ignore or render: `engine` (`diamond`), `database`, `database_version`, `db_sequence_count`. Note Logan target IDs are ORF IDs (not GenBank accessions), so `metadata` is typically `null` for Logan hits.

### Scoring & e-values

`percent_identity` (0–100), `bitscore`, and the alignment coordinates are taken straight from DIAMOND. Results are ranked by **bitscore** (tiebreak: e-value), which is correct across shards because a bit score depends only on the scoring system, not database size.

**E-values, however, scale with database size** — so a shard searched in isolation would report e-values calibrated against only ~1/20 of the corpus (≈20× too significant). To fix this, every worker is given `--dbsize <total corpus residues>` (the manifest's `total_letters`, threaded orchestrator → worker), so e-values are calibrated against the **full corpus** and match a single full-database search. Bit scores are unaffected. (Engineering detail in `docs/evalue-calibration.md`; if a manifest lacks `total_letters`, workers omit `--dbsize` and fall back to per-shard e-values.)

### Telemetry

Each worker writes a standalone `shard_N.meta.json` sidecar (download/search ms, shard size, hit count, status) from its `finally`, so even a failed shard leaves a breadcrumb. The aggregator rolls these into `timing.json` (total wall, slowest shard, per-shard array). Telemetry lives **beside** the result, not inside it, so it survives the fail-fast path and needs no contract change.

### Failure policy (fail-fast)

If any shard fails after the Step Functions per-branch retries, the **whole job fails** — no partial results, no `incomplete` flag, schema unchanged. A failed job writes a `timing.json` with `status: "failed"` and per-shard `error` fields, but **no result JSON / `.index`** — so a poller sees the search simply never complete (the caller should apply its own timeout).

### Cutover

To move the web app from nr → Logan: point its search invocation from `petadex-mmseqs2-search` to **`petadex-diamond-orchestrator`** and grant its role `lambda:InvokeFunction` on the orchestrator. Because the contract is preserved, no result-parsing changes are needed; latency rises from ~58 s to ~5 min, so the caller should add a poll timeout / failure state.

---

## Project Structure

```
petadex-sequence-search/
├── lambda_function.py          # Lambda handler (search + history actions)
├── cli.py                      # CLI entrypoint (outputs JSON to stdout)
├── Dockerfile                  # ARM64 Lambda image with MMseqs2
├── requirements.txt
├── scripts/
│   ├── update_sequence_index.py  # Rebuilds MMseqs2 index from PostgreSQL
│   ├── setup_s3_access.sh        # S3 bucket/IAM setup helper
│   ├── .env.example
│   └── README.md
└── .github/workflows/
    ├── deploy.yml                # Auto-deploy to Lambda on push to main
    ├── docker-publish.yml        # Publish image to Docker Hub on tag
    └── update-database.yml.example
```

---

## Lambda API

The Lambda function accepts two actions via the event body.

**Search:**
```json
{
  "action": "search",
  "sessionId": "abc123",
  "sequence": "MKLLIVLLAACLAVFAAAEPQIAVV",
  "max_results": 50
}
```

Response:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "s3_key": "results/abc123/550e8400-e29b-41d4-a716-446655440000.json"
}
```

**History:**
```json
{
  "action": "history",
  "sessionId": "abc123"
}
```

Response:
```json
{
  "history": [
    { "job_id": "...", "s3_key": "...", "last_modified": "...", "size": 1234 }
  ]
}
```

---

## Deployment

Pushes to `main` automatically build and deploy via GitHub Actions (`deploy.yml`):
1. Bumps the semver tag
2. Builds a `linux/arm64` Docker image and pushes to ECR (`petadex-mmseq2-search`)
3. Updates the Lambda function (`petadex-mmseqs2-search`) with the new image

Manual deploy:
```bash
export DOCKER_DEFAULT_PLATFORM=linux/arm64

aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin \
  YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com

docker buildx build --platform linux/arm64 --provenance=false -t petadex-search .

docker tag petadex-search:latest \
  YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/petadex-mmseq2-search:latest
docker push YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/petadex-mmseq2-search:latest

aws lambda update-function-code \
  --function-name petadex-mmseqs2-search \
  --image-uri YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/petadex-mmseq2-search:latest
```

---

## Local Testing

```bash
docker build -t petadex-search .

# Lambda mode
docker run -p 9000:8080 \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  petadex-search

curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"action":"search","sessionId":"test","sequence":"MKLLIVLLALAVAALHAQQGVGAPVP","max_results":10}'

# CLI mode (results to stdout, no S3 upload)
docker run --rm \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  --entrypoint python3 \
  petadex-search cli.py "MKLLIVLLALAVAALHAQQGVGAPVP" 10
```

---

## Database Updates

The MMseqs2 index is version-controlled via a `LATEST` pointer file in S3. To rebuild the index from the PETadex PostgreSQL database:

```bash
cp scripts/.env.example scripts/.env
# Fill in DB credentials

pip install -r scripts/requirements.txt
brew install mmseqs2

source scripts/.env
./scripts/update_sequence_index.py
```

This extracts sequences from the `enzyme_fastaa` table, builds a new timestamped MMseqs2 index, uploads it to `s3://petadex/mmseqs2/{version}/`, and updates the `LATEST` pointer. No Lambda redeployment needed — the next cold start picks up the new version automatically.

---

## Input Validation

- Valid amino acids: `ACDEFGHIKLMNPQRSTVWY`
- Minimum length: 10 amino acids
- Maximum length: 10,000 amino acids

---

## Tech Stack

- **Search**: DIAMOND2 `blastp --very-sensitive` (scale-out, current target) · MMseqs2 `easy-search` (legacy single-Lambda, still live)
- **Runtime**: Python 3.11 on AWS Lambda ARM64 (Graviton)
- **Coordination**: AWS Step Functions (`Map` fan-out) across orchestrator / worker / aggregator Lambdas
- **Storage**: Amazon S3 (`petadex` bucket) — sharded `.dmnd` database + results
- **Enrichment**: PETadex RDS (PostgreSQL) `blast_nr_metadata`
- **CI/CD**: GitHub Actions → ECR → Lambda
- **Database source**: full Logan catalytic-ORF corpus, 307M sequences (DIAMOND) · `enzyme_fastaa` ~1M nr subset (legacy MMseqs2)
