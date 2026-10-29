# Infrastructure (`infra/`)

AWS resources around the search: the Step Functions definition, IAM policies, the alarm relay, the data-flow diagram, and the live Lambda configuration. Overview: [main README](../README.md).

| Path | What |
|---|---|
| [search_state_machine.asl.json](search_state_machine.asl.json) | `petadex-diamond-search`: `Map` over shards (one worker each, per-branch retry, `Catch: States.ALL` → fail-fast) → aggregator; on failure writes `results/{sessionId}.error` |
| [iam/](iam/) | Least-privilege policies per Lambda, the align invoke grants, and the GitHub Actions deploy role |
| [alarms/ntfy_notifier.py](alarms/ntfy_notifier.py) | SNS → ntfy relay for the search alarms (set up by [setup_search_alarms.sh](../scripts/ops/setup_search_alarms.sh)) |
| [architecture.d2](architecture.d2) → [architecture.svg](architecture.svg) | Data-flow diagram. Regenerate: `d2 --theme 200 --sketch infra/architecture.d2 infra/architecture.svg` |

## Key runtime parameters

| Parameter | Value | Notes |
|---|---|---|
| Corpus size | 298,687,346 sequences / 88,181,580,142 residues | v260701 (v1.1 was 307,155,746 / 102,929,980,061) |
| Total shard size | 24.34 GiB | 32 `.fa.zst` shards, ~779 MB each |
| `SHARD_COUNT` | **32** | ~2.76 G residues per shard; peak worker RSS ~4.2 GB at `-b1` |
| Worker memory | **10240 MB** (Lambda max, ≈6 vCPU) | DIAMOND search is CPU-bound; vCPU count sets search wall time |
| Worker `/tmp` | **10240 MB** | holds one ~0.8 GB shard (cached per S3 key) + query + output; an uncapped export part is up to ~100 MB |
| Worker timeout | **600 s** | a normal shard takes ~30–45 s; the slowest export shard measured 116 s |
| Worker reserved concurrency | **200** | 32 workers per search; the orchestrator admits at most 4 running public searches (128 workers) |
| Orchestrator | **512 MB / 30 s**, reserved concurrency **2** | serializes the admission check-then-start (see [Public-search limits](../src/README.md#public-search-limits--alarms)) |
| Align | **4096 MB / 300 s** | FAMSA + FastTree, ~10 s for ISPETase |
| Aggregator | **2048 MB / 600 s / 6144 MB `/tmp`** | sized for the export merge (7.9M rows + 1.4 GB FASTA in ~3 min); a normal aggregation takes ~3 s |
| Sensitivity | **default** (no flag) | recall-for-speed trade chosen 2026-06-04 over the Phase-0 `--very-sensitive` lock (~34 s vs ~170 s per shard). Env `DIAMOND_SENSITIVITY` overrides (empty/`default`/`none` ⇒ no flag). Re-validate parity if distant homologs matter. |
| `-b` (block size) | **1** | bounds RAM, not disk |
| Per-shard timing | download ~8 s, search ~21–33 s (default sensitivity) | job wall ~47–50 s end to end incl. ~3 s enrichment (v260701, 1.6.0, 2026-10-28) |

## Deployment

One ARM64 image serves all roles. Pushes to `main` build and deploy via GitHub Actions (`deploy.yml`):
1. Bumps the semver tag
2. Builds a `linux/arm64` image (on a native arm64 runner, with GHA layer cache, since the DIAMOND source compile is slow under QEMU) and pushes to ECR
3. `update-function-code` on the four DIAMOND functions (`petadex-diamond-{orchestrator,worker,aggregator,align}`), then updates the Step Functions state-machine definition (the CI role can't yet, so apply ASL changes by hand). Each DIAMOND step no-ops with a `::notice::` if the resource isn't provisioned yet, so a push never breaks the pipeline.
4. Regenerates the example searches.

Docs-only commits can skip the rebuild with `[skip ci]` in the commit message.

**One-time provisioning** (admin IAM, not the CI/build role) creates the orchestrator, worker and aggregator, the `petadex-diamond-search` state machine, and their IAM roles from the same image; the align Lambda has its own script, [provision_align.sh](../scripts/deploy/provision_align.sh):
```bash
METADATA_STORE=s3://petadex/metadata/v260701/<tag> ./scripts/deploy/provision_diamond_infra.sh
```

Manual code update (per function):
```bash
export DOCKER_DEFAULT_PLATFORM=linux/arm64

aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin \
  YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com

docker buildx build --platform linux/arm64 --provenance=false -t petadex-search .

docker tag petadex-search:latest \
  YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/petadex-mmseq2-search:latest
docker push YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/petadex-mmseq2-search:latest

# Re-point each function at the new image (container Lambdas pin an immutable digest)
for FN in petadex-diamond-orchestrator petadex-diamond-worker petadex-diamond-aggregator petadex-diamond-align; do
  aws lambda update-function-code --function-name "$FN" \
    --image-uri YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/petadex-mmseq2-search:latest
done
```

> **The CI deploy role can't update the state machine yet**: the step skips with a notice, so apply ASL changes by hand (`aws stepfunctions update-state-machine`; updates are eventually consistent). Container Lambdas pin an image digest; after a deploy, check that `ResolvedImageUri` advanced.

## Cleanup & lifecycle

Each search writes an immutable, `jobId`-addressed result tree under `results/{sessionId}/{jobId}/`, and `results/{sessionId}.index` is overwritten to point at the newest `jobId`. Nothing garbage-collects the previous `jobId` trees, and version-aware re-keying (above) deliberately orphans every prior `sessionId`, so without reaping, the bulky per-shard `parts/*.tsv` + `*.meta.json` and `timing.json` accumulate indefinitely.

These objects are **write-once-then-dead** the moment `{jobId}.json` exists: the web app only ever reads `{sessionId}.index → {jobId}.json` and never re-reads `parts/` or `timing.json`. So they are reaped by an S3 lifecycle rule, scoped by **object tag** rather than prefix (a prefix filter can't isolate `…/parts/…` from the sibling `{jobId}.json`, which share a key root):

- The worker tags each `parts/*.tsv` and `*.meta.json`, and the aggregator tags `timing.json`, with **`reapable=true`** at `put_object` time. The authoritative `{jobId}.json` and `{sessionId}.index` are left **untagged**, so the rule can *never* expire a live result. Safety is structural, not dependent on the rule's correctness.
- A lifecycle rule filtering `Tag {reapable=true}` with `Expiration {Days: 14}` deletes them. 14 days comfortably exceeds any in-flight poll (so a still-running job is never reaped) while bounding orphan accumulation; parts are recomputable on a cache miss, so reaping slightly early costs a re-run, not data.

> Tagging on `put_object` requires **`s3:PutObjectTagging`** in addition to `s3:PutObject` (granted in [infra/iam/worker-policy.json](iam/worker-policy.json) and [aggregator-policy.json](iam/aggregator-policy.json)). The IAM grant must be live **before** the tagging image deploys: the worker's `parts/*.tsv` write is on the fail-fast path, so a missing tagging permission would 403 and fail every search.
