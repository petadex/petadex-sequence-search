# Tests

Plain scripts, not pytest. Run each with the project venv (host Python has no `boto3`):

```bash
.venv/bin/python tests/test_admission.py
```

| Test | Covers |
|---|---|
| [test_admission.py](test_admission.py) | Public-search limits and in-flight dedup in the orchestrator |
| [test_align.py](test_align.py) | Alignment + tree, the aggregator's background start and the `align` action (real FAMSA/FastTree if on PATH) |
| [test_cli.py](test_cli.py) | Local CLI and the part parser (end-to-end search only if `diamond` is on PATH) |
| [test_export.py](test_export.py) | Uncapped export: worker parts, k-way merge, TSV + FASTA output |
| [test_metadata.py](test_metadata.py) | Metadata-store enrichment, summary and `samples` lookups |
| [test_result_depth.py](test_result_depth.py) | Fixed `RESULT_DEPTH` searching and display-count subsetting |
| [test_shard_cache.py](test_shard_cache.py) | `/tmp` shard cache keyed by full S3 key (no cross-version reuse) |

**Fixtures** ([fixtures/](fixtures/)): a real 1.5.0 ISPETase result (500 hits), a single-id `samples` response and a batch `samples` response for that result's Logan hits.

Live checks against the deployed stack are in [scripts/deploy/](../scripts/deploy/) (`smoke_test.sh`, `validate_export.py`, `validate_pinned_version.py`).
