# PETadex Sequence Search

> **Note: This is an internal component, not a standalone tool. It is designed to be invoked as an AWS Lambda function by the PETadex web application.**

MMseqs2-powered protein sequence similarity search against 217M+ plastic-degrading enzyme sequences from the PETadex database. Packaged as a Docker container that runs as either an AWS Lambda function or a standalone CLI.

---

## How It Works

1. On invocation, downloads the MMseqs2 sequence index from S3 (`s3://petadex/mmseqs2/`) — cached in `/tmp` across warm Lambda invocations
2. Runs `mmseqs easy-search` against the index
3. Uploads results as JSON to `s3://petadex/results/{sessionId}/{job_id}.json`
4. Returns the `job_id` to the caller; the web app fetches results from S3

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

- **Search**: MMseqs2 `easy-search`
- **Runtime**: Python 3.11 on AWS Lambda ARM64 (Graviton)
- **Storage**: Amazon S3 (`petadex` bucket)
- **CI/CD**: GitHub Actions → ECR → Lambda
- **Database source**: PETadex PostgreSQL (`enzyme_fastaa` table), 217M+ sequences
