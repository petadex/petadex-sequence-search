# PETadex Sequence Search

**Note that this is done, and is not ready as a standalone tool!!**

**Fast protein sequence similarity search against 217M+ plastic-degrading enzyme sequences**

MMseqs2-powered search engine available as both a standalone CLI tool and AWS Lambda function.

---

## 🚀 Quick Start (Standalone CLI)

```bash
# Run with Docker (no installation required)
docker run --rm \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  -e AWS_DEFAULT_REGION=us-east-1 \
  yourusername/petadex-search \
  "MKLLIVLLAACLAVFAAAEPQIAVVPPRQCPVVAASVAVVAASVAAAVV" 10
```

**First run downloads ~5GB database (cached for subsequent searches)**

---

## 📋 Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [CLI Mode](#cli-mode)
  - [Lambda Deployment](#lambda-deployment)
- [Database Updates](#database-updates)
- [Architecture](#architecture)
- [Development](#development)

---

## ✨ Features

- ⚡ **Ultra-fast**: MMseqs2-powered sequence search
- 🧬 **Massive database**: 217M+ enzyme sequences from PETadex
- 🔄 **Version-controlled**: Reproducible searches via manifest system
- 💾 **Smart caching**: Database cached after first download
- 🐳 **Containerized**: Docker image for consistent environments
- ☁️ **Scalable**: Deploy as serverless Lambda or standalone CLI

---

## 📦 Installation

### Option 1: Docker (Recommended)

```bash
docker pull yourusername/petadex-search:latest
```

### Option 2: Build from Source

```bash
git clone https://github.com/yourusername/petadex-search.git
cd petadex-search
docker build -t petadex-search .
```

---

## 🔧 Usage

### CLI Mode

**Basic search:**
```bash
docker run --rm \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  petadex-search \
  "MKLLIVLLAACLAVFAAAEPQIAVV" 10
```

**Save results to file:**
```bash
docker run --rm \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  petadex-search \
  "MKLLIVLLAACLAVFAAAEPQIAVV" 50 > results.json
```

**Mount local directory for input files:**
```bash
docker run --rm \
  -v $(pwd):/data \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  petadex-search \
  "$(cat /data/sequence.txt)" 100
```

### Lambda Deployment

#### Prerequisites
- AWS account with ECR and Lambda access
- Docker with BuildKit support
- AWS CLI configured

#### 1. Create ECR Repository
```bash
aws ecr create-repository \
  --repository-name petadex-search \
  --region us-east-1
```

#### 2. Build and Push Image
```bash
# Set platform for Lambda compatibility
export DOCKER_DEFAULT_PLATFORM=linux/arm64

# Login to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin \
  YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com

# Build with Lambda-friendly flags
docker buildx build --platform linux/arm64 --provenance=false -t petadex-search .

# Tag and push
docker tag petadex-search:latest \
  YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/petadex-search:latest
docker push YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/petadex-search:latest
```

#### 3. Create Lambda Function
```bash
aws lambda create-function \
  --function-name petadex-search \
  --package-type Image \
  --code ImageUri=YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/petadex-search:latest \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role \
  --memory-size 10240 \
  --timeout 900 \
  --architectures arm64
```

#### 4. Invoke Lambda
```bash
aws lambda invoke \
  --function-name petadex-search \
  --payload '{"sequence":"MKLLIVLLAACLAVFAAAEPQIAVV","max_results":10}' \
  response.json
```

**Lambda Response Format:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "s3_key": "results/550e8400-e29b-41d4-a716-446655440000.json"
}
```

Retrieve results from S3:
```bash
aws s3 cp s3://petadex/results/550e8400-e29b-41d4-a716-446655440000.json results.json
```

---

## 🗄️ Database Updates

The system uses a **version-controlled manifest architecture** for reproducible searches.

### How It Works

1. **Extract** - Queries PostgreSQL `enzyme_fastaa` table
2. **Build** - Creates MMseqs2 searchable index with timestamp
3. **Upload** - Pushes to `s3://petadex/mmseqs2/{version}/`
4. **Update LATEST** - Points manifest to new version
5. **Auto-Update** - Lambda picks up new version on next cold start

**No redeployment needed!**

### Quick Start

```bash
# 1. Set up environment
cp scripts/.env.example scripts/.env
# Edit .env with database credentials

# 2. Install dependencies
pip install -r scripts/requirements.txt
brew install mmseqs2  # or apt-get install mmseqs2

# 3. Update database
source scripts/.env
./scripts/update_sequence_index.py
```

### Automated Updates

Use GitHub Actions to update database on schedule:

```yaml
# .github/workflows/update-database.yml
name: Update Database
on:
  schedule:
    - cron: '0 2 * * 0'  # Weekly on Sunday 2am
  workflow_dispatch:

jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Update database
        env:
          DB_HOST: ${{ secrets.DB_HOST }}
          DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
        run: ./scripts/update_sequence_index.py
```

---

## 🏗️ Architecture

### Version-Controlled Genomic Repository

> *"The PETadex architecture implements a version-controlled genomic repository using a manifest-based approach. A single `LATEST` pointer file enables instantaneous, global database updates across all compute instances. This design ensures **reproducibility** - researchers can reference specific database versions in publications, and results remain verifiable regardless of future database updates."*

### Component Breakdown

```
┌─────────────────┐
│   S3 Bucket     │
│    petadex      │
└────────┬────────┘
         │
         ├── mmseqs2/
         │   ├── LATEST (pointer file)
         │   ├── enzyme_fastaa_20260130_143527/
         │   │   ├── enzyme_fastaa_mmseqs_20260130_143527.index
         │   │   ├── enzyme_fastaa_mmseqs_20260130_143527.dbtype
         │   │   └── ...
         │   └── enzyme_fastaa_20260125_092341/ (previous version)
         │
         └── results/
             └── {job_id}.json

┌──────────────────┐      ┌──────────────────┐
│  Lambda Function │ ───> │  CLI Container   │
│  (Serverless)    │      │  (Standalone)    │
└──────────────────┘      └──────────────────┘
         │                         │
         └─────────┬───────────────┘
                   │
                   ▼
           ┌──────────────┐
           │  MMseqs2     │
           │  Search      │
           └──────────────┘
```

### Technology Stack

- **Search Engine**: MMseqs2 (ultra-fast sequence search)
- **Runtime**: Python 3.11
- **Container**: AWS Lambda ARM64 / Docker
- **Storage**: Amazon S3
- **Database**: 217M+ sequences from PETadex PostgreSQL

---

## 🛠️ Development

### Local Testing

```bash
# Build image
docker build -t petadex-search .

# Test Lambda mode locally
docker run -p 9000:8080 \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  petadex-search

# Invoke (in another terminal)
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{
    "sequence": "MKLLIVLLALAVAALHAQQGVGAPVP",
    "max_results": 10
  }'

# Test CLI mode
docker run --rm \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  --entrypoint python3 \
  petadex-search cli.py "MKLLIVLLALAVAALHAQQGVGAPVP" 10
```

### Project Structure

```
petadex-search/
├── lambda_function.py      # Lambda handler
├── cli.py                  # CLI entrypoint
├── Dockerfile              # Multi-mode container
├── requirements.txt        # Python dependencies
├── scripts/
│   ├── update_sequence_index.py
│   ├── setup_s3_access.sh
│   └── README.md
└── .github/
    └── workflows/
        ├── docker-publish.yml
        └── update-database.yml.example
```

### Input Validation

- **Valid amino acids**: `ACDEFGHIKLMNPQRSTVWY`
- **Minimum length**: 10 amino acids
- **Maximum length**: 10,000 amino acids

### Output Format

```json
{
  "query_length": 50,
  "num_results": 10,
  "results": [
    {
      "target_id": "enzyme_12345|genbank_ABC123|orf_0",
      "query_start": 1,
      "query_end": 50,
      "target_start": 1,
      "target_end": 50,
      "alignment_length": 50,
      "percent_identity": 98.5,
      "evalue": 1.2e-25,
      "bitscore": 95.3
    }
  ]
}
```

---

## 📊 Performance

- **Search time**: ~10-30 seconds (first run includes database download)
- **Database size**: ~5GB (cached in `/tmp`)
- **Memory usage**: 3-10GB RAM
- **Cache persistence**: Lambda `/tmp` persists across warm invocations

---

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

## 📄 License

[Your License Here]

---

## 🙏 Acknowledgments

- MMseqs2: Steinegger & Söding (2017)
- PETadex: Plastic-degrading enzyme database
- AWS Lambda: Serverless compute platform

---

## 📚 Citation

If you use PETadex Search in your research, please cite:

```bibtex
@software{petadex_search,
  title = {PETadex Sequence Search},
  author = {Your Name},
  year = {2026},
  url = {https://github.com/yourusername/petadex-search}
}
```
