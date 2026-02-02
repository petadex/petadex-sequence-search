# Database Update Scripts

This directory contains scripts for building and uploading the PETadex sequence database to S3.

## Files

- **[update_sequence_index.py](update_sequence_index.py)** - Main script that extracts sequences from PostgreSQL, builds MMseqs2 database, and uploads to S3
- **[setup_s3_access.sh](setup_s3_access.sh)** - Script to create IAM user with limited S3 permissions
- **[S3_SETUP.md](S3_SETUP.md)** - Detailed documentation for S3 access setup
- **[requirements.txt](requirements.txt)** - Python dependencies

## Quick Start

### 1. Set up S3 Access

```bash
# Run the automated setup script
./scripts/setup_s3_access.sh

# Or see S3_SETUP.md for manual setup and other options
```

### 2. Install Dependencies

```bash
# Python packages
pip install -r scripts/requirements.txt

# MMseqs2 (choose one)
brew install mmseqs2                    # macOS
sudo apt install mmseqs2                # Ubuntu/Debian
# or download from https://github.com/soedinglab/MMseqs2
```

### 3. Set Environment Variables

```bash
# Database connection
export DB_HOST="localhost"
export DB_NAME="petadex"
export DB_USER="postgres"
export DB_PASSWORD="your-password"

# AWS credentials (from step 1)
export AWS_ACCESS_KEY_ID="AKIAxxxxxxxxx"
export AWS_SECRET_ACCESS_KEY="xxxxxxxxxxxxxxxx"
```

### 4. Run Database Update

```bash
# Build and upload to S3
./scripts/update_sequence_index.py

# Or build locally only (for testing)
./scripts/update_sequence_index.py --skip-s3
```

## Usage Examples

### Test Locally First

```bash
# Build database locally without uploading
./scripts/update_sequence_index.py --skip-s3

# Check the generated files
ls -lh /tmp/enzyme_fastaa_mmseqs_*
```

### Upload to S3

```bash
# Build and upload to default bucket (petadex-sequence-db)
./scripts/update_sequence_index.py

# Upload to custom bucket
./scripts/update_sequence_index.py --bucket my-custom-bucket
```

### Use with Docker

```bash
# Build database inside Docker container
docker run --rm \
  -v $(pwd)/scripts:/scripts \
  -e DB_HOST=host.docker.internal \
  -e DB_NAME=petadex \
  -e DB_USER=postgres \
  -e DB_PASSWORD=password \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  soedinglab/mmseqs2 \
  python /scripts/update_sequence_index.py
```

## What Happens

1. **Extract Sequences** - Queries PostgreSQL `enzyme_fastaa` table and creates FASTA file
2. **Build MMseqs2 Database** - Creates searchable MMseqs2 index (~8 files)
3. **Upload to S3** - Uploads all database files to `s3://petadex-sequence-db/mmseqs2/enzyme_fastaa_TIMESTAMP/`
4. **Create Metadata** - Generates `metadata.json` with version info
5. **Update LATEST** - Points `mmseqs2/LATEST` to new version
6. **Cleanup** - Removes temporary files

## Lambda Integration

The Lambda function automatically uses the latest database version:

1. On cold start, Lambda reads `s3://petadex-sequence-db/mmseqs2/LATEST`
2. Downloads database files from the version specified in LATEST
3. Caches database in `/tmp` for subsequent invocations
4. Automatically picks up new versions on next cold start

No Lambda redeployment needed when updating the database!

## Automation

See [S3_SETUP.md](S3_SETUP.md) for GitHub Actions OIDC setup to run updates automatically without managing credentials.

## Troubleshooting

### `psycopg2` Import Error
```bash
pip install psycopg2-binary
```

### `mmseqs` Command Not Found
Install MMseqs2 (see step 2 above)

### S3 Permission Denied
Check AWS credentials are set and IAM policy is attached

### Database Connection Failed
Verify PostgreSQL is running and credentials are correct

## Security

The IAM user created by `setup_s3_access.sh` has minimal permissions:
- ✓ Upload files to `petadex-sequence-db` bucket
- ✓ List files in `petadex-sequence-db` bucket
- ✓ Download files from `petadex-sequence-db` bucket
- ✗ Cannot access other buckets
- ✗ Cannot create Lambda functions
- ✗ Cannot modify IAM policies

See [S3_SETUP.md](S3_SETUP.md) for additional security options like IP restrictions and OIDC.

## Support

For issues or questions, see the main [README.md](../README.md) or open an issue.
