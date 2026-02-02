# S3 Bucket Access Setup

This guide explains how to provide S3 access for updating the PETadex sequence database without sharing your main AWS credentials.

## Quick Start

### Option 1: IAM User with Limited Permissions (Recommended)

Create an IAM user that can ONLY upload to the S3 bucket:

```bash
# Run the setup script
chmod +x scripts/setup_s3_access.sh
./scripts/setup_s3_access.sh
```

This creates:
- IAM user: `petadex-db-uploader`
- Limited permissions: Only S3 upload/download/list on `petadex-sequence-db`
- Access keys saved to: `petadex-s3-credentials.txt`

Then use the credentials:

```bash
# Set environment variables
export AWS_ACCESS_KEY_ID="AKIAxxxxxxxxx"
export AWS_SECRET_ACCESS_KEY="xxxxxxxxxxxxxxxx"

# Run database update
python scripts/update_sequence_index.py
```

### Option 2: Manual Setup

If you prefer to set up manually:

```bash
# 1. Create IAM user
aws iam create-user --user-name petadex-db-uploader

# 2. Create policy
cat > /tmp/s3-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::petadex-sequence-db",
        "arn:aws:s3:::petadex-sequence-db/*"
      ]
    }
  ]
}
EOF

aws iam create-policy \
  --policy-name PetadexS3Upload \
  --policy-document file:///tmp/s3-policy.json

# 3. Attach policy (replace YOUR_ACCOUNT_ID)
aws iam attach-user-policy \
  --user-name petadex-db-uploader \
  --policy-arn arn:aws:iam::YOUR_ACCOUNT_ID:policy/PetadexS3Upload

# 4. Create access keys
aws iam create-access-key --user-name petadex-db-uploader
```

## Security Options

### Add IP Restriction (More Secure)

Restrict access to a specific IP address:

```bash
cat > /tmp/bucket-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::YOUR_ACCOUNT_ID:user/petadex-db-uploader"
      },
      "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::petadex-sequence-db",
        "arn:aws:s3:::petadex-sequence-db/*"
      ],
      "Condition": {
        "IpAddress": {
          "aws:SourceIp": "YOUR_IP_ADDRESS/32"
        }
      }
    }
  ]
}
EOF

aws s3api put-bucket-policy \
  --bucket petadex-sequence-db \
  --policy file:///tmp/bucket-policy.json
```

### GitHub Actions with OIDC (Best for CI/CD)

No credentials needed - GitHub authenticates directly with AWS.

#### 1. Create OIDC Provider

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

#### 2. Create IAM Role with Trust Policy

```bash
cat > /tmp/github-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::YOUR_ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:YOUR_GITHUB_ORG/petadex:*"
        }
      }
    }
  ]
}
EOF

aws iam create-role \
  --role-name GitHubActionsS3Upload \
  --assume-role-policy-document file:///tmp/github-trust-policy.json

# Attach S3 permissions
aws iam attach-role-policy \
  --role-name GitHubActionsS3Upload \
  --policy-arn arn:aws:iam::YOUR_ACCOUNT_ID:policy/PetadexS3Upload
```

#### 3. GitHub Actions Workflow

Create `.github/workflows/update-database.yml`:

```yaml
name: Update Sequence Database

on:
  workflow_dispatch:  # Manual trigger
  schedule:
    - cron: '0 0 * * 0'  # Weekly on Sunday

permissions:
  id-token: write   # Required for OIDC
  contents: read

jobs:
  update-database:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v2
        with:
          role-to-assume: arn:aws:iam::YOUR_ACCOUNT_ID:role/GitHubActionsS3Upload
          aws-region: us-east-1

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install boto3 psycopg2-binary

      - name: Install MMseqs2
        run: |
          wget https://mmseqs.com/latest/mmseqs-linux-avx2.tar.gz
          tar xvfz mmseqs-linux-avx2.tar.gz
          export PATH=$(pwd)/mmseqs/bin:$PATH

      - name: Update database
        env:
          DB_HOST: ${{ secrets.DB_HOST }}
          DB_NAME: ${{ secrets.DB_NAME }}
          DB_USER: ${{ secrets.DB_USER }}
          DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
        run: |
          python scripts/update_sequence_index.py
```

## Database Update Script

### Requirements

```bash
pip install boto3 psycopg2-binary
```

Also requires MMseqs2 installed:
```bash
# macOS
brew install mmseqs2

# Linux
wget https://mmseqs.com/latest/mmseqs-linux-avx2.tar.gz
tar xvfz mmseqs-linux-avx2.tar.gz
export PATH=$(pwd)/mmseqs/bin:$PATH
```

### Environment Variables

```bash
# Database connection
export DB_HOST="localhost"
export DB_NAME="petadex"
export DB_USER="postgres"
export DB_PASSWORD="your-password"
export DB_PORT="5432"

# AWS credentials (if not using IAM role)
export AWS_ACCESS_KEY_ID="AKIAxxxxxxxxx"
export AWS_SECRET_ACCESS_KEY="xxxxxxxxxxxxxxxx"
```

### Usage

```bash
# Build and upload to S3
python scripts/update_sequence_index.py

# Build locally only (for testing)
python scripts/update_sequence_index.py --skip-s3

# Use different bucket
python scripts/update_sequence_index.py --bucket my-custom-bucket
```

### What It Does

1. Extracts sequences from PostgreSQL `enzyme_fastaa` table
2. Builds MMseqs2 database
3. Uploads all database files to S3
4. Creates metadata file with version info
5. Updates `mmseqs2/LATEST` pointer
6. Cleans up temporary files

### Output

```
==========================================================
PETadex Sequence Database Builder
==========================================================
Source: enzyme_fastaa table
Timestamp: 20240115_143022
S3 Bucket: petadex-sequence-db
==========================================================

[1/5] Extracting sequences from PostgreSQL...
  Extracted 1,234,567 sequences... Done!

[2/5] Building MMseqs2 database...
  Running: mmseqs createdb /tmp/enzyme_fastaa_20240115_143022.fasta /tmp/enzyme_fastaa_mmseqs_20240115_143022
  MMseqs2 database created successfully
  Database files: 8 files
    - enzyme_fastaa_mmseqs_20240115_143022: 245.67 MB
    - enzyme_fastaa_mmseqs_20240115_143022.index: 12.34 MB
    ...

[3/5] Uploading to S3...
  Uploading 8 files to s3://petadex-sequence-db/mmseqs2/enzyme_fastaa_20240115_143022/
    Uploading enzyme_fastaa_mmseqs_20240115_143022 (245.67 MB)... ✓
    ...

[4/5] Creating metadata...
  Creating metadata file: mmseqs2/enzyme_fastaa_20240115_143022/metadata.json
  Metadata file created

[5/5] Updating LATEST pointer...
  Updating LATEST pointer to: enzyme_fastaa_20240115_143022
  LATEST pointer updated

[Cleanup] Removing temporary files...
    Removed /tmp/enzyme_fastaa_20240115_143022.fasta
    ...

==========================================================
✓ DATABASE BUILD COMPLETE!
==========================================================
Sequences indexed: 1,234,567
S3 Location: s3://petadex-sequence-db/mmseqs2/enzyme_fastaa_20240115_143022
Database version: enzyme_fastaa_20240115_143022
==========================================================

The Lambda function will use this database on next cold start.
```

## Troubleshooting

### psycopg2 Import Error

```bash
pip install psycopg2-binary
```

### MMseqs2 Not Found

Install MMseqs2:
- macOS: `brew install mmseqs2`
- Linux: Download from https://github.com/soedinglab/MMseqs2

### S3 Upload Permission Denied

Check that:
1. IAM user has correct policy attached
2. Bucket name is correct
3. AWS credentials are set in environment

### Database Connection Error

Check that:
1. PostgreSQL is running
2. Database credentials are correct
3. Network access is allowed (firewall, security groups)

## Security Best Practices

1. **Never commit credentials** - Use environment variables or AWS IAM roles
2. **Use least privilege** - IAM user can only access specific S3 bucket
3. **Rotate credentials** - Periodically create new access keys
4. **Monitor usage** - Check AWS CloudTrail for unexpected access
5. **Use OIDC for CI/CD** - GitHub Actions doesn't need long-lived credentials

## Managing Access Keys

### List existing keys
```bash
aws iam list-access-keys --user-name petadex-db-uploader
```

### Rotate keys
```bash
# Create new key
aws iam create-access-key --user-name petadex-db-uploader

# After confirming new key works, delete old one
aws iam delete-access-key \
  --user-name petadex-db-uploader \
  --access-key-id AKIAOLD_KEY_ID
```

### Revoke access
```bash
# Delete all access keys
aws iam list-access-keys --user-name petadex-db-uploader \
  --query 'AccessKeyMetadata[].AccessKeyId' --output text | \
  xargs -I {} aws iam delete-access-key \
    --user-name petadex-db-uploader \
    --access-key-id {}

# Or delete the entire user
aws iam detach-user-policy \
  --user-name petadex-db-uploader \
  --policy-arn arn:aws:iam::YOUR_ACCOUNT_ID:policy/PetadexS3Upload

aws iam delete-user --user-name petadex-db-uploader
```
