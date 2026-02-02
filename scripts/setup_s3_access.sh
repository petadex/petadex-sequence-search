#!/bin/bash
#
# Setup S3 Access for PETadex Database Updates
#
# This script creates an IAM user with limited permissions to upload
# database files to the petadex-sequence-db S3 bucket.
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "PETadex S3 Access Setup"
echo "=========================================="

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo -e "${RED}Error: AWS CLI not installed${NC}"
    echo "Install from: https://aws.amazon.com/cli/"
    exit 1
fi

# Get AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo -e "${GREEN}AWS Account ID: ${ACCOUNT_ID}${NC}"

# Configuration
IAM_USER="petadex-db-uploader"
POLICY_NAME="PetadexS3Upload"
BUCKET_NAME="petadex-sequence-db"

echo ""
echo "Configuration:"
echo "  IAM User: ${IAM_USER}"
echo "  Policy: ${POLICY_NAME}"
echo "  Bucket: ${BUCKET_NAME}"
echo ""

# Step 1: Create IAM user
echo "=========================================="
echo "Step 1: Creating IAM User"
echo "=========================================="

if aws iam get-user --user-name ${IAM_USER} &> /dev/null; then
    echo -e "${YELLOW}User ${IAM_USER} already exists${NC}"
else
    aws iam create-user --user-name ${IAM_USER}
    echo -e "${GREEN}✓ User created${NC}"
fi

# Step 2: Create IAM policy
echo ""
echo "=========================================="
echo "Step 2: Creating IAM Policy"
echo "=========================================="

cat > /tmp/petadex-s3-upload-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET_NAME}",
        "arn:aws:s3:::${BUCKET_NAME}/*"
      ]
    }
  ]
}
EOF

POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"

if aws iam get-policy --policy-arn ${POLICY_ARN} &> /dev/null; then
    echo -e "${YELLOW}Policy ${POLICY_NAME} already exists${NC}"

    # Update the policy with a new version
    echo "Updating policy with new version..."
    aws iam create-policy-version \
        --policy-arn ${POLICY_ARN} \
        --policy-document file:///tmp/petadex-s3-upload-policy.json \
        --set-as-default
    echo -e "${GREEN}✓ Policy updated${NC}"
else
    aws iam create-policy \
        --policy-name ${POLICY_NAME} \
        --policy-document file:///tmp/petadex-s3-upload-policy.json \
        --description "Allow upload to PETadex S3 sequence database"
    echo -e "${GREEN}✓ Policy created${NC}"
fi

# Step 3: Attach policy to user
echo ""
echo "=========================================="
echo "Step 3: Attaching Policy to User"
echo "=========================================="

aws iam attach-user-policy \
    --user-name ${IAM_USER} \
    --policy-arn ${POLICY_ARN}
echo -e "${GREEN}✓ Policy attached${NC}"

# Step 4: Create access keys
echo ""
echo "=========================================="
echo "Step 4: Creating Access Keys"
echo "=========================================="

# Check if user already has access keys
EXISTING_KEYS=$(aws iam list-access-keys --user-name ${IAM_USER} --query 'AccessKeyMetadata[].AccessKeyId' --output text)

if [ -n "$EXISTING_KEYS" ]; then
    echo -e "${YELLOW}User already has access keys:${NC}"
    echo "$EXISTING_KEYS"
    echo ""
    read -p "Create new access keys? (existing keys will remain active) [y/N]: " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Skipping access key creation"
        exit 0
    fi
fi

# Create access keys
CREDENTIALS=$(aws iam create-access-key --user-name ${IAM_USER} --output json)

ACCESS_KEY_ID=$(echo $CREDENTIALS | jq -r '.AccessKey.AccessKeyId')
SECRET_ACCESS_KEY=$(echo $CREDENTIALS | jq -r '.AccessKey.SecretAccessKey')

# Save credentials to file
CREDS_FILE="petadex-s3-credentials.txt"
cat > $CREDS_FILE << EOF
PETadex S3 Upload Credentials
Created: $(date)

IAM User: ${IAM_USER}
AWS Account: ${ACCOUNT_ID}

Access Key ID: ${ACCESS_KEY_ID}
Secret Access Key: ${SECRET_ACCESS_KEY}

Permissions:
- s3:PutObject on s3://${BUCKET_NAME}/*
- s3:GetObject on s3://${BUCKET_NAME}/*
- s3:ListBucket on s3://${BUCKET_NAME}

Usage:
  export AWS_ACCESS_KEY_ID="${ACCESS_KEY_ID}"
  export AWS_SECRET_ACCESS_KEY="${SECRET_ACCESS_KEY}"
  python scripts/update_sequence_index.py

IMPORTANT: Store these credentials securely!
These credentials CANNOT be retrieved again after this screen is closed.
EOF

echo ""
echo "=========================================="
echo -e "${GREEN}✓ Setup Complete!${NC}"
echo "=========================================="
echo ""
echo "Credentials saved to: ${CREDS_FILE}"
echo ""
echo -e "${YELLOW}IMPORTANT: Store these credentials securely!${NC}"
echo ""
cat $CREDS_FILE
echo ""
echo "=========================================="

# Cleanup temp policy file
rm /tmp/petadex-s3-upload-policy.json

echo ""
echo "Next steps:"
echo "  1. Save the credentials from ${CREDS_FILE}"
echo "  2. Set environment variables:"
echo "     export AWS_ACCESS_KEY_ID=\"${ACCESS_KEY_ID}\""
echo "     export AWS_SECRET_ACCESS_KEY=\"${SECRET_ACCESS_KEY}\""
echo "  3. Run: python scripts/update_sequence_index.py"
echo ""
