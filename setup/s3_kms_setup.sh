#!/bin/bash
# S3 + KMS setup for ClinicalFlow AWS Healthcare Data Lakehouse
# Run once before iam_setup.sh. Creates the KMS key, S3 bucket, and CloudTrail trail.

set -e

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="clinicalflow-datalake-${ACCOUNT_ID}"
REGION="us-east-1"

echo "Account: ${ACCOUNT_ID}"
echo "Bucket:  ${BUCKET}"
echo "Region:  ${REGION}"
echo ""

# ── 1. KMS customer-managed key ───────────────────────────────────────────────
# SSE-KMS encrypts every S3 object at rest. Using a CMK (not AWS-managed key)
# gives full control: key rotation, access policy, CloudTrail KMS events.

echo "Creating KMS CMK..."
KEY_ID=$(aws kms create-key \
  --description "ClinicalFlow data lake encryption key" \
  --tags TagKey=Project,TagValue=clinicalflow \
  --query KeyMetadata.KeyId \
  --output text)

aws kms create-alias \
  --alias-name alias/clinicalflow-cmk \
  --target-key-id "${KEY_ID}"

echo "  KMS key created: ${KEY_ID} (alias/clinicalflow-cmk)"
echo ""

# ── 2. S3 bucket ──────────────────────────────────────────────────────────────
# Block all public access — healthcare data must never be public.
# Versioning ON — supports Iceberg snapshot recovery and S3 Object Lock.
# SSE-KMS default encryption — all new objects encrypted with clinicalflow-cmk.

echo "Creating S3 bucket: ${BUCKET}..."
aws s3api create-bucket \
  --bucket "${BUCKET}" \
  --region "${REGION}"

# Block all public access
aws s3api put-public-access-block \
  --bucket "${BUCKET}" \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Enable versioning
aws s3api put-bucket-versioning \
  --bucket "${BUCKET}" \
  --versioning-configuration Status=Enabled

# Default SSE-KMS encryption with clinicalflow-cmk
KMS_KEY_ARN=$(aws kms describe-key --key-id alias/clinicalflow-cmk --query KeyMetadata.Arn --output text)
aws s3api put-bucket-encryption \
  --bucket "${BUCKET}" \
  --server-side-encryption-configuration "{
    \"Rules\": [{
      \"ApplyServerSideEncryptionByDefault\": {
        \"SSEAlgorithm\": \"aws:kms\",
        \"KMSMasterKeyID\": \"${KMS_KEY_ARN}\"
      },
      \"BucketKeyEnabled\": true
    }]
  }"

# Create S3 folder structure (empty objects as placeholders)
for prefix in raw/synthea/csv/ silver/patients/ quarantine/patients/ gold/ cloudtrail-logs/ scripts/; do
  aws s3api put-object --bucket "${BUCKET}" --key "${prefix}" > /dev/null
done

echo "  S3 bucket ready with SSE-KMS encryption and folder structure."
echo ""

# ── 3. CloudTrail — data event logging ────────────────────────────────────────
# Logs every S3 object-level operation (GetObject, PutObject, DeleteObject)
# on the bucket. Required for HIPAA audit trail — proves who accessed what data.
# Management events (API calls) are free; data events cost $0.10/100K events.

echo "Creating CloudTrail trail: clinicalflow-audit-trail..."
aws cloudtrail create-trail \
  --name clinicalflow-audit-trail \
  --s3-bucket-name "${BUCKET}" \
  --s3-key-prefix cloudtrail-logs \
  --is-multi-region-trail \
  --enable-log-file-validation \
  --kms-key-id alias/clinicalflow-cmk

aws cloudtrail start-logging --name clinicalflow-audit-trail

# Enable S3 data events on the bucket
aws cloudtrail put-event-selectors \
  --trail-name clinicalflow-audit-trail \
  --event-selectors "[{
    \"ReadWriteType\": \"All\",
    \"IncludeManagementEvents\": true,
    \"DataResources\": [{
      \"Type\": \"AWS::S3::Object\",
      \"Values\": [\"arn:aws:s3:::${BUCKET}/\"]
    }]
  }]"

echo "  CloudTrail trail active with S3 data events enabled."
echo ""

echo "S3 + KMS setup complete. Run iam_setup.sh next."
