#!/bin/bash
# IAM setup for ClinicalFlow AWS Healthcare Data Lakehouse
# Run once from any machine with AWS CLI configured as an admin user.
# All resources are tagged with Project=clinicalflow for cost tracking.

set -e

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="clinicalflow-datalake-${ACCOUNT_ID}"
KMS_KEY_ARN=$(aws kms describe-key --key-id alias/clinicalflow-cmk --query KeyMetadata.Arn --output text)

echo "Account: ${ACCOUNT_ID}"
echo "Bucket:  ${BUCKET}"
echo "KMS key: ${KMS_KEY_ARN}"
echo ""

# ── 1. Glue service role ───────────────────────────────────────────────────────
# Used by Glue crawlers and ETL jobs to read/write S3 and access the Glue catalog.

echo "Creating AWSGlueServiceRole-clinicalflow..."
aws iam create-role \
  --role-name AWSGlueServiceRole-clinicalflow \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "glue.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }' \
  --tags Key=Project,Value=clinicalflow 2>/dev/null || echo "  (already exists)"

# AWS managed policy — grants Glue catalog access, CloudWatch logs, basic networking
aws iam attach-role-policy \
  --role-name AWSGlueServiceRole-clinicalflow \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# S3 data access — read raw, write silver/quarantine/gold, delete for overwrite
aws iam put-role-policy \
  --role-name AWSGlueServiceRole-clinicalflow \
  --policy-name S3DataLakeAccess \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\", \"s3:ListBucket\"],
      \"Resource\": [
        \"arn:aws:s3:::${BUCKET}\",
        \"arn:aws:s3:::${BUCKET}/*\"
      ]
    }]
  }"

# KMS — decrypt to read SSE-KMS objects, GenerateDataKey to write new ones
aws iam put-role-policy \
  --role-name AWSGlueServiceRole-clinicalflow \
  --policy-name KMSAccess \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Action\": [\"kms:Decrypt\", \"kms:GenerateDataKey\"],
      \"Resource\": \"${KMS_KEY_ARN}\"
    }]
  }"

echo "  AWSGlueServiceRole-clinicalflow ready."
echo ""

# ── 2. EMR Serverless execution role ──────────────────────────────────────────
# Used by EMR Serverless jobs to read/write S3 and query the Glue catalog.

echo "Creating EMRServerlessRole-clinicalflow..."
aws iam create-role \
  --role-name EMRServerlessRole-clinicalflow \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "emr-serverless.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }' \
  --tags Key=Project,Value=clinicalflow 2>/dev/null || echo "  (already exists)"

# S3 full access — reads raw CSVs, reads Iceberg silver, writes gold output
aws iam attach-role-policy \
  --role-name EMRServerlessRole-clinicalflow \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# Glue catalog access — reads table schemas for Iceberg catalog integration
aws iam attach-role-policy \
  --role-name EMRServerlessRole-clinicalflow \
  --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess

# KMS — required because all S3 objects are SSE-KMS encrypted with clinicalflow-cmk
aws iam put-role-policy \
  --role-name EMRServerlessRole-clinicalflow \
  --policy-name KMSDecryptPolicy \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Action\": [\"kms:Decrypt\", \"kms:GenerateDataKey\"],
      \"Resource\": \"${KMS_KEY_ARN}\"
    }]
  }"

echo "  EMRServerlessRole-clinicalflow ready."
echo ""

# ── 3. Lake Formation — register data lake admin ──────────────────────────────
# Adds the current IAM user as a Lake Formation data lake administrator.
# Required before granting column-level permissions on Glue catalog tables.

CURRENT_USER_ARN=$(aws sts get-caller-identity --query Arn --output text)
echo "Registering Lake Formation admin: ${CURRENT_USER_ARN}..."
aws lakeformation put-data-lake-settings \
  --data-lake-settings "{
    \"DataLakeAdmins\": [{\"DataLakePrincipalIdentifier\": \"${CURRENT_USER_ARN}\"}],
    \"CreateDatabaseDefaultPermissions\": [],
    \"CreateTableDefaultPermissions\": []
  }"
echo "  Lake Formation admin registered."
echo ""

echo "IAM setup complete."
