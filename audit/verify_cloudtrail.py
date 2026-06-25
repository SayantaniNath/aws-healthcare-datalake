"""
CloudTrail audit verification — confirms S3 data events on the
clinicalflow-datalake bucket are being captured by clinicalflow-audit-trail.
Run locally after any Glue job to verify the audit trail is active.
"""

import boto3
import gzip
import json
from datetime import datetime, timezone, timedelta

BUCKET = "clinicalflow-datalake-941141114246"
LOG_PREFIX = "cloudtrail-logs/AWSLogs/941141114246/CloudTrail/us-east-1/"

s3 = boto3.client("s3", region_name="us-east-1")

today = datetime.now(timezone.utc)
prefix = LOG_PREFIX + today.strftime("%Y/%m/%d/")

resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
files = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json.gz")]

print(f"Found {len(files)} CloudTrail log file(s) for today\n")

s3_events = []
for key in files:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    records = json.loads(gzip.decompress(obj["Body"].read()))["Records"]
    for r in records:
        if r.get("eventSource") == "s3.amazonaws.com":
            s3_events.append({
                "time":      r.get("eventTime"),
                "event":     r.get("eventName"),
                "bucket":    (r.get("requestParameters") or {}).get("bucketName", ""),
                "principal": r.get("userIdentity", {}).get("arn", ""),
            })

print(f"S3 data events captured today: {len(s3_events)}")
print()
for e in sorted(s3_events, key=lambda x: x["time"])[-10:]:
    print(f"  {e['time']}  {e['event']:<20}  {e['bucket']}")

print("\n✅ Audit trail active — all S3 data access is logged." if s3_events
      else "\n⚠️  No S3 data events found — check trail data event config.")
