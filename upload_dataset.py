#!/usr/bin/env python3
"""Stream-gzip the original CSV and multipart-upload it to the configured RustFS bucket."""
import hashlib
import json
import os
import subprocess
from pathlib import Path

import boto3
from botocore.client import Config

SOURCE = Path(os.getenv("RUSTFS_DATASET", "/media/eugene/Data1/vacancies_2024.csv"))
BUCKET = os.getenv("RUSTFS_BUCKET", "hh-lake")
KEY = "vacancies_2024.csv.gz"
endpoint = os.getenv("RUSTFS_ENDPOINT", "http://127.0.0.1:9000")
s3 = boto3.client("s3", endpoint_url=endpoint,
    aws_access_key_id=os.getenv("RUSTFS_ACCESS_KEY", "de_student"),
    aws_secret_access_key=os.getenv("RUSTFS_SECRET_KEY", "ROTATED_CREDENTIAL_REVOKED"),
    region_name="us-east-1", config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))

if not SOURCE.is_file():
    raise SystemExit(f"CSV not found: {SOURCE}")
try:
    s3.head_bucket(Bucket=BUCKET)
except Exception:
    s3.create_bucket(Bucket=BUCKET)

upload_id = s3.create_multipart_upload(Bucket=BUCKET, Key=KEY, ContentType="application/gzip")["UploadId"]
proc = subprocess.Popen(["gzip", "-c", "-1", str(SOURCE)], stdout=subprocess.PIPE)
digest, total, parts, number = hashlib.sha256(), 0, [], 1
try:
    while True:
        block = proc.stdout.read(64 * 1024 * 1024)
        if not block:
            break
        digest.update(block)
        total += len(block)
        etag = s3.upload_part(Bucket=BUCKET, Key=KEY, UploadId=upload_id, PartNumber=number, Body=block)["ETag"]
        parts.append({"ETag": etag, "PartNumber": number})
        print(f"uploaded_part={number} bytes={total}", flush=True)
        number += 1
    if proc.wait() != 0:
        raise RuntimeError("gzip process failed")
    s3.complete_multipart_upload(Bucket=BUCKET, Key=KEY, UploadId=upload_id, MultipartUpload={"Parts": parts})
except BaseException:
    s3.abort_multipart_upload(Bucket=BUCKET, Key=KEY, UploadId=upload_id)
    proc.kill()
    raise
print(json.dumps({"status": "OK", "bucket": BUCKET, "key": KEY, "compressed_bytes": total,
    "compressed_sha256": digest.hexdigest(), "source_bytes": SOURCE.stat().st_size}, indent=2))
