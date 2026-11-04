#!/usr/bin/env python3
"""Read back required S3 objects and compare their SHA-256 digests."""
import hashlib
import json
from pathlib import Path

from pipeline import BUCKET, KEY, OUT, s3client

source = json.loads((OUT / "ingest_verification.json").read_text(encoding="utf-8"))
expected_source_sha = source["object"]["sha256"]
keys = [
    KEY,
    "results/mapreduce_1000.json",
    "results/mapreduce_10000.json",
    "results/mapreduce_100000.json",
    "results/duckdb.json",
    "results/audit.json",
    "results/mapper_failure.json",
    "results/optimization_benchmark.json",
]
s3 = s3client()
manifest = {"bucket": BUCKET, "status": "PASS", "objects": []}
for key in keys:
    response = s3.get_object(Bucket=BUCKET, Key=key)
    digest = hashlib.sha256()
    size = 0
    for block in iter(lambda: response["Body"].read(8 * 1024 * 1024), b""):
        digest.update(block)
        size += len(block)
    actual_sha = digest.hexdigest()
    local_path = OUT / Path(key).name
    expected_sha = expected_source_sha if key == KEY else hashlib.sha256(local_path.read_bytes()).hexdigest()
    item = {"key": key, "bytes": size, "sha256": actual_sha,
        "expected_sha256": expected_sha, "matches": actual_sha == expected_sha}
    manifest["objects"].append(item)
    print(json.dumps(item, ensure_ascii=False))
    if not item["matches"]:
        manifest["status"] = "FAIL"

(OUT / "s3_integrity_manifest.json").write_text(
    json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"status": manifest["status"], "verified_objects": len(keys)}, ensure_ascii=False))
if manifest["status"] != "PASS":
    raise SystemExit(2)
