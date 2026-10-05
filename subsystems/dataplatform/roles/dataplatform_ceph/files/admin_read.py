#!/usr/bin/env python3
"""The admin role's read access to every bucket on Ceph, and the bucket index
of the S3 browser (make dataplatform-ceph, tasks/admin_read.yml).

Reads a JSON document on stdin (keys included, so never on the command line):
  {endpoint, region, account, role_arn, index_bucket, index_key,
   root: {access_key, secret_key},              # the account's root: writes the index
   buckets: [{name, owner, objects, size, owner_keys: {access_key, secret_key} | null}]}

- Every bucket not owned by the account gets, in its bucket policy, the
  statement SID: the admin role may list and read it (nothing else). Other
  statements of the policy are kept; the statement is added again if someone
  removed it. The owner's key sets it (only a bucket's owner may). An STS
  session of the admin role then reads the bucket: the role's own policy
  allows reading everywhere, the bucket's policy allows it in (checked on
  this Ceph: without the statement, AccessDenied).
- The index (every bucket, its owner, objects and size) goes to
  s3://<index_bucket>/<index_key>: an STS session lists only its account's
  buckets, the browser shows the others from here.
Prints a JSON summary, without keys. Needs boto3 (requirements.txt).
"""
import json
import sys
import time

import boto3
import botocore

SID = "DataplatformAdminsRead"


def client(doc: dict, keys: dict):
    return boto3.client("s3", endpoint_url=doc["endpoint"], region_name=doc["region"],
                        aws_access_key_id=keys["access_key"], aws_secret_access_key=keys["secret_key"])


def statement(doc: dict, bucket: str) -> dict:
    return {"Sid": SID, "Effect": "Allow", "Principal": {"AWS": [doc["role_arn"]]},
            "Action": ["s3:ListBucket", "s3:GetObject", "s3:GetBucketLocation"],
            "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]}


def grant(doc: dict, b: dict) -> str:
    s3 = client(doc, b["owner_keys"])
    try:
        policy = json.loads(s3.get_bucket_policy(Bucket=b["name"])["Policy"])
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("NoSuchBucketPolicy", "NoSuchKey", "404"):
            raise
        policy = {"Version": "2012-10-17", "Statement": []}
    stmts = policy.get("Statement", [])
    stmts = stmts if isinstance(stmts, list) else [stmts]
    want = statement(doc, b["name"])
    if [s for s in stmts if s.get("Sid") == SID] == [want]:
        return "kept"
    policy["Statement"] = [s for s in stmts if s.get("Sid") != SID] + [want]
    s3.put_bucket_policy(Bucket=b["name"], Policy=json.dumps(policy))
    return "granted"


def main() -> None:
    doc = json.load(sys.stdin)
    summary = {"granted": [], "kept": [], "skipped": [], "index": ""}
    index = []
    for b in sorted(doc["buckets"], key=lambda x: x["name"]):
        entry = {"name": b["name"], "owner": b["owner"], "objects": b.get("objects", 0), "size": b.get("size", 0)}
        if b["owner"] == doc["account"]:
            entry["owner"] = f"{b['owner']} (the platform's account)"
        elif "/" in b["name"] or "$" in b["owner"]:
            entry["note"] = "tenant bucket: not readable by the admin role"
            summary["skipped"].append(f"{b['name']}: tenant")
        elif not b.get("owner_keys"):
            entry["note"] = "owner without an S3 key: not readable by the admin role"
            summary["skipped"].append(f"{b['name']}: owner {b['owner']} has no key")
        else:
            try:
                summary[grant(doc, b)].append(b["name"])
            except botocore.exceptions.ClientError as e:
                entry["note"] = "the read grant failed: " + e.response["Error"]["Code"]
                summary["skipped"].append(f"{b['name']}: {e.response['Error']['Code']}")
        index.append(entry)
    body = json.dumps({"generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "buckets": index}, indent=1)
    client(doc, doc["root"]).put_object(Bucket=doc["index_bucket"], Key=doc["index_key"], Body=body.encode(),
                                        ContentType="application/json")
    summary["index"] = f"s3://{doc['index_bucket']}/{doc['index_key']}: {len(index)} buckets"
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
