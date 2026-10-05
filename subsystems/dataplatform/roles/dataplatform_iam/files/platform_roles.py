#!/usr/bin/env python3
"""The platform's own STS roles in its RGW account (make dataplatform-iam):

- grp-<group> for every admin group (access.yaml admins.groups): every
  permission on the bucket, and read on every bucket of Ceph (those outside
  the account let the role in through their own policy: admin_read.yml);
- usr-<provisioner> (access.yaml metacatalog.provisioner): rw on the prefix,
  where it creates every schema and table.

Trust: the Keycloak group's members or the user (AssumeRoleWithWebIdentity),
and Trino on behalf of an authenticated user (AssumeRole). Every other role
of the account is Meta Catalog's (provisioning/lib-iam.sh), which may not
touch these. Compare-and-set; --check prints what would change. Prints one
JSON line: {"changed": bool, "actions": [...]}. Keys from the environment
(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY: the account's root user).
"""
import argparse
import json
import os
import sys

import boto3
import botocore


def canon(v):
    """JSON with the order of every list ignored."""
    if isinstance(v, dict):
        return {k: canon(x) for k, x in v.items()}
    if isinstance(v, list):
        return sorted((canon(x) for x in v), key=lambda x: json.dumps(x, sort_keys=True))
    return v


def doc(v):
    return json.loads(v) if isinstance(v, str) else v


def trust(a, kind: str, name: str) -> dict:
    key = f"{a.idp}:{'preferred_username' if kind == 'user' else 'groups'}"
    return {"Version": "2012-10-17", "Statement": [
        {"Sid": "TheKeycloakUser" if kind == "user" else "MembersOfTheKeycloakGroup", "Effect": "Allow",
         "Principal": {"Federated": [f"arn:aws:iam::{a.account_id}:oidc-provider/{a.idp}"]},
         "Action": ["sts:AssumeRoleWithWebIdentity"], "Condition": {"StringEquals": {key: name}}},
        {"Sid": "TrinoOnBehalfOfAuthenticatedUsers", "Effect": "Allow",
         "Principal": {"AWS": [f"arn:aws:iam::{a.account_id}:user/{a.trino_user}"]}, "Action": ["sts:AssumeRole"]}]}


def admin_policy(a) -> dict:
    b = f"arn:aws:s3:::{a.bucket}"
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:*"], "Resource": [b, f"{b}/*"]},
        {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetObject", "s3:GetBucketLocation", "s3:ListAllMyBuckets"],
         "Resource": ["arn:aws:s3:::*", "arn:aws:s3:::*/*"]}]}


def rw_policy(a, path: str) -> dict:
    b = f"arn:aws:s3:::{a.bucket}"
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetBucketLocation"], "Resource": [b]},
        {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [b],
         "Condition": {"StringLike": {"s3:prefix": [path, f"{path}/*"]}}},
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload",
                                       "s3:ListMultipartUploadParts"], "Resource": [f"{b}/{path}/*"]}]}


def main() -> int:
    ap = argparse.ArgumentParser()
    for opt in ("--endpoint", "--region", "--account-id", "--idp", "--trino-user", "--bucket", "--prefix", "--provisioner"):
        ap.add_argument(opt, required=True)
    ap.add_argument("--max-session", type=int, required=True)
    ap.add_argument("--admin-group", action="append", default=[])
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    iam = boto3.client("iam", endpoint_url=a.endpoint, region_name=a.region)
    want = {f"grp-{g}": (trust(a, "group", g), admin_policy(a)) for g in a.admin_group}
    want[f"usr-{a.provisioner}"] = (trust(a, "user", a.provisioner), rw_policy(a, a.prefix))
    actions = []
    for role, (tr, pol) in sorted(want.items()):
        try:
            cur = iam.get_role(RoleName=role)["Role"]
        except botocore.exceptions.ClientError as e:
            if "NoSuchEntity" not in str(e):
                raise
            cur = None
        if cur is None:
            actions.append(f"create role {role}")
            if not a.check:
                iam.create_role(RoleName=role, Path="/", MaxSessionDuration=a.max_session,
                                AssumeRolePolicyDocument=json.dumps(tr))
        elif canon(doc(cur.get("AssumeRolePolicyDocument"))) != canon(tr):
            actions.append(f"update the trust of {role}")
            if not a.check:
                iam.update_assume_role_policy(RoleName=role, PolicyDocument=json.dumps(tr))
        try:
            have = doc(iam.get_role_policy(RoleName=role, PolicyName="access")["PolicyDocument"]) if cur else None
        except botocore.exceptions.ClientError as e:
            if "NoSuchEntity" not in str(e):
                raise
            have = None
        if have is None or canon(have) != canon(pol):
            actions.append(f"put the policy of {role}")
            if not a.check:
                iam.put_role_policy(RoleName=role, PolicyName="access", PolicyDocument=json.dumps(pol))
    print(json.dumps({"changed": bool(actions), "actions": actions}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
