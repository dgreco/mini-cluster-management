#!/usr/bin/env python3
"""Idempotently register an OIDC provider with an RGW IAM endpoint (needs boto3).

An existing provider is converged: its client IDs and thumbprints are made to
match the arguments (a realm key rotation changes the thumbprint).
"""
import argparse
import base64
import hashlib
import json
import os
import ssl
import sys
import urllib.request
from urllib.parse import urlparse

try:
    import boto3
    import urllib3
except ImportError:
    sys.exit("boto3 is required on the control node: pip install boto3")


def tls_thumbprint(url: str) -> str:
    """SHA-1 of the IdP's TLS certificate (the old default, kept for -e oidc_thumbprint_source=tls)."""
    u = urlparse(url)
    pem = ssl.get_server_certificate((u.hostname, u.port or 443))
    return hashlib.sha1(ssl.PEM_cert_to_DER_cert(pem)).hexdigest()


def jwks_thumbprints(url: str, verify: bool) -> list:
    """SHA-1 of every signing certificate (x5c) in the IdP's JWKS: what RGW checks by default."""
    ctx = None if verify else ssl._create_unverified_context()
    with urllib.request.urlopen(url.rstrip("/") + "/.well-known/openid-configuration", context=ctx, timeout=15) as r:
        jwks_uri = json.load(r)["jwks_uri"]
    with urllib.request.urlopen(jwks_uri, context=ctx, timeout=15) as r:
        keys = json.load(r)["keys"]
    prints = [hashlib.sha1(base64.b64decode(k["x5c"][0])).hexdigest()
              for k in keys if k.get("use", "sig") == "sig" and k.get("x5c")]
    if not prints:
        sys.exit(f"no signing certificate (x5c) in {jwks_uri}")
    return sorted(set(prints))[:5]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--region", required=True, help="SigV4 region of the IAM calls (RGW does not check it)")
    ap.add_argument("--access-key", default=os.environ.get("AWS_ACCESS_KEY_ID"), help="or AWS_ACCESS_KEY_ID")
    ap.add_argument("--secret-key", default=os.environ.get("AWS_SECRET_ACCESS_KEY"), help="or AWS_SECRET_ACCESS_KEY")
    ap.add_argument("--url", required=True, help="issuer URL, e.g. https://idp/realms/lab")
    ap.add_argument("--client-id", action="append", required=True)
    ap.add_argument("--thumbprint", action="append", help="default: from the IdP's JWKS")
    ap.add_argument("--thumbprint-source", choices=["jwks", "tls"], default="jwks")
    ap.add_argument("--no-verify", action="store_true", help="do not verify the RGW endpoint certificate")
    ap.add_argument("--account-id", default="", help="RGW IAM account the caller belongs to (its ARNs carry it)")
    a = ap.parse_args()
    if not a.access_key or not a.secret_key:
        ap.error("--access-key/--secret-key or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY")

    if a.no_verify:
        urllib3.disable_warnings()
    iam = boto3.client(
        "iam",
        endpoint_url=a.endpoint,
        aws_access_key_id=a.access_key,
        aws_secret_access_key=a.secret_key,
        region_name=a.region,
        verify=not a.no_verify,
    )
    tps = [t.lower().replace(":", "") for t in a.thumbprint] if a.thumbprint else (
        jwks_thumbprints(a.url, verify=True) if a.thumbprint_source == "jwks" else [tls_thumbprint(a.url)])
    clients = sorted(set(a.client_id))
    arn = f"arn:aws:iam::{a.account_id}:oidc-provider/" + a.url.split("://", 1)[-1].rstrip("/")
    existing = [p["Arn"] for p in iam.list_open_id_connect_providers()["OpenIDConnectProviderList"]]
    if arn not in existing:
        iam.create_open_id_connect_provider(Url=a.url, ClientIDList=clients, ThumbprintList=tps)
        print(json.dumps({"changed": True, "arn": arn, "client_ids": clients, "thumbprints": tps}))
        return

    cur = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
    changed = False
    # RGW (Squid) does not implement adding or removing a client ID: a changed
    # list means delete and create again (the ARN stays, the roles' trust
    # policies keep pointing at it).
    if sorted(set(cur.get("ClientIDList", []))) != clients:
        iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)
        iam.create_open_id_connect_provider(Url=a.url, ClientIDList=clients, ThumbprintList=tps)
        changed = True
    elif sorted(t.lower() for t in cur.get("ThumbprintList", [])) != sorted(tps):
        iam.update_open_id_connect_provider_thumbprint(OpenIDConnectProviderArn=arn, ThumbprintList=tps)
        changed = True
    print(json.dumps({"changed": changed, "arn": arn, "client_ids": clients, "thumbprints": tps}))


if __name__ == "__main__":
    main()
