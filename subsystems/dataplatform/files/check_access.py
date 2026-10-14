#!/usr/bin/env python3
"""Validate gitops/iam/access.yaml (make dataplatform-iam-check, CI's check job).

access.yaml holds only the platform's own part of the data authorization:
the bucket, the admin groups, and where Meta Catalog creates and grants
(metacatalog.prefix) with the user that creates every schema and table
(metacatalog.provisioner). Every grant comes from Meta Catalog: a `grants`
list here fails the check. iam-sync reads the file without PyYAML
(parse_access in iam_sync.py), so the check also proves both read it alike.

With --record FILE it validates a record of grants as well (a copy from
s3://<backup bucket>/metacatalog-grants/, or a file for
make dataplatform-grants ARGS="import FILE"): {resource: entry}, or the same
under "resources". An entry is a table's folder, {"path", "grants":
[{"access", "users", "groups"}]}, every path under the prefix, or the table
in the metastore, {"catalog", "schema", "table", "grants": [...]}. Prints a
summary; exits 1 on any problem. Standard library plus PyYAML
(requirements.txt).
"""
import argparse
import importlib.util
import json
import os
import re
import sys

import yaml

NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,40}[a-z0-9]$")           # users and groups (as iam-sync, lib-iam.sh)
PATH = re.compile(r"^[a-z0-9][a-z0-9._/-]*[a-z0-9]$")
BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
# folder.<domain>.<subdomain>.<layer>.<table> or table.<schema>.<table>
RESOURCE = re.compile(r"^(folder(\.[a-z][a-z0-9_]*){4}|table(\.[a-z][a-z0-9_]*){2})$")
IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")                       # a catalog, schema or table name


def check_access(path: str) -> tuple:
    """(errors, the document)"""
    errors = []
    with open(path) as f:
        doc = yaml.safe_load(f) or {}
    if not isinstance(doc, dict):
        return ["the file is not a mapping"], {}
    unknown = set(doc) - {"bucket", "admins", "metacatalog"}
    if "grants" in unknown:
        errors.append("grants: data grants come from Meta Catalog only (authorize a table there); "
                      "Meta Catalog's provisioner has rw on metacatalog.prefix without one")
        unknown.discard("grants")
    if unknown:
        errors.append(f"unknown top-level keys {sorted(unknown)}")
    bucket = doc.get("bucket")
    if not isinstance(bucket, str) or not BUCKET.match(bucket):
        errors.append(f"bucket: {bucket!r} is not a bucket name")
    admins = (doc.get("admins") or {}).get("groups", []) if isinstance(doc.get("admins") or {}, dict) else None
    if not isinstance(admins, list) or not admins or not all(isinstance(a, str) and NAME.match(a) for a in admins):
        errors.append("admins.groups must be a non-empty list of Keycloak group names")
    mc = doc.get("metacatalog")
    if not isinstance(mc, dict):
        errors.append("metacatalog: a mapping with prefix and provisioner")
    else:
        if set(mc) - {"prefix", "provisioner"}:
            errors.append(f"metacatalog: unknown keys {sorted(set(mc) - {'prefix', 'provisioner'})}")
        prefix = mc.get("prefix")
        if (not isinstance(prefix, str) or not PATH.match(prefix) or ".." in prefix.split("/") or "//" in prefix
                or prefix == "system" or prefix.startswith("system/")):
            errors.append(f"metacatalog.prefix: {prefix!r} must be a directory outside system/ (Spark's event logs)")
        prov = mc.get("provisioner")
        if not isinstance(prov, str) or not NAME.match(prov):
            errors.append(f"metacatalog.provisioner: {prov!r} is not a Keycloak user name")
    sync = os.path.join(os.path.dirname(os.path.abspath(path)), "..", "manifests", "governance", "iam-sync", "iam_sync.py")
    if os.path.isfile(sync):
        spec = importlib.util.spec_from_file_location("iam_sync", sync)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        with open(path) as f:
            if mod.parse_access(f.read()) != doc:
                errors.append("iam-sync's reader (parse_access in iam_sync.py) reads this file differently from PyYAML: "
                              "keep to block mappings, lists and [a, b] flow lists")
    return errors, doc


def check_record(path: str, prefix: str) -> tuple:
    """(errors, {role or "Trino": [what it is granted]})"""
    where = os.path.basename(path)
    try:
        with open(path) as f:
            doc = json.load(f)
    except (OSError, ValueError) as e:
        return [f"{where}: {e}"], {}
    if isinstance(doc, dict) and isinstance(doc.get("resources"), dict):
        doc = doc["resources"]
    if not isinstance(doc, dict):
        return [f"{where}: not a mapping of resources"], {}
    errors, roles, seen = [], {}, {}
    for name, r in sorted(doc.items()):
        at = f"{where}: {name}"
        if not RESOURCE.match(name):
            errors.append(f"{at}: not folder.<domain>.<subdomain>.<layer>.<table> nor table.<schema>.<table>")
        if not isinstance(r, dict) or not isinstance(r.get("grants"), list):
            errors.append(f"{at}: must have grants (a list)")
            continue
        if set(r) == {"path", "grants"}:
            p = r["path"]
            if not isinstance(p, str) or not PATH.match(p) or ".." in p.split("/") or "//" in p:
                errors.append(f"{at}: path {p!r}: lower-case letters, digits, '.', '_', '-' and '/', no leading or trailing '/'")
                continue
            if not prefix or not p.startswith(prefix + "/"):
                errors.append(f"{at}: path {p!r} is not under {prefix}/ (access.yaml metacatalog.prefix)")
                continue
            what, prefix_of = p, ("usr-", "grp-")
        elif set(r) == {"catalog", "schema", "table", "grants"}:
            if not all(isinstance(r[k], str) and IDENT.match(r[k]) for k in ("catalog", "schema", "table")):
                errors.append(f"{at}: catalog, schema and table: lower-case letters, digits and '_'")
                continue
            what, prefix_of = f"{r['catalog']}.{r['schema']}.{r['table']}", ("Trino: user ", "Trino: group ")
        else:
            errors.append(f"{at}: must be a folder (path, grants) or a table (catalog, schema, table, grants)")
            continue
        if what in seen:
            errors.append(f"{at}: {what!r} is also {seen[what]}'s")
        seen[what] = name
        levels = [g.get("access") if isinstance(g, dict) else None for g in r["grants"]]
        if len(set(levels)) != len(levels):
            errors.append(f"{at}: one entry per access level")
        for g in r["grants"]:
            if not isinstance(g, dict) or set(g) != {"access", "users", "groups"} or g["access"] not in ("rw", "ro"):
                errors.append(f"{at}: a grant is {{access: rw|ro, users: [...], groups: [...]}}")
                continue
            for kind in ("users", "groups"):
                names = g[kind]
                if not isinstance(names, list) or not all(isinstance(n, str) and NAME.match(n) for n in names):
                    errors.append(f"{at}: {kind} must be a list of Keycloak names (lower-case letters, digits, '-')")
                    continue
                for n in names:
                    roles.setdefault(prefix_of[kind == "groups"] + n, []).append(f"{what} ({g['access']})")
    return errors, roles


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("access", nargs="?", default=os.path.join(os.path.dirname(__file__), "..", "gitops", "iam", "access.yaml"))
    ap.add_argument("--record", help="a record of grants to validate too (JSON)")
    args = ap.parse_args()
    errors, doc = check_access(args.access)
    mc = doc.get("metacatalog") if isinstance(doc.get("metacatalog"), dict) else {}
    roles = {}
    if args.record:
        rec_errors, roles = check_record(args.record, mc.get("prefix") or "")
        errors += rec_errors
    for e in errors:
        print(f"access.yaml: {e}" if not e.startswith(os.path.basename(args.record or "-") + ":") else e, file=sys.stderr)
    if errors:
        return 1
    print(f"bucket {doc['bucket']}: admins {', '.join(doc['admins']['groups'])}; Meta Catalog grants under "
          f"{mc['prefix']}/, its provisioner {mc['provisioner']} has rw there")
    for role in sorted(roles):
        print(f"  {role:<36} {', '.join(roles[role])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
