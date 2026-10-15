#!/usr/bin/env python3
"""No site value outside the settings (make site-check, CI's ansible job).

Every value of inventory/group_vars/all/site.yml (and hosts.ini's node_ip)
belongs in that file: anything else derives from it, and the files of the
gitops tree that carry one are rendered from their .in templates. This
fails, naming file and line, when a tracked file holds one of them, in any
of the forms it takes (as written, regex-escaped, the network prefixes of
the LAN and of the pod network). Words that are also generic names (the
revision `main`, the bucket and account name, the storage classes, the
directory's NetBIOS name) are not searched. Allowed: site.yml itself, hosts.ini and inventory/known_hosts,
files rendered from a .in template, Markdown documentation, test fixtures
(files/tests/), the example site (publish/example/), and Meta Catalog's model
and tables (subsystems/dataplatform/metacatalog/: written out as Meta Catalog
gets them, so a table's grants name the site's groups as they are).

  site_check.py                  check the repository (git ls-files)
  site_check.py --forbid A.yml   check against another site file's values
                                 (with --no-exempt: rendered files too), the
                                 test that a different site leaves none of
                                 these values behind
Standard library plus PyYAML (requirements.txt).
"""
import argparse
import os
import re
import subprocess
import sys

import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SITE = "inventory/group_vars/all/site.yml"
HOSTS = "inventory/hosts.ini"
# values that are also ordinary words or names elsewhere in the tree
GENERIC = {"site_git_revision", "site_bucket", "site_rgw_account_name", "site_rbd_storage_class",
           "site_cephfs_storage_class", "site_git_host_keys", "site_k3s_api_vip", "site_ad_netbios_domain"}
EXEMPT_FILES = {SITE, HOSTS, "inventory/known_hosts"}
# the test fixtures; the example site (publish/), which the public copy puts in place of this one;
# Meta Catalog's model and tables, written out (their grants name the site's groups)
EXEMPT_PREFIXES = ("subsystems/dataplatform/files/tests/", "publish/example/", "subsystems/dataplatform/metacatalog/")


def needles(site_file: str, hosts_file: str) -> dict:
    """{text to find: the setting it comes from}"""
    with open(site_file) as f:
        site = yaml.safe_load(f) or {}
    out = {}
    for key, value in site.items():
        if key in GENERIC or not isinstance(value, str) or not value:
            continue
        out[value] = key
        if "." in value:
            out[re.escape(value)] = key + " (regex-escaped)"
    # network prefixes: the LAN's and the pod network's first octets
    for key in ("site_lan_network", "site_k3s_cluster_cidr"):
        net = site.get(key, "")
        m = re.match(r"^(\d+\.\d+\.\d+\.\d+)/(\d+)$", net)
        if m:
            octets = int(m.group(2)) // 8
            out[".".join(m.group(1).split(".")[:octets]) + "."] = key + " (prefix)"
    # the S3 VIP without its prefix length, the node addresses
    vip = site.get("site_s3_vip", "")
    if "/" in vip:
        out[vip.split("/")[0]] = "site_s3_vip"
    if os.path.exists(hosts_file):
        for ip in re.findall(r"\bnode_ip=(\S+)", open(hosts_file).read()):
            out[ip] = "node_ip (hosts.ini)"
    return out


def tracked() -> list:
    return subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True,
                          text=True).stdout.split("\0")


def exempt(path: str, files: set) -> bool:
    return (path in EXEMPT_FILES or path.startswith(EXEMPT_PREFIXES) or path.endswith(".md")
            or (path + ".in") in files)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--forbid", help="a site.yml whose values must not appear (default: this repository's)")
    ap.add_argument("--forbid-hosts", help="its hosts.ini (node_ip values)")
    ap.add_argument("--no-exempt", action="store_true", help="check rendered files and the inventory too")
    args = ap.parse_args()
    find = needles(args.forbid or os.path.join(ROOT, SITE), args.forbid_hosts or os.path.join(ROOT, HOSTS))
    # whole tokens: a value glued to a letter or digit is another word
    pattern = re.compile("|".join(f"(?<![A-Za-z0-9_]){re.escape(n)}(?![A-Za-z0-9_])" if not n.endswith(".")
                                  else f"(?<![A-Za-z0-9_.]){re.escape(n)}(?=[0-9])"
                                  for n in sorted(find, key=len, reverse=True)))
    files = set(f for f in tracked() if f)
    hits = []
    for path in sorted(files):
        if not args.no_exempt and exempt(path, files):
            continue
        if args.no_exempt and path in (SITE, HOSTS, "inventory/known_hosts"):
            continue
        try:
            with open(os.path.join(ROOT, path), encoding="utf-8") as f:
                lines = f.read().splitlines()
        except (UnicodeDecodeError, IsADirectoryError, FileNotFoundError):
            continue
        for n, line in enumerate(lines, 1):
            for m in pattern.finditer(line):
                hits.append(f"{path}:{n}: {m.group(0)} ({find.get(m.group(0), 'derived')})")
    if hits:
        print("Site values outside inventory/group_vars/all/site.yml (use the variable, or a .in template "
              "rendered by make configure):", file=sys.stderr)
        for h in hits:
            print("  " + h, file=sys.stderr)
        return 1
    print(f"no site value outside the settings ({len(find)} values, {len(files)} files)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
