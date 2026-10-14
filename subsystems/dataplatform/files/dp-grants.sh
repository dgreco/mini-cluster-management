#!/usr/bin/env bash
# The record of what is granted on the platform's data (ConfigMap
# governance/metacatalog-grants), for platform administrators: what Meta
# Catalog's authorize and reject wrote. A table has two entries: its folder
# (folder.<domain>.<subdomain>.<layer>.<table>), which Ceph's STS roles and
# Trino's S3 mapping follow, and the table in the metastore
# (table.<schema>.<table>), which Trino's rules in the Open Policy Agent
# follow. Grants are given and taken in Meta Catalog; this is for looking at
# them and for repairs. Needs kubectl (context k3s: make
# k3s-kubeconfig), jq, and for import python3 with PyYAML (requirements.txt).
#
#   dp-grants.sh list               every resource with its folder or table and its grants
#   dp-grants.sh export [<file>]    the record as JSON (standard output without a file)
#   dp-grants.sh plan               what the roles in Ceph lack against the record (changes nothing)
#   dp-grants.sh apply              bring the roles to the record now (iam-reconcile does it hourly)
#   dp-grants.sh remove <resource>  drop one resource's grants, then apply: for a table
#                                   deleted in Meta Catalog without a reject first
#   dp-grants.sh import <file>      put the resources of a copy into the record, then apply:
#                                   a restore from s3://<backup bucket>/metacatalog-grants/
#                                   (validated first; resources missing from the copy are kept)
set -euo pipefail

NS=governance
CM=metacatalog-grants
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

die() { echo "$*" >&2; exit 1; }
record() { kubectl -n "$NS" get configmap "$CM" -o json | jq '(.data // {}) | map_values(fromjson? // .)'; }
in_pod() { kubectl -n "$NS" exec deploy/metacatalog -c metacatalog -- bash /opt/metacatalog/provisioning/reconcile.sh "$1"; }

case ${1:-} in
  list)
    record | jq -r 'to_entries[] | "\(.key)\t\(.value.path // "\(.value.catalog).\(.value.schema).\(.value.table)")\t\(.value.grants | map(.access + ": " + ((.groups | map("group " + .)) + (.users | map("user " + .)) | join(", "))) | join("; "))"' \
      | column -t -s $'\t' ;;
  export)
    if [ -n "${2:-}" ]; then record >"$2"; echo "written to $2"; else record; fi ;;
  plan)
    in_pod plan ;;
  apply)
    in_pod apply ;;
  remove)
    [ -n "${2:-}" ] || die "usage: dp-grants.sh remove <resource>   (dp-grants.sh list names them)"
    record | jq -e --arg r "$2" 'has($r)' >/dev/null || die "$2 is not in the record"
    kubectl -n "$NS" patch configmap "$CM" --type merge -p "$(jq -cn --arg r "$2" '{data: {($r): null}}')" >/dev/null
    echo "removed $2 from the record"
    # taken out on purpose: should the record now be empty, the roles go too
    kubectl -n "$NS" exec deploy/metacatalog -c metacatalog -- \
      env IAM_ENTRY_REMOVED=true bash /opt/metacatalog/provisioning/reconcile.sh apply ;;
  import)
    [ -f "${2:-}" ] || die "usage: dp-grants.sh import <file>"
    python3 "$HERE/check_access.py" --record "$2" >/dev/null
    kubectl -n "$NS" patch configmap "$CM" --type merge \
      -p "$(jq -c '(if (.resources | type) == "object" then .resources else . end) | {data: map_values(tojson)}' "$2")" >/dev/null
    echo "imported $(jq '(if (.resources | type) == "object" then .resources else . end) | length' "$2") resources into the record"
    in_pod apply ;;
  *)
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
    exit 1 ;;
esac
