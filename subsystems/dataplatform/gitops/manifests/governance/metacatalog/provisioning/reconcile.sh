#!/usr/bin/env bash
# Takes out of the record of what is granted the entries of resources Meta
# Catalog no longer has (prune, below), then brings Ceph's STS roles back to
# the record (lib-iam.sh): a role changed or deleted by hand, an authorize
# that failed half-way. The CronJob iam-reconcile runs `apply` every ten
# minutes; make dataplatform-grants runs both in the Meta Catalog pod.
#
#   bash reconcile.sh plan     prints what apply would change, changes nothing
#   bash reconcile.sh apply    converges, then copies the record to
#                              s3://$BACKUP_BUCKET/metacatalog-grants/ (latest.json
#                              and one file per day) when BACKUP_BUCKET is set
#
# It applies the record, never Meta Catalog's current policies: a policy
# edited since its last authorize stays a proposal until an administrator
# authorizes it.
set -euo pipefail
shopt -s inherit_errexit

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
fail() { echo "$*" >&2; exit 1; }

# shellcheck source=lib-iam.sh
. "$HERE/lib-iam.sh"

# The record as one JSON document ({resource: entry}), the input of
# make dataplatform-grants ARGS="import <file>".
backup() {
  local body day key code
  [ -n "${BACKUP_BUCKET:-}" ] || return 0
  : "${BACKUP_KEY_DIR:?}"
  body=$(jq -S . <<<"$RECORD")
  day=$(date -u +%F)
  for key in metacatalog-grants/latest.json "metacatalog-grants/$day.json"; do
    code=$(curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT \
      --aws-sigv4 "aws:amz:$IAM_REGION:s3" \
      -K <(printf 'user = "%s:%s"\n' "$(cat "$BACKUP_KEY_DIR/access-key")" "$(cat "$BACKUP_KEY_DIR/secret-key")") \
      -H 'Content-Type: application/json' --data-binary "$body" \
      "$IAM_ENDPOINT/$BACKUP_BUCKET/$key") || code=000
    [ "$code" = 200 ] || fail "copying the record to s3://$BACKUP_BUCKET/$key answered HTTP $code"
  done
  echo "record copied to s3://$BACKUP_BUCKET/metacatalog-grants/ ($(jq length <<<"$RECORD") resources)"
}

# The entries of resources Meta Catalog no longer has go out of the record:
# a table deleted without a reject (Meta Catalog's delete, in its UI or its
# API, calls no script) keeps its grants in force otherwise. A folder's entry
# counts by its path among the S3FolderType resources, a table's by its schema
# and name among the HiveTableType ones. Its rules then leave the Open Policy
# Agent with iam-sync's next round, its folder leaves the roles in the
# converge that follows (an entry taken out on purpose: IAM_ENTRY_REMOVED).
# Meta Catalog is asked as its provisioner, at its own Service (the proxy in
# front of it lets people in, not services), for two listings. Nothing is
# taken out while it does not answer, or holds no resource at all while the
# record holds some: a Meta Catalog set up again empty is likelier than every
# table deleted (make dataplatform-grants ARGS="remove ..." when it is).
# Sets PRUNED (the entries taken out, or that plan would take out).
prune() { # plan|apply
  local secret resp token folders tables gone key
  PRUNED=0
  record_read
  record_check
  [ "$(jq length <<<"$RECORD")" -gt 0 ] || return 0
  if [ -z "${METACATALOG_API:-}" ] || [ -z "${KEYCLOAK_TOKEN_URL:-}" ] || [ -z "${PROVISIONER_CLIENT_ID:-}" ] \
     || [ ! -r "${PROVISIONER_CLIENT_SECRET_FILE:-/nonexistent}" ]; then
    echo "pruning skipped: METACATALOG_API, KEYCLOAK_TOKEN_URL, PROVISIONER_CLIENT_ID or PROVISIONER_CLIENT_SECRET_FILE is not set"
    return 0
  fi
  secret=$(cat "$PROVISIONER_CLIENT_SECRET_FILE")
  resp=$(curl -sS --max-time 30 -K <(printf 'user = "%s:%s"\n' "$PROVISIONER_CLIENT_ID" "$secret") \
    -d grant_type=client_credentials "$KEYCLOAK_TOKEN_URL" 2>&1) || resp=""
  token=$(jq -r '.access_token // empty' <<<"$resp" 2>/dev/null) || token=""
  if [ -z "$token" ]; then
    echo "pruning skipped: no token for $PROVISIONER_CLIENT_ID from Keycloak"
    return 0
  fi
  listing() { # <entity type>: the values of its entities as a JSON array, or nothing
    local out code
    out=$(mktemp)
    code=$(curl -sS --max-time 60 -o "$out" -w '%{http_code}' -G --data-urlencode "entityTypeName=$1" \
      -K <(printf 'header = "Authorization: Bearer %s"\n' "$token") "$METACATALOG_API/entity") || code=000
    [ "$code" = 200 ] && jq -c '[.[] | .values | if type == "string" then fromjson else . end]' "$out" 2>/dev/null
    rm -f "$out"
  }
  folders=$(listing S3FolderType) || folders=""
  tables=$(listing HiveTableType) || tables=""
  if [ -z "$folders" ] || [ -z "$tables" ]; then
    echo "pruning skipped: Meta Catalog ($METACATALOG_API) did not list its folders and tables"
    return 0
  fi
  if [ "$(jq -n --argjson f "$folders" --argjson t "$tables" '($f | length) + ($t | length)')" = 0 ]; then
    echo "pruning skipped: Meta Catalog holds no folder and no table, the record $(jq length <<<"$RECORD") entries (if every table is gone on purpose: make dataplatform-grants ARGS=\"remove ...\")"
    return 0
  fi
  gone=$(jq -r --argjson f "$folders" --argjson t "$tables" '
    ($f | map(.path)) as $paths | ($t | map("\(.schema).\(.table)")) as $names
    | to_entries[]
    | select((.key | startswith("folder.")) and ((.value.path) as $p | $paths | index([$p]) | not)
             or (.key | startswith("table.")) and ("\(.value.schema).\(.value.table)" as $n | $names | index([$n]) | not))
    | .key' <<<"$RECORD")
  for key in $gone; do
    PRUNED=$((PRUNED + 1))
    if [ "$1" = plan ]; then
      echo "would remove $key from the record: Meta Catalog has no such resource any more"
    else
      record_put "$key" null
      echo "removed $key from the record: Meta Catalog has no such resource any more"
    fi
  done
  return 0
}

case ${1:-} in
  plan)
    prune plan
    iam_converge plan
    echo "$((IAM_CHANGES + PRUNED)) change(s) planned (record version $RECORD_RV, $(jq length <<<"$RECORD") resources)" ;;
  apply)
    prune apply
    [ "$PRUNED" -eq 0 ] || export IAM_ENTRY_REMOVED=true
    iam_apply
    backup ;;
  *)
    fail "usage: reconcile.sh <plan|apply>" ;;
esac
