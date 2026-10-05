#!/usr/bin/env bash
# Brings Ceph's STS roles back to the record of what is granted (lib-iam.sh):
# a role changed or deleted by hand, an authorize that failed half-way. The
# hourly CronJob iam-reconcile runs `apply`; make dataplatform-grants runs
# both in the Meta Catalog pod.
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

case ${1:-} in
  plan)
    iam_converge plan
    echo "$IAM_CHANGES change(s) planned (record version $RECORD_RV, $(jq length <<<"$RECORD") resources)" ;;
  apply)
    iam_apply
    backup ;;
  *)
    fail "usage: reconcile.sh <plan|apply>" ;;
esac
