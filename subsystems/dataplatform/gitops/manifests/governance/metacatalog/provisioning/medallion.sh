#!/usr/bin/env bash
# Meta Catalog's `script` provisioning task for the medallion lakehouses
# (subsystems/dataplatform/metacatalog/medallion-model.yaml). Meta Catalog runs
#
#   bash medallion.sh <provision|unprovision|authorize|reject>
#
# once per physical resource of a lakehouse, with the resource's values (JSON)
# on standard input and, on authorize, the grants the lakehouse's policy
# resolved for it in METACATALOG_ACCESS (JSON). The task has one script for
# both physical types: values with a `table` are a MedallionTableType (an
# Iceberg table), the others a MedallionSchemaType (an Iceberg schema).
# What it prints is recorded in the resource's provisioningResult or
# authorizationResult; a non-zero exit fails the resource.
#
#   provision    the schema (CREATE SCHEMA ... WITH (location)) or the table
#                (CREATE TABLE ... WITH (location, format PARQUET)) in the Trino
#                catalog $MEDALLION_CATALOG, as the Keycloak service account of
#                $PROVISIONER_CLIENT_ID: access.yaml grants it rw on
#                s3://$MEDALLION_BUCKET/$MEDALLION_PREFIX, which also makes it
#                an owner of every schema there. An existing schema or table
#                is kept when it is at the same location; a table gets the
#                columns the model added (ALTER TABLE ADD COLUMN); a column
#                whose type changed, or one at another location, fails: no
#                data is ever rewritten here.
#   unprovision  drops the table (Trino deletes its files) or the schema (only
#                when empty: Trino refuses otherwise, and so does this task)
#   authorize    writes the resource's grants into the record of what is
#                granted and brings Ceph's STS roles to it (lib-grants.sh,
#                lib-iam.sh); only platform-admins get this far (the gate in
#                metacatalog-proxy.yaml)
#   reject       removes them, likewise
#
# Identifiers and locations come from catalog values every realm user may
# edit, and they end up in SQL: each is checked against a strict pattern
# first, and column types against the list in trino_type.
set -euo pipefail
# errexit in $(...) too: bash drops it there otherwise, and a check failing
# inside a substitution (a refused column type) would let the statement run
shopt -s inherit_errexit

OPERATION=${1:?usage: medallion.sh <provision|unprovision|authorize|reject>}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

fail() { echo "$*" >&2; exit 1; }

: "${MEDALLION_CATALOG:?}" "${MEDALLION_BUCKET:?}" "${MEDALLION_PREFIX:?}"

VALUES=$(cat)
# A separator that is not white space: read collapses runs of tabs, which
# would shift the location into the table of a schema (no `table` value)
IFS=$'\x1f' read -r LAKEHOUSE NAMESPACE TABLE LOCATION < <(
  jq -r '[.lakehouse, .namespace, .table, .location] | map(. // "" | tostring) | join("\u001f")' <<<"$VALUES")

IDENT='^[a-z][a-z0-9_]{0,62}$'
[[ $LAKEHOUSE =~ ^[a-z][a-z0-9_]{0,40}$ ]] || fail "lakehouse name '$LAKEHOUSE': lower-case letters, digits and _"
[[ $NAMESPACE =~ $IDENT ]] || fail "schema name '$NAMESPACE': lower-case letters, digits and _"
[ -z "$TABLE" ] || [[ $TABLE =~ $IDENT ]] || fail "table name '$TABLE': lower-case letters, digits and _"
ROOT="s3://$MEDALLION_BUCKET/$MEDALLION_PREFIX/$LAKEHOUSE"
[[ $LOCATION =~ ^s3://[a-z0-9.-]+/[a-z0-9_/]+[a-z0-9_]$ && ${LOCATION#s3://} != *//* && $LOCATION == "$ROOT/"* ]] \
  || fail "location '$LOCATION' is not a directory under $ROOT/ (the lakehouse's storageRoot)"

Q_SCHEMA="\"$MEDALLION_CATALOG\".\"$NAMESPACE\""
if [ -n "$TABLE" ]; then
  RESOURCE="$MEDALLION_CATALOG.$NAMESPACE.$TABLE"
  Q_TABLE="$Q_SCHEMA.\"$TABLE\""
else
  RESOURCE="$MEDALLION_CATALOG.$NAMESPACE"
fi

# shellcheck source=lib-trino.sh
. "$HERE/lib-trino.sh"
case $OPERATION in
  authorize|reject)
    # shellcheck source=lib-iam.sh
    . "$HERE/lib-iam.sh"
    # shellcheck source=lib-grants.sh
    . "$HERE/lib-grants.sh" ;;
esac

# Iceberg's type names (as the model writes them) -> Trino's. Anything else is
# refused rather than passed into the statement.
trino_type() { # <type>
  local t
  t=$(tr 'A-Z' 'a-z' <<<"$1" | tr -d ' ')
  case $t in
    string|varchar) echo varchar ;;
    long|bigint) echo bigint ;;
    int|integer) echo integer ;;
    short|smallint) echo smallint ;;
    byte|tinyint) echo tinyint ;;
    boolean) echo boolean ;;
    float|real) echo real ;;
    double) echo double ;;
    date) echo date ;;
    time) echo 'time(6)' ;;
    timestamp) echo 'timestamp(6)' ;;
    timestamptz) echo 'timestamp(6) with time zone' ;;
    binary|varbinary) echo varbinary ;;
    uuid) echo uuid ;;
    decimal\([0-9],[0-9]\)|decimal\([0-9],[0-9][0-9]\)|decimal\([0-9][0-9],[0-9]\)|decimal\([0-9][0-9],[0-9][0-9]\)) echo "$t" ;;
    *) fail "column type '$1' of $RESOURCE: not one of string, long, int, short, byte, boolean, float, double, date, time, timestamp, timestamptz, binary, uuid, decimal(p,s)" ;;
  esac
}

# The model's columns as "name<TAB>trino type" lines.
model_columns() {
  local name type
  jq -r '.columns // "[]" | fromjson | .[] | [.name, .type] | @tsv' <<<"$VALUES" | while IFS=$'\t' read -r name type; do
    [[ $name =~ ^[a-z_][a-z0-9_]{0,62}$ ]] || fail "column name '$name' of $RESOURCE: lower-case letters, digits and _"
    type=$(trino_type "$type") || exit 1
    printf '%s\t%s\n' "$name" "$type"
  done
}

# The location an existing schema or table was created at, or nothing.
current_location() { # SCHEMA|TABLE <qualified name>
  trino "SHOW CREATE $1 $2" | jq -r '.[0]' | sed -n "s/^ *location = '\(.*\)'.*$/\1/p"
}

schema_exists() {
  [ "$(trino "SELECT count(*) FROM \"$MEDALLION_CATALOG\".information_schema.schemata WHERE schema_name = $(sql_str "$NAMESPACE")")" = "[1]" ]
}

table_exists() {
  [ "$(trino "SELECT count(*) FROM \"$MEDALLION_CATALOG\".information_schema.tables WHERE table_schema = $(sql_str "$NAMESPACE") AND table_name = $(sql_str "$TABLE")")" = "[1]" ]
}

provision_schema() {
  local at
  if schema_exists; then
    at=$(current_location SCHEMA "$Q_SCHEMA")
    [ "$at" = "$LOCATION" ] || fail "schema $RESOURCE exists at '$at', not at $LOCATION: drop or move it by hand"
    echo "schema $RESOURCE exists at $LOCATION"
  else
    trino "CREATE SCHEMA IF NOT EXISTS $Q_SCHEMA WITH (location = $(sql_str "$LOCATION"))" >/dev/null
    echo "created schema $RESOURCE at $LOCATION"
  fi
}

provision_table() {
  local want have at cols name type current added=""
  want=$(model_columns)
  [ -n "$want" ] || fail "table $RESOURCE has no columns"
  if ! table_exists; then
    cols=$(while IFS=$'\t' read -r name type; do printf '"%s" %s, ' "$name" "$type"; done <<<"$want")
    trino "CREATE TABLE IF NOT EXISTS $Q_TABLE (${cols%, }) WITH (format = 'PARQUET', location = $(sql_str "$LOCATION"))" >/dev/null
    echo "created table $RESOURCE at $LOCATION ($(wc -l <<<"$want" | tr -d ' ') columns)"
    return 0
  fi
  at=$(current_location TABLE "$Q_TABLE")
  [ "$at" = "$LOCATION" ] || fail "table $RESOURCE exists at '$at', not at $LOCATION: drop or move it by hand"
  have=$(trino "SELECT column_name, data_type FROM \"$MEDALLION_CATALOG\".information_schema.columns WHERE table_schema = $(sql_str "$NAMESPACE") AND table_name = $(sql_str "$TABLE")" | jq -r '@tsv')
  while IFS=$'\t' read -r name type; do
    current=$(awk -F'\t' -v n="$name" '$1 == n { print $2 }' <<<"$have")
    if [ -z "$current" ]; then
      trino "ALTER TABLE $Q_TABLE ADD COLUMN IF NOT EXISTS \"$name\" $type" >/dev/null
      added="$added $name"
    elif [ "$current" != "$type" ]; then
      fail "table $RESOURCE: column $name is $current, the model says $type: change it by hand (ALTER TABLE ... SET DATA TYPE) or unprovision"
    fi
  done <<<"$want"
  echo "table $RESOURCE exists at $LOCATION${added:+; added columns:$added}"
}

authorize() {
  local entries
  entries=$(grants_entries)
  grants_write "$(jq -cn --arg p "${LOCATION#"s3://$MEDALLION_BUCKET/"}" --argjson g "$entries" '{path: $p, grants: $g}')"
  echo "granted on ${LOCATION}: $(jq -r 'map(.access + " " + ((.groups | map("group " + .)) + (.users | map("user " + .)) | join(", "))) | join("; ") | if . == "" then "nobody" else . end' <<<"$entries")"
}

# one token for every statement of the run (trino runs in subshells, which
# would each log in again)
case $OPERATION in provision|unprovision) trino_login ;; esac

case $OPERATION in
  provision)
    if [ -n "$TABLE" ]; then provision_table; else provision_schema; fi ;;
  unprovision)
    if [ -n "$TABLE" ]; then
      trino "DROP TABLE IF EXISTS $Q_TABLE" >/dev/null
      echo "dropped table $RESOURCE (and its files)"
    else
      trino "DROP SCHEMA IF EXISTS $Q_SCHEMA" >/dev/null
      echo "dropped schema $RESOURCE"
    fi ;;
  authorize)
    authorize ;;
  reject)
    grants_write null
    echo "no grants on $LOCATION any more" ;;
  *)
    fail "unknown operation: $OPERATION" ;;
esac
