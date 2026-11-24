#!/usr/bin/env bash
# Meta Catalog's `script` provisioning task for the platform's tables
# (subsystems/dataplatform/metacatalog/table-model.yaml). Meta Catalog runs
#
#   bash table.sh <provision|unprovision|authorize|reject>
#
# once per physical resource of a table, with the resource's values (JSON) on
# standard input and, on authorize, the grants the table's policy resolved
# for it in METACATALOG_ACCESS (JSON). The task has one script for both
# physical types; `kind` in the values tells them apart:
#
#   folder (S3FolderType)   the table's directory in the platform bucket,
#                           s3://$LAKEHOUSE_BUCKET/$LAKEHOUSE_PREFIX/<domain>/<layer>/<name>_v<version>
#   table  (HiveTableType)  the table in the Hive Metastore, as Trino's
#                           $LAKEHOUSE_CATALOG.<domain>_<layer>.<name>_v<version>
#
# A table's version is part of its name, and its domain is an entity of
# Meta Catalog (a $DOMAIN_TYPE), which must exist before anything is made.
#
# Meta Catalog takes the folder first on provision and authorize, the table
# first on reject and unprovision. What this prints is recorded in the
# resource's provisioningResult or authorizationResult; a non-zero exit
# fails the resource.
#
#   provision    folder: makes the directory (an empty object <path>/), as
#                the provisioner's own STS role. The metastore then finds it
#                and never has to write to the bucket.
#                table: the schema <domain>_<layer> when it is
#                not there (at the layer's directory), then the table, in
#                the table's format: iceberg (an Iceberg table), parquet or
#                avro (a Hive table over the folder's files). Through Trino,
#                as the Keycloak service account of $PROVISIONER_CLIENT_ID,
#                the only user Trino lets create anything. An existing table
#                is kept when it is the same (format, folder) and gets the
#                columns the model added; a column whose type changed, or a
#                table of another format or at another place, fails: no data
#                is ever rewritten here.
#   unprovision  table: drops it (and the schema once it is empty). Trino
#                deletes an Iceberg table's files; a Hive table's stay.
#                folder: deletes whatever is left in the directory.
#   authorize    writes the resource's grants into the record of what is
#                granted (lib-grants.sh). For the folder they become rights
#                on its files: Ceph's STS roles, at once (lib-iam.sh). For
#                the table they become rights on statements: Trino's rules
#                in the Open Policy Agent, which iam-sync writes from the
#                record within a minute. Only the admin group gets this far
#                (the gate in metacatalog-proxy.yaml).
#   reject       removes them, likewise
#
# Names and places come from catalog values every user of Meta Catalog may
# edit, and they end up in SQL and in S3 keys: each name is checked against
# a strict pattern, every place is built again here from the names and must
# be what the values say, and column types are checked against a list.
set -euo pipefail
# errexit in $(...) too: bash drops it there otherwise, and a check failing
# inside a substitution (a refused column type) would let the statement run
shopt -s inherit_errexit

OPERATION=${1:?usage: table.sh <provision|unprovision|authorize|reject>}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

fail() { echo "$*" >&2; exit 1; }

: "${LAKEHOUSE_CATALOG:?}" "${LAKEHOUSE_BUCKET:?}" "${LAKEHOUSE_PREFIX:?}"

VALUES=$(cat)
# A separator that is not white space: read collapses runs of tabs, which
# would shift the values after an empty one
IFS=$'\x1f' read -r KIND DOMAIN SUBDOMAIN LAYER TABLE LOCATION DIRECTORY CATALOG SCHEMA FORMAT SCHEMA_LOCATION < <(
  jq -r '[.kind, .domain, .subdomain, .layer, .table, .location, .path, .catalog, .schema, .format, .schemaLocation]
         | map(. // "" | tostring) | join("\u001f")' <<<"$VALUES")

NAME='^[a-z][a-z0-9]{0,23}$'
[[ $DOMAIN =~ $NAME ]] || fail "domain '$DOMAIN': lower-case letters and digits"
[[ $LAYER =~ ^(bronze|silver|gold)$ ]] || fail "layer '$LAYER': bronze, silver or gold"
[[ $TABLE =~ ^[a-z][a-z0-9_]{0,62}$ ]] || fail "table name '$TABLE': lower-case letters, digits and _"

# where the table is, said by its names alone; a subdomain is a value of the
# platform's earlier model, which no longer exists
[ -z "$SUBDOMAIN" ] || fail "subdomain '$SUBDOMAIN': the platform's model has no subdomains (an earlier model's resource?)"
[[ $TABLE =~ _v[1-9][0-9]{0,2}$ ]] || fail "table name '$TABLE': a table is named <name>_v<version>"
PLACE_NAMES="$DOMAIN/$LAYER/$TABLE"
LAYER_DIRECTORY="$LAKEHOUSE_PREFIX/$DOMAIN/$LAYER"
WANT_SCHEMA="${DOMAIN}_${LAYER}"
FOLDER_RESOURCE="folder.$DOMAIN.$LAYER.$TABLE"
WANT_DIRECTORY="$LAYER_DIRECTORY/$TABLE"
WANT_LOCATION="s3://$LAKEHOUSE_BUCKET/$WANT_DIRECTORY"
[ "$LOCATION" = "$WANT_LOCATION" ] \
  || fail "the $KIND of $PLACE_NAMES is at '$LOCATION', not at $WANT_LOCATION: a table's names do not change (make another table)"

case $KIND in
  folder)
    [ "$DIRECTORY" = "$WANT_DIRECTORY" ] || fail "the folder's path is '$DIRECTORY', not $WANT_DIRECTORY"
    RESOURCE=$FOLDER_RESOURCE ;;
  table)
    [ "$CATALOG" = "$LAKEHOUSE_CATALOG" ] || fail "catalog '$CATALOG': this platform's is $LAKEHOUSE_CATALOG"
    [ "$SCHEMA" = "$WANT_SCHEMA" ] || fail "schema '$SCHEMA' is not $WANT_SCHEMA"
    [ "$SCHEMA_LOCATION" = "s3://$LAKEHOUSE_BUCKET/$LAYER_DIRECTORY" ] \
      || fail "the schema's place is '$SCHEMA_LOCATION', not s3://$LAKEHOUSE_BUCKET/$LAYER_DIRECTORY"
    [[ $FORMAT =~ ^(parquet|avro|iceberg)$ ]] || fail "format '$FORMAT': parquet, avro or iceberg"
    RESOURCE="table.$SCHEMA.$TABLE"
    NAMED="$CATALOG.$SCHEMA.$TABLE"
    Q_SCHEMA="\"$CATALOG\".\"$SCHEMA\""
    Q_TABLE="$Q_SCHEMA.\"$TABLE\"" ;;
  *)
    fail "kind '$KIND': folder or table (the values of an S3FolderType or a HiveTableType)" ;;
esac

case $OPERATION in
  provision|unprovision)
    # shellcheck source=lib-trino.sh
    . "$HERE/lib-trino.sh"
    # shellcheck source=lib-s3.sh
    [ "$KIND" != folder ] || . "$HERE/lib-s3.sh" ;;
  authorize|reject)
    # shellcheck source=lib-iam.sh
    . "$HERE/lib-iam.sh"
    # shellcheck source=lib-grants.sh
    . "$HERE/lib-grants.sh" ;;
  *)
    fail "unknown operation: $OPERATION" ;;
esac

# ---------------------------------------------------------------- the table ----
# The model's type names -> Trino's, for the table's format. Anything else is
# refused rather than passed into the statement. A Hive table (parquet, avro)
# holds neither a time, a time zone nor a uuid, and keeps milliseconds; avro
# has no integer smaller than int.
trino_type() { # <type>
  local t
  t=$(tr 'A-Z' 'a-z' <<<"$1" | tr -d ' ')
  case $t in
    string|varchar) echo varchar; return ;;
    long|bigint) echo bigint; return ;;
    int|integer) echo integer; return ;;
    boolean) echo boolean; return ;;
    float|real) echo real; return ;;
    double) echo double; return ;;
    date) echo date; return ;;
    binary|varbinary) echo varbinary; return ;;
    decimal\([0-9],[0-9]\)|decimal\([0-9],[0-9][0-9]\)|decimal\([0-9][0-9],[0-9]\)|decimal\([0-9][0-9],[0-9][0-9]\)) echo "$t"; return ;;
  esac
  case $FORMAT:$t in
    iceberg:timestamp) echo 'timestamp(6)' ;;
    iceberg:timestamptz) echo 'timestamp(6) with time zone' ;;
    iceberg:time) echo 'time(6)' ;;
    iceberg:uuid) echo uuid ;;
    parquet:timestamp|avro:timestamp) echo 'timestamp(3)' ;;
    parquet:short|parquet:smallint) echo smallint ;;
    parquet:byte|parquet:tinyint) echo tinyint ;;
    *) fail "column type '$1' of $NAMED: a table in $FORMAT holds string, long, int, boolean, float, double, date, timestamp, binary, decimal(p,s)$(
         case $FORMAT in iceberg) echo ", timestamptz, time, uuid" ;; parquet) echo ", short, byte" ;; esac)" ;;
  esac
}

# The model's columns as "name<TAB>trino type" lines.
model_columns() {
  local name type
  jq -r '.columns // "[]" | fromjson | .[] | [.name, .type] | @tsv' <<<"$VALUES" | while IFS=$'\t' read -r name type; do
    [[ $name =~ ^[a-z_][a-z0-9_]{0,62}$ ]] || fail "column name '$name' of $NAMED: lower-case letters, digits and _"
    type=$(trino_type "$type") || exit 1
    printf '%s\t%s\n' "$name" "$type"
  done
}

# What Trino says a schema or table was made with: the value of a property
# of its SHOW CREATE statement, or nothing.
created_with() { # SCHEMA|TABLE <qualified name> <property>
  trino "SHOW CREATE $1 $2" | jq -r '.[0]' | sed -n "s/^ *$3 = '\(.*\)'.*$/\1/p"
}

# Both are asked in conditions (if !, ||), where bash ignores errexit: a
# Trino that does not answer must end the run (trino has said why), never
# read as "not there". 2026-11-22: unprovisions run while Trino was stopped
# answered "its schema is not there, nothing to drop", succeeded, and left
# every table in the metastore with its files deleted.
schema_exists() {
  local n
  n=$(trino "SELECT count(*) FROM \"$CATALOG\".information_schema.schemata WHERE schema_name = $(sql_str "$SCHEMA")") || exit 1
  [ "$n" = "[1]" ]
}

table_exists() {
  local n
  n=$(trino "SELECT count(*) FROM \"$CATALOG\".information_schema.tables WHERE table_schema = $(sql_str "$SCHEMA") AND table_name = $(sql_str "$TABLE")") || exit 1
  [ "$n" = "[1]" ]
}

# The schema of the table's layer: made by the first table that needs it.
provision_schema() {
  local at
  # two tables of one layer may get here together: IF NOT EXISTS, then look
  schema_exists || trino "CREATE SCHEMA IF NOT EXISTS $Q_SCHEMA WITH (location = $(sql_str "$SCHEMA_LOCATION"))" >/dev/null
  at=$(created_with SCHEMA "$Q_SCHEMA" location)
  [ "$at" = "$SCHEMA_LOCATION" ] || fail "schema $CATALOG.$SCHEMA exists at '$at', not at $SCHEMA_LOCATION: drop or move it by hand"
}

# How the table's format is said to Trino: the connector's table type, its
# file format, and the property that names the place.
case ${FORMAT:-} in
  iceberg) TABLE_TYPE=ICEBERG FILE_FORMAT=PARQUET PLACE=location ;;
  parquet) TABLE_TYPE=HIVE FILE_FORMAT=PARQUET PLACE=external_location ;;
  avro) TABLE_TYPE=HIVE FILE_FORMAT=AVRO PLACE=external_location ;;
esac

provision_table() {
  local want have at type format cols name current added=""
  want=$(model_columns)
  [ -n "$want" ] || fail "table $NAMED has no columns"
  provision_schema
  if ! table_exists; then
    cols=$(while IFS=$'\t' read -r name type; do printf '"%s" %s, ' "$name" "$type"; done <<<"$want")
    trino "CREATE TABLE IF NOT EXISTS $Q_TABLE (${cols%, }) WITH (type = '$TABLE_TYPE', format = '$FILE_FORMAT', $PLACE = $(sql_str "$LOCATION"))" >/dev/null
    echo "created table $NAMED ($FORMAT, $(wc -l <<<"$want" | tr -d ' ') columns) at $LOCATION"
    return 0
  fi
  type=$(created_with TABLE "$Q_TABLE" type)
  format=$(created_with TABLE "$Q_TABLE" format)
  [ "$type" = "$TABLE_TYPE" ] && [ "$format" = "$FILE_FORMAT" ] \
    || fail "table $NAMED exists as $type/$format, the model says $FORMAT ($TABLE_TYPE/$FILE_FORMAT): a table's format does not change (unprovision it, or make another table)"
  at=$(created_with TABLE "$Q_TABLE" "$PLACE")
  [ "$at" = "$LOCATION" ] || fail "table $NAMED exists at '$at', not at $LOCATION: drop or move it by hand"
  have=$(trino "SELECT column_name, data_type FROM \"$CATALOG\".information_schema.columns WHERE table_schema = $(sql_str "$SCHEMA") AND table_name = $(sql_str "$TABLE")" | jq -r '@tsv')
  while IFS=$'\t' read -r name type; do
    current=$(awk -F'\t' -v n="$name" '$1 == n { print $2 }' <<<"$have")
    if [ -z "$current" ]; then
      trino "ALTER TABLE $Q_TABLE ADD COLUMN IF NOT EXISTS \"$name\" $type" >/dev/null
      added="$added $name"
    elif [ "$current" != "$type" ]; then
      fail "table $NAMED: column $name is $current, the model says $type: change it by hand (ALTER TABLE ... SET DATA TYPE) or unprovision"
    fi
  done <<<"$want"
  echo "table $NAMED exists ($FORMAT) at $LOCATION${added:+; added columns:$added}"
}

# Trino's lakehouse connector cannot drop an Iceberg table whose metadata
# files are gone ("Unsupported table handle: ...CorruptedIcebergTableHandle",
# measured on 2026-11-22 after a reset that emptied the folders while the
# entries stayed): the metastore's Iceberg REST catalog then drops the entry
# without reading them (no purge; measured on Hive 4.2.0: 200, the table's
# neighbours stay, 404 once gone).
drop_table() {
  local err code
  if err=$( (trino "DROP TABLE IF EXISTS $Q_TABLE" >/dev/null) 2>&1 ); then
    return 0
  fi
  case $err in
    *CorruptedIcebergTableHandle*) ;;
    *) fail "$err" ;;
  esac
  : "${METASTORE_REST:?}"
  code=$(curl -sS --max-time 60 -o /dev/null -w '%{http_code}' -X DELETE \
    "$METASTORE_REST/v1/namespaces/$SCHEMA/tables/$TABLE") || code=000
  case $code in
    200|204|404) echo "table $NAMED: its metadata files were gone; its entry was dropped in the metastore's REST catalog" ;;
    *) fail "the metastore's REST catalog ($METASTORE_REST) did not drop $SCHEMA.$TABLE (HTTP $code)" ;;
  esac
}

unprovision_table() {
  local left files="" schema=""
  if ! schema_exists; then
    echo "table $NAMED: its schema is not there, nothing to drop"
    return 0
  fi
  drop_table
  [ "$TABLE_TYPE" != HIVE ] || files="; its files stay until the folder goes"
  # the schema goes with its last table; another table made meanwhile keeps it
  left=$(trino "SELECT count(*) FROM \"$CATALOG\".information_schema.tables WHERE table_schema = $(sql_str "$SCHEMA")")
  if [ "$left" = "[0]" ] && (trino "DROP SCHEMA IF EXISTS $Q_SCHEMA") >/dev/null 2>&1; then
    schema="; its schema $CATALOG.$SCHEMA was empty and went too"
  fi
  echo "dropped table $NAMED$files$schema"
}

# ---------------------------------------------------------------- the folder ----
provision_folder() {
  s3_mkdir "$DIRECTORY"
  echo "folder $LOCATION is there"
}

unprovision_folder() {
  s3_rmtree "$DIRECTORY"
  echo "emptied $LOCATION ($S3_DELETED objects deleted)"
}

# ---------------------------------------------------------------- the grants ----
said() { # <entries>: the grants in words
  jq -r 'map(.access + " " + ((.groups | map("group " + .)) + (.users | map("user " + .)) | join(", "))) | join("; ")
         | if . == "" then "nobody" else . end' <<<"$1"
}

authorize() {
  local entries
  entries=$(grants_entries)
  if [ "$KIND" = folder ]; then
    grants_write "$(jq -cn --arg p "$DIRECTORY" --argjson g "$entries" '{path: $p, grants: $g}')"
    echo "granted on the files of $LOCATION: $(said "$entries")"
  else
    grants_write "$(jq -cn --arg c "$CATALOG" --arg s "$SCHEMA" --arg t "$TABLE" --argjson g "$entries" \
      '{catalog: $c, schema: $s, table: $t, grants: $g}')"
    echo "granted on the table $NAMED in Trino: $(said "$entries") (in force within a minute)"
  fi
}

# A table is made only in a domain that is an entity of Meta Catalog: a
# $DOMAIN_TYPE named so (subsystems/dataplatform/metacatalog/table-model.yaml;
# make dataplatform-tables makes the domains of tables.yaml). Meta Catalog
# keeps no rule that ties a table's `domain` to one. Asked at Meta Catalog's
# own Service, as the provisioner, on provision only: the tables of a domain
# taken out must still be unprovisioned, rejected and deleted. The script
# runs on Meta Catalog's task executor, on a thread of its own that may start
# before the transaction that planned it commits; the call only reads, so it
# waits on no lock of that transaction. Without an answer nothing is made.
DOMAIN_TYPE=${DOMAIN_TYPE:-DomainType}
known_domain() {
  local out code found
  : "${METACATALOG_API:?}"
  out=$(mktemp)
  code=$(curl -sS --max-time 30 -o "$out" -w '%{http_code}' -G \
    -K <(printf 'header = "Authorization: Bearer %s"\n' "$TRINO_TOKEN") \
    --data-urlencode "entityTypeName=$DOMAIN_TYPE" --data-urlencode "queryPath=\$ ? (@.name == \"$DOMAIN\")" \
    "$METACATALOG_API/entity") || code=000
  found=$(jq 'length' "$out" 2>/dev/null) || found=""
  rm -f "$out"
  [ "$code" = 200 ] && [ -n "$found" ] || fail "Meta Catalog ($METACATALOG_API) did not list the domains ($DOMAIN_TYPE, HTTP $code): nothing is made"
  [ "$found" -ge 1 ] || fail "domain '$DOMAIN' is no $DOMAIN_TYPE of Meta Catalog: nothing is made"
}

# one token for every statement of the run (trino runs in subshells, which
# would each log in again)
case $OPERATION in provision|unprovision) trino_login ;; esac
[ "$OPERATION" != provision ] || known_domain

case $OPERATION:$KIND in
  provision:folder) provision_folder ;;
  provision:table) provision_table ;;
  unprovision:folder) unprovision_folder ;;
  unprovision:table) unprovision_table ;;
  authorize:*) authorize ;;
  reject:folder)
    grants_write null
    echo "no grants on the files of $LOCATION any more" ;;
  reject:table)
    grants_write null
    echo "no grants on the table $NAMED in Trino any more" ;;
esac
