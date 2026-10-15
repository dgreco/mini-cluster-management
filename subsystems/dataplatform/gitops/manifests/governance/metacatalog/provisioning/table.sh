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
#                           s3://$LAKEHOUSE_BUCKET/$LAKEHOUSE_PREFIX/<domain>/<subdomain>/<layer>/<table>
#   table  (HiveTableType)  the table in the Hive Metastore, as Trino's
#                           $LAKEHOUSE_CATALOG.<domain>_<subdomain>_<layer>.<table>
#
# Meta Catalog takes the folder first on provision and authorize, the table
# first on reject and unprovision. What this prints is recorded in the
# resource's provisioningResult or authorizationResult; a non-zero exit
# fails the resource.
#
#   provision    folder: makes the directory (an empty object <path>/), as
#                the provisioner's own STS role. The metastore then finds it
#                and never has to write to the bucket.
#                table: the schema <domain>_<subdomain>_<layer> when it is
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
[[ $SUBDOMAIN =~ $NAME ]] || fail "subdomain '$SUBDOMAIN': lower-case letters and digits"
[[ $LAYER =~ ^(bronze|silver|gold)$ ]] || fail "layer '$LAYER': bronze, silver or gold"
[[ $TABLE =~ ^[a-z][a-z0-9_]{0,62}$ ]] || fail "table name '$TABLE': lower-case letters, digits and _"

# A table is made only under one of its domain's own subdomains (domains.json,
# rendered from metacatalog/domains.yaml): Meta Catalog checks the domain and
# the subdomain each against its enum, not the pair. On provision only: the
# tables of a pair taken out of the definition must still be unprovisioned,
# rejected and deleted.
if [ "$OPERATION" = provision ]; then
  DOMAINS_FILE="$HERE/domains.json"
  SUBDOMAINS=$(jq -r --arg d "$DOMAIN" 'if has($d) then .[$d] | join(", ") else "" end' "$DOMAINS_FILE") \
    || fail "the domains' definition $DOMAINS_FILE cannot be read: nothing is made"
  [ -n "$SUBDOMAINS" ] || fail "domain '$DOMAIN' is not one of metacatalog/domains.yaml: nothing is made"
  jq -e --arg d "$DOMAIN" --arg s "$SUBDOMAIN" '.[$d] | index($s) != null' "$DOMAINS_FILE" >/dev/null \
    || fail "subdomain '$SUBDOMAIN' is not one of the domain $DOMAIN's ($SUBDOMAINS; metacatalog/domains.yaml): nothing is made"
fi

# where the table is, said by its names alone
LAYER_DIRECTORY="$LAKEHOUSE_PREFIX/$DOMAIN/$SUBDOMAIN/$LAYER"
WANT_DIRECTORY="$LAYER_DIRECTORY/$TABLE"
WANT_LOCATION="s3://$LAKEHOUSE_BUCKET/$WANT_DIRECTORY"
[ "$LOCATION" = "$WANT_LOCATION" ] \
  || fail "the $KIND of $DOMAIN/$SUBDOMAIN/$LAYER/$TABLE is at '$LOCATION', not at $WANT_LOCATION: a table's names do not change (make another table)"

case $KIND in
  folder)
    [ "$DIRECTORY" = "$WANT_DIRECTORY" ] || fail "the folder's path is '$DIRECTORY', not $WANT_DIRECTORY"
    RESOURCE="folder.$DOMAIN.$SUBDOMAIN.$LAYER.$TABLE" ;;
  table)
    [ "$CATALOG" = "$LAKEHOUSE_CATALOG" ] || fail "catalog '$CATALOG': this platform's is $LAKEHOUSE_CATALOG"
    [ "$SCHEMA" = "${DOMAIN}_${SUBDOMAIN}_${LAYER}" ] || fail "schema '$SCHEMA' is not ${DOMAIN}_${SUBDOMAIN}_${LAYER}"
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

schema_exists() {
  [ "$(trino "SELECT count(*) FROM \"$CATALOG\".information_schema.schemata WHERE schema_name = $(sql_str "$SCHEMA")")" = "[1]" ]
}

table_exists() {
  [ "$(trino "SELECT count(*) FROM \"$CATALOG\".information_schema.tables WHERE table_schema = $(sql_str "$SCHEMA") AND table_name = $(sql_str "$TABLE")")" = "[1]" ]
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

unprovision_table() {
  local left files="" schema=""
  if ! schema_exists; then
    echo "table $NAMED: its schema is not there, nothing to drop"
    return 0
  fi
  trino "DROP TABLE IF EXISTS $Q_TABLE" >/dev/null
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

# one token for every statement of the run (trino runs in subshells, which
# would each log in again)
case $OPERATION in provision|unprovision) trino_login ;; esac

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
