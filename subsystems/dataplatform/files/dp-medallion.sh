#!/usr/bin/env bash
# dp-medallion.sh: the medallion lakehouses in Meta Catalog, from the command line.
#
#   dp-medallion.sh [-l <user>] load [<lakehouse.yaml> ...]
#   dp-medallion.sh [-l <user>] provision|authorize|reject <lakehouse>
#   dp-medallion.sh [-l <user>] [-y] unprovision <lakehouse>
#   dp-medallion.sh [-l <user>] status <lakehouse>
#
# load         the medallion model (subsystems/dataplatform/metacatalog/
#              medallion-model.yaml) unless Meta Catalog has it, then each
#              lakehouse file unless a lakehouse of that name is there. Once
#              loaded, a lakehouse lives in Meta Catalog: change it there (UI
#              or API), not in its file.
# provision    its Iceberg schemas and tables, through Trino (bronze, then
#              silver, then gold); again: adds the columns the model gained
# authorize    its grants (principals, permissions, grants of the root) for
#              every schema and table: Meta Catalog writes them into the
#              record of grants and gives Ceph's STS roles their directories;
#              iam-sync updates Trino's mapping within a minute. Members of
#              the admin group only (the gate refuses everyone else 403):
#              anyone of the realm may edit a policy, it counts once an
#              administrator authorizes it
# reject       withdraws all its grants (the admin group only, likewise)
# unprovision  DROPS its tables with their data, then its schemas (asks first;
#              -y does not)
# status       each schema and table: provisioned? authorized for whom?
#
# Logs you in to Keycloak (device login of the client data-cli; -l <user>: a
# fresh login of that user) unless MEDALLION_TOKEN_FILE names a file holding
# an access token; Meta Catalog takes any token of the realm. The token is
# kept in .secrets/sso-metacatalog.token while it is valid (an hour), so the
# next commands ask for no login; make dataplatform-logout deletes it. Needs
# curl, jq and python3.
set -euo pipefail
shopt -s inherit_errexit    # errexit inside $(...) as well

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
# shellcheck source=lib-keycloak.sh
. "$HERE/lib-keycloak.sh"
CLIENT=${CLIENT:-data-cli}
API="$METACATALOG_URL/metacatalog/v1"
MODEL=${MEDALLION_MODEL:-$ROOT/subsystems/dataplatform/metacatalog/medallion-model.yaml}
yes=false
usage() { sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
while getopts l:yh opt; do
  case $opt in l) export KEYCLOAK_USER=$OPTARG ;; y) yes=true ;; *) usage ;; esac
done
shift $((OPTIND - 1))
[ $# -ge 1 ] || usage
cmd=$1; shift
case $cmd in
  load) ;;
  provision|unprovision|authorize|reject|status) [ $# -eq 1 ] || usage ;;
  *) usage ;;
esac

fail() { echo "$*" >&2; exit 1; }

# The cached token while it has a minute left (and is the -l user's), else a login
CACHE=${SECRETS_DIR:-$ROOT/.secrets}/sso-metacatalog.token
TOKEN=""
if [ -z "${MEDALLION_TOKEN_FILE:-}" ] && [ -s "$CACHE" ]; then
  TOKEN=$(cat "$CACHE")
  jq -e --arg u "${KEYCLOAK_USER:-}" '.exp > (now + 60) and ($u == "" or .preferred_username == $u)' \
    <<<"$(jwt_claims "$TOKEN")" >/dev/null 2>&1 || TOKEN=""
fi
if [ -z "$TOKEN" ]; then
  TOKEN=$(keycloak_login "$CLIENT" "${MEDALLION_TOKEN_FILE:-}")
  if [ -z "${MEDALLION_TOKEN_FILE:-}" ]; then
    mkdir -p "$(dirname "$CACHE")"
    (umask 077; printf '%s\n' "$TOKEN" > "$CACHE")
  fi
fi

# api <method> <path> [curl args...]: sets BODY and CODE (not in a $(...):
# the variables would stay in the subshell)
api() {
  local method=$1 path=$2 out; shift 2
  out=$(mktemp)
  CODE=$(curl -sS -o "$out" -w '%{http_code}' -X "$method" \
    -K <(printf 'header = "Authorization: Bearer %s"\n' "$TOKEN") "$@" "$API$path") || CODE=000
  BODY=$(cat "$out"); rm -f "$out"
}

# The id of the lakehouse of that name, or nothing
lakehouse_id() { # <name>
  api GET /entity --get --data-urlencode entityTypeName=LakehouseType \
    --data-urlencode "queryPath=\$ ? (@.name == \"$1\")"
  [ "$CODE" = 200 ] || fail "Meta Catalog answered HTTP $CODE: $BODY"
  jq -r '.[0].id // empty' <<<"$BODY"
}

# The physical resources of a lakehouse, one JSON object per line
resources() { # <name>
  local t
  for t in MedallionSchemaType MedallionTableType; do
    api GET /entity --get --data-urlencode entityTypeName=$t \
      --data-urlencode "queryPath=\$ ? (@.lakehouse == \"$1\")"
    [ "$CODE" = 200 ] || fail "Meta Catalog answered HTTP $CODE: $BODY"
    jq -c '.[] | (.values | if type == "string" then fromjson else . end) + {id}' <<<"$BODY"
  done | jq -sc 'sort_by(.namespace, .table // "") | .[]'
}

load() {
  local names missing=0 present=0 n f lh
  names=$(awk '/^EntityTypes:/ { s = 1; next } /^[A-Za-z]/ { s = 0 } s && /^  - name:/ { print $3 }' "$MODEL")
  for n in $names; do
    api GET "/entity-type/$n"
    case $CODE in 200) present=$((present + 1)) ;; 404) missing=$((missing + 1)) ;; *) fail "Meta Catalog answered HTTP $CODE for entity type $n" ;; esac
  done
  if [ "$missing" -eq 0 ]; then
    echo "model: already in Meta Catalog ($present entity types)"
  elif [ "$present" -eq 0 ]; then
    api POST /bulk-creation -H 'Content-Type: application/octet-stream' --data-binary @"$MODEL"
    [ "$CODE" = 204 ] || fail "loading the model: HTTP $CODE: $BODY"
    echo "model: loaded ($missing entity types, their traits and mappings)"
  else
    fail "model: $present of $((present + missing)) entity types are in Meta Catalog already; finish or undo that by hand (Meta Catalog UI)"
  fi
  for f in "$@"; do
    lh=$(sed -n 's/^  name:[[:space:]]*"\{0,1\}\([a-z0-9_]*\)"\{0,1\}[[:space:]]*$/\1/p' "$f" | head -1)
    [ -n "$lh" ] || fail "$f: no lakehouse name (values.name)"
    if [ -n "$(lakehouse_id "$lh")" ]; then
      echo "lakehouse $lh: already in Meta Catalog (change it there; this file is not read again)"
      continue
    fi
    api POST /aggregate/yaml -H 'Content-Type: application/octet-stream' --data-binary @"$f"
    [ "$CODE" = 204 ] || fail "loading $f: HTTP $CODE: $BODY"
    echo "lakehouse $lh: loaded; its schemas and tables appear in a few seconds (mappings)"
  done
}

# Starts a procedure on the lakehouse and waits for its end
run() { # <operation> <name>
  local id sid status n=0
  id=$(lakehouse_id "$2"); [ -n "$id" ] || fail "no lakehouse $2 in Meta Catalog (load it first)"
  api POST "/aggregate/$id/$1?async=true"
  [ "$CODE" = 202 ] || fail "$1 $2: HTTP $CODE: $BODY"
  sid=$(jq -r .scheduleId <<<"$BODY")
  printf '%s %s ' "$1" "$2"
  while :; do
    api GET "/procedure/$sid"
    status=$(jq -r '.status // "?"' <<<"$BODY")
    [ "$status" = RUNNING ] || break
    n=$((n + 1)); [ $n -lt 400 ] || { echo; fail "still running after 20 min (schedule $sid)"; }
    printf .; sleep 3
  done
  echo " $status"
  status_of "$2" "$1"
  [ "$status" = SUCCEEDED ] || fail "$(jq -r '.error // empty' <<<"$BODY")"
}

# One line per schema and table: provisioning and authorization status, then
# the last line the script printed for the operation, or the grants in force
status_of() { # <name> [<operation>]
  local field rs
  case ${2:-} in provision|unprovision) field=provisioningResult ;; authorize|reject) field=authorizationResult ;; *) field="" ;; esac
  rs=$(resources "$1")
  [ -n "$rs" ] || { echo "(no schemas or tables yet: the mappings derive them a few seconds after a load)"; return 0; }
  jq -r --arg f "$field" '
    "\(.namespace)\(if .table then "." + .table else "" end)\t\(.provisioningStatus // "-")\t\(.authorizationStatus // "-")\t" +
    (if $f != "" then (.[$f] // "" | sub("^[A-Za-z]+ Medallion[A-Za-z]+ id=[0-9a-f-]+ \\(script task\\): "; "")
                       | split("\n") | map(select(. != "")) | last // "")
     else ((.effectiveGrants // []) | map(.principal.id + "=" + (.permissions | join("+"))) | join(", ")) end)' <<<"$rs" \
  | column -t -s $'\t'
}

case $cmd in
  load) load "$@" ;;
  status)
    [ -n "$(lakehouse_id "$1")" ] || fail "no lakehouse $1 in Meta Catalog"
    status_of "$1" ;;
  unprovision)
    if ! $yes; then
      read -r -p "This drops every table of the lakehouse $1 with its data. Type its name to go on: " answer
      [ "$answer" = "$1" ] || fail "not confirmed"
    fi
    run unprovision "$1" ;;
  *) run "$cmd" "$1" ;;
esac
