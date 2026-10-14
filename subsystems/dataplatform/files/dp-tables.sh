#!/usr/bin/env bash
# dp-tables.sh: the platform's tables in Meta Catalog, from the command line.
#
#   dp-tables.sh [-l <user>] load [<file.yaml> ...]
#   dp-tables.sh [-l <user>] list [<tables>]
#   dp-tables.sh [-l <user>] status <tables>
#   dp-tables.sh [-l <user>] provision|authorize|reject <tables>
#   dp-tables.sh [-l <user>] [-y] unprovision <tables>
#   dp-tables.sh [-l <user>] [-y] delete <tables>
#   dp-tables.sh [-l <user>] [-y] unload-model <model.yaml>
#
# <tables> is a table, <domain>/<subdomain>/<layer>/<name>, or every table
# under what comes first: <domain>, <domain>/<subdomain>,
# <domain>/<subdomain>/<layer>.
#
# load         the model (subsystems/dataplatform/metacatalog/table-model.yaml)
#              unless Meta Catalog has it, then each file. A file holds
#              domains with their subdomains (DomainType: a domain that is
#              there gets the subdomains it lacks) or tables (TableType, one
#              document each, `dependsOn` for their lineage: all of a file's
#              tables are loaded together, or none when they are all there).
#              A table's subdomain must be there: load its domain first. Each
#              table is then put under its subdomain. Once loaded, a table
#              lives in Meta Catalog: change it there (UI or API), not in
#              its file.
# list         the tables, with what is provisioned and authorized
# status       each table's folder and metastore table: provisioned?
#              authorized for whom?
# provision    the table's folder in the bucket, then the table in the Hive
#              Metastore (through Trino), in its format; again: adds the
#              columns its schema gained
# authorize    its grants (principals, permissions, grants of the table): on
#              the folder they become Ceph's STS roles at once, on the table
#              Trino's rules in the Open Policy Agent within a minute.
#              Members of the admin group only (the gate refuses everyone
#              else 403): anyone who may use Meta Catalog may edit a policy,
#              it counts once an administrator authorizes it
# reject       withdraws all its grants (the admin group only, likewise)
# unprovision  DROPS the table and DELETES the files in its folder (asks
#              first; -y does not)
# delete       takes an unprovisioned, rejected table out of Meta Catalog,
#              with its place under its subdomain and its lineage (asks
#              first; -y does not)
# unload-model takes a model out of Meta Catalog: the mappings, entity types,
#              relationships and traits a model file defines (asks first; -y
#              does not). Nothing of its types may be left in the catalog:
#              delete those entities first. For a model that is replaced by
#              another, or given up.
#
# Logs you in to Keycloak (device login of the client data-cli; -l <user>: a
# fresh login of that user) unless TABLES_TOKEN_FILE names a file holding an
# access token; Meta Catalog takes any token of the realm. The token is kept
# in .secrets/sso-metacatalog.token while it is valid (an hour), so the next
# commands ask for no login; make dataplatform-logout deletes it. Needs curl,
# jq and python3 (load: with PyYAML, requirements.txt; PYTHON names another
# interpreter that has it).
set -euo pipefail
shopt -s inherit_errexit    # errexit inside $(...) as well

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
# shellcheck source=lib-keycloak.sh
. "$HERE/lib-keycloak.sh"
CLIENT=${CLIENT:-data-cli}
API="$METACATALOG_URL/metacatalog/v1"
MODEL=${TABLES_MODEL:-$ROOT/subsystems/dataplatform/metacatalog/table-model.yaml}
yes=false
usage() { sed -n '2,53p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
while getopts l:yh opt; do
  case $opt in l) export KEYCLOAK_USER=$OPTARG ;; y) yes=true ;; *) usage ;; esac
done
shift $((OPTIND - 1))
[ $# -ge 1 ] || usage
cmd=$1; shift
case $cmd in
  load) ;;
  list) [ $# -le 1 ] || usage ;;
  provision|unprovision|authorize|reject|status|delete|unload-model) [ $# -eq 1 ] || usage ;;
  *) usage ;;
esac

fail() { echo "$*" >&2; exit 1; }

# The cached token while it has a minute left (and is the -l user's), else a login
CACHE=${SECRETS_DIR:-$ROOT/.secrets}/sso-metacatalog.token
TOKEN=""
if [ -z "${TABLES_TOKEN_FILE:-}" ] && [ -s "$CACHE" ]; then
  TOKEN=$(cat "$CACHE")
  jq -e --arg u "${KEYCLOAK_USER:-}" '.exp > (now + 60) and ($u == "" or .preferred_username == $u)' \
    <<<"$(jwt_claims "$TOKEN")" >/dev/null 2>&1 || TOKEN=""
fi
if [ -z "$TOKEN" ]; then
  TOKEN=$(keycloak_login "$CLIENT" "${TABLES_TOKEN_FILE:-}")
  if [ -z "${TABLES_TOKEN_FILE:-}" ]; then
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

# Entities of a type as {id, values} lines; <filter>: a PostgreSQL jsonpath,
# or `all` (never an empty one: that is a filter that failed to be made)
entities() { # <type> <filter>
  [ -n "${2:-}" ] || fail "entities $1: no filter"
  if [ "$2" = all ]; then
    api GET /entity --get --data-urlencode "entityTypeName=$1"
  else
    api GET /entity --get --data-urlencode "entityTypeName=$1" --data-urlencode "queryPath=$2"
  fi
  [ "$CODE" = 200 ] || fail "Meta Catalog answered HTTP $CODE: $BODY"
  jq -c '.[] | {id, values: (.values | if type == "string" then fromjson else . end)}' <<<"$BODY"
}

# The entities one is linked to, as {id, type, values} lines
linked() { # <id> <relation>
  api GET "/entity/link/$1/$2"
  [ "$CODE" = 200 ] || fail "Meta Catalog answered HTTP $CODE: $BODY"
  jq -c '.[] | {id, type: .entityType, values: (.values | if type == "string" then fromjson else . end)}' <<<"$BODY"
}

link() { # <source id> <relation> <target id>
  api POST /entity/link -H 'Content-Type: application/json' \
    -d "$(jq -cn --arg s "$1" --arg r "$2" --arg t "$3" '{sourceEntityId: $s, relationshipTypeName: $r, targetEntityId: $t}')"
  [ "$CODE" = 204 ] || fail "linking $1 $2 $3: HTTP $CODE: $BODY"
}

# The jsonpath that selects the tables of a reference: domain[/subdomain[/layer[/name]]]
selector() { # <reference>
  local d s l n extra
  IFS=/ read -r d s l n extra <<<"$1"
  [[ -z ${extra:-} && $d =~ ^[a-z][a-z0-9]*$ && ${s:-a} =~ ^[a-z][a-z0-9]*$ && ${l:-gold} =~ ^(bronze|silver|gold)$ && ${n:-a} =~ ^[a-z][a-z0-9_]*$ ]] \
    || fail "'$1' is not <domain>[/<subdomain>[/<layer>[/<table>]]]"
  printf '$ ? (@.domain == "%s"%s%s%s)' "$d" "${s:+ && @.subdomain == \"$s\"}" "${l:+ && @.layer == \"$l\"}" "${n:+ && @.name == \"$n\"}"
}

# The tables of a reference as {id, ref, values} lines, sorted; fails on none
tables() { # <reference>
  local filter found
  # on its own line: a reference that is refused must stop here, not go on
  # as no filter at all (every table)
  filter=$(selector "$1")
  found=$(entities TableType "$filter" \
    | jq -sc 'map(. + {ref: (.values | "\(.domain)/\(.subdomain)/\(.layer)/\(.name)")}) | sort_by(.ref) | .[]')
  [ -n "$found" ] || fail "no table $1 in Meta Catalog (dp-tables.sh list shows them)"
  echo "$found"
}

# The two physical resources of a table (values), the folder first
resources() { # <domain> <subdomain> <layer> <name>
  local t
  for t in S3FolderType HiveTableType; do
    entities "$t" "$(printf '$ ? (@.domain == "%s" && @.subdomain == "%s" && @.layer == "%s" && @.table == "%s")' "$@")" | jq -c .values
  done
}

# ---------------------------------------------------------------- load ----
# The documents of a file as JSON lines: {kind: domain|table, ...}
documents() { # <file>
  "${PYTHON:-python3}" - "$1" <<'PY'
import json, sys
try:
    import yaml
except ImportError:
    sys.exit("PyYAML is needed to read the files: pip3 install pyyaml (requirements.txt)")
for entry in yaml.safe_load_all(open(sys.argv[1])):
    if not entry:
        continue
    v = entry.get("values") or {}
    if entry.get("entityType") == "DomainType":
        print(json.dumps({"kind": "domain", "name": v.get("name"), "values": v,
                          "subdomains": [p.get("values") or {} for p in entry.get("parts") or []]}))
    elif entry.get("entityType") == "TableType":
        print(json.dumps({"kind": "table", "ref": "/".join(str(v.get(k)) for k in ("domain", "subdomain", "layer", "name")),
                          "domain": v.get("domain"), "subdomain": v.get("subdomain"),
                          "schema": any(p.get("entityType") == "TableSchemaType" for p in entry.get("parts") or [])}))
    else:
        sys.exit(f"{sys.argv[1]}: a document of type {entry.get('entityType')!r}: DomainType or TableType")
PY
}

# The id of a subdomain of a domain, or nothing
subdomain_id() { # <domain> <subdomain>
  local d
  d=$(entities DomainType "$(printf '$ ? (@.name == "%s")' "$1")" | jq -r .id | head -1)
  [ -n "$d" ] || return 0
  linked "$d" HAS_PART | jq -r --arg n "$2" 'select(.type == "SubdomainType" and .values.name == $n) | .id' | head -1
}

load_model() {
  local names missing=0 present=0 n
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
    echo "model: loaded ($missing entity types, their traits, relationships and mappings)"
  else
    fail "model: $present of $((present + missing)) entity types are in Meta Catalog already; finish or undo that by hand (Meta Catalog UI)"
  fi
}

load_domains() { # <file> <documents>
  local entry name id sub sid
  while IFS= read -r entry; do
    name=$(jq -r .name <<<"$entry")
    id=$(entities DomainType "$(printf '$ ? (@.name == "%s")' "$name")" | jq -r .id | head -1)
    if [ -z "$id" ]; then
      api POST /entity -H 'Content-Type: application/json' \
        -d "$(jq -c '{entityType: "DomainType", values: (.values | tojson)}' <<<"$entry")"
      [ "$CODE" = 200 ] || fail "$1: domain $name: HTTP $CODE: $BODY"
      id=$BODY
      echo "domain $name: loaded"
    fi
    while IFS= read -r sub; do
      [ -n "$sub" ] || continue
      if [ -n "$(linked "$id" HAS_PART | jq -r --arg n "$(jq -r .name <<<"$sub")" 'select(.type == "SubdomainType" and .values.name == $n) | .id')" ]; then
        continue
      fi
      api POST /entity -H 'Content-Type: application/json' -d "$(jq -c '{entityType: "SubdomainType", values: tojson}' <<<"$sub")"
      [ "$CODE" = 200 ] || fail "$1: subdomain $name/$(jq -r .name <<<"$sub"): HTTP $CODE: $BODY"
      sid=$BODY
      link "$id" HAS_PART "$sid"
      echo "subdomain $name/$(jq -r .name <<<"$sub"): loaded"
    done < <(jq -c '.subdomains[]' <<<"$entry")
  done <<<"$2"
}

load_tables() { # <file> <documents>
  local entry ref filter found there=0 total=0 d s sid id n have want
  while IFS= read -r entry; do
    total=$((total + 1))
    ref=$(jq -r .ref <<<"$entry")
    filter=$(selector "$ref")
    found=$(entities TableType "$filter")
    [ -z "$found" ] || there=$((there + 1))
    d=$(jq -r .domain <<<"$entry"); s=$(jq -r .subdomain <<<"$entry")
    [ -n "$(subdomain_id "$d" "$s")" ] || fail "$1: table $ref: Meta Catalog has no subdomain $d/$s (load its domain first)"
  done <<<"$2"
  if [ "$there" -eq "$total" ]; then
    echo "$1: its $total tables are in Meta Catalog already (change them there; this file is not read again)"
    return 0
  fi
  [ "$there" -eq 0 ] || fail "$1: $there of its $total tables are in Meta Catalog already: load the others from a file of their own"
  api POST /aggregate/yaml -H 'Content-Type: application/octet-stream' --data-binary @"$1"
  [ "$CODE" = 204 ] || fail "loading $1: HTTP $CODE: $BODY"
  while IFS= read -r entry; do
    ref=$(jq -r .ref <<<"$entry")
    id=$(tables "$ref" | jq -r .id)
    sid=$(subdomain_id "$(jq -r .domain <<<"$entry")" "$(jq -r .subdomain <<<"$entry")")
    link "$sid" HAS_PART "$id"
    # the mappings derive the folder, and the metastore table of a schema
    want=$(jq -r 'if .schema then 2 else 1 end' <<<"$entry")
    for n in $(seq 1 30); do
      # shellcheck disable=SC2046
      have=$(resources $(tr / ' ' <<<"$ref") | wc -l | tr -d ' ')
      [ "$have" -lt "$want" ] || break
      sleep 2
    done
    [ "$have" -ge "$want" ] || fail "table $ref: loaded, but Meta Catalog derived $have of its $want resources (its log says why). Delete it (dp-tables.sh delete $ref) and load it again"
    echo "table $ref: loaded"
  done <<<"$2"
}

load() {
  local f entries
  load_model
  for f in "$@"; do
    [ -f "$f" ] || fail "no file $f"
    entries=$(documents "$f")
    [ -n "$entries" ] || fail "$f: no document"
    if jq -se 'all(.kind == "domain")' <<<"$entries" >/dev/null; then
      load_domains "$f" "$entries"
    elif jq -se 'all(.kind == "table")' <<<"$entries" >/dev/null; then
      load_tables "$f" "$entries"
    else
      fail "$f: domains and tables in one file: one kind per file"
    fi
  done
}

# ---------------------------------------------------------------- a model, out ----
# What a model file defines, as one JSON object: the mappings (source and
# target type), the entity types and the traits (those that inherit first,
# so that each goes before what it is built on), the relationships.
model_of() { # <file>
  "${PYTHON:-python3}" - "$1" <<'PY'
import json, sys
try:
    import yaml
except ImportError:
    sys.exit("PyYAML is needed to read the file: pip3 install pyyaml (requirements.txt)")
sections = {}
for part in yaml.safe_load_all(open(sys.argv[1])):
    for key, value in (part or {}).items():
        sections.setdefault(key, value or [])

def heirs_first(items):
    """names, each after every item that inherits from it"""
    names, father = [i["name"] for i in items], {i["name"]: i.get("inheritsFrom") for i in items}
    def depth(n):
        return 0 if father.get(n) not in father else 1 + depth(father[n])
    return sorted(names, key=lambda n: -depth(n))

print(json.dumps({
    "mappings": [[m["sourceEntityType"], m["targetEntityType"]] for m in sections.get("Mappings", [])],
    "types": heirs_first(sections.get("EntityTypes", [])),
    "relationships": [[r["sourceTrait"], r["relationshipType"], r["targetTrait"]] for r in sections.get("Relationships", [])],
    "traits": heirs_first(sections.get("Traits", []))}))
PY
}

unload_model() { # <file>
  local model t left id pair rel gone=0
  [ -f "$1" ] || fail "no file $1"
  model=$(model_of "$1")
  # nothing of the model's types may be left: its entities go first
  for t in $(jq -r '.types[]' <<<"$model"); do
    api GET /entity --get --data-urlencode "entityTypeName=$t"
    case $CODE in
      200) left=$(jq length <<<"$BODY")
           [ "$left" -eq 0 ] || fail "$left entities of type $t are in Meta Catalog: delete them first (an aggregate goes with all its parts and resources)" ;;
      400|404) ;;                               # the type is not there
      *) fail "Meta Catalog answered HTTP $CODE for type $t: $BODY" ;;
    esac
  done
  confirm "This takes the model of $1 out of Meta Catalog: $(jq -r '"\(.mappings | length) mappings, \(.types | length) entity types, \(.relationships | length) relationships, \(.traits | length) traits"' <<<"$model")."$'\n' "$(basename "$1")"
  api GET /mapping
  [ "$CODE" = 200 ] || fail "Meta Catalog answered HTTP $CODE: $BODY"
  while IFS= read -r id; do
    [ -n "$id" ] || continue
    api DELETE "/mapping/$id"
    [ "$CODE" = 204 ] || [ "$CODE" = 200 ] || fail "deleting a mapping: HTTP $CODE: $BODY"
    gone=$((gone + 1))
  done < <(jq -r --argjson m "$(jq -c .mappings <<<"$model")" '.[] | select([.sourceEntityType, .targetEntityType] as $p | $m | index([$p])) | .id' <<<"$BODY")
  for t in $(jq -r '.types[]' <<<"$model"); do
    api DELETE "/entity-type/$t"
    case $CODE in 200|204) gone=$((gone + 1)) ;; 404) ;; *) fail "deleting the entity type $t: HTTP $CODE: $BODY" ;; esac
  done
  while IFS=$'\t' read -r pair rel t; do
    [ -n "$pair" ] || continue
    api DELETE "/trait/link/$pair/$rel/$t"
    case $CODE in 200|204) gone=$((gone + 1)) ;; 400|404) ;; *) fail "deleting the relationship $pair $rel $t: HTTP $CODE: $BODY" ;; esac
  done < <(jq -r '.relationships[] | @tsv' <<<"$model")
  for t in $(jq -r '.traits[]' <<<"$model"); do
    api DELETE "/trait/$t"
    case $CODE in 200|204) gone=$((gone + 1)) ;; 404) ;; *) fail "deleting the trait $t: HTTP $CODE: $BODY" ;; esac
  done
  echo "model of $(basename "$1"): $gone definitions taken out of Meta Catalog"
}

# ---------------------------------------------------------------- procedures ----
# What the script printed last for a resource, or the grants in force
summary() { # <resource values> <result field | "">
  jq -r --arg f "$2" '
    if $f != "" then (.[$f] // "" | sub("^[A-Za-z]+ [A-Za-z0-9]+ id=[0-9a-f-]+ \\(script task\\): "; "")
                      | sub("^Script [^ ]+ exited with status [0-9]+: "; "") | split("\n") | map(select(. != "")) | last // "")
    else ((.effectiveGrants // []) | map(.principal.id + "=" + (.permissions | join("+"))) | join(", ")) end' <<<"$1"
}

# One line per resource of a table: provisioning and authorization status,
# then the last line the script printed for the operation, or the grants
status_of() { # <table {ref, values}> [<operation>]
  local field r ref
  case ${2:-} in provision|unprovision) field=provisioningResult ;; authorize|reject) field=authorizationResult ;; *) field="" ;; esac
  ref=$(jq -r .ref <<<"$1")
  # shellcheck disable=SC2046
  resources $(tr / ' ' <<<"$ref") | while IFS= read -r r; do
    printf '%s\t%s\t%s\t%s\t%s\n' "$ref" "$(jq -r 'if .kind == "folder" then "folder" else .format + " table" end' <<<"$r")" \
      "$(jq -r '.provisioningStatus // "-"' <<<"$r")" "$(jq -r '.authorizationStatus // "-"' <<<"$r")" "$(summary "$r" "$field")"
  done
}

# Starts a procedure on a table and waits for its end
run() { # <operation> <table {id, ref, values}>
  local id ref sid status n=0
  id=$(jq -r .id <<<"$2"); ref=$(jq -r .ref <<<"$2")
  api POST "/aggregate/$id/$1?async=true"
  [ "$CODE" = 202 ] || fail "$1 $ref: HTTP $CODE: $BODY"
  sid=$(jq -r .scheduleId <<<"$BODY")
  printf '%s %s ' "$1" "$ref"
  while :; do
    api GET "/procedure/$sid"
    status=$(jq -r '.status // "?"' <<<"$BODY")
    [ "$status" = RUNNING ] || break
    n=$((n + 1)); [ $n -lt 400 ] || { echo; fail "still running after 20 min (schedule $sid)"; }
    printf .; sleep 3
  done
  echo " $status"
  status_of "$2" "$1" | cut -f2- | sed 's/^/  /' | column -t -s $'\t'
  [ "$status" = SUCCEEDED ]
}

# Takes a table out of Meta Catalog: only one that holds nothing any more
delete() { # <table {id, ref, values}>
  local id ref r other
  id=$(jq -r .id <<<"$1"); ref=$(jq -r .ref <<<"$1")
  # shellcheck disable=SC2046
  r=$(resources $(tr / ' ' <<<"$ref") | jq -sc .)
  jq -e 'all(.provisioningStatus != "PROVISIONED")' <<<"$r" >/dev/null || fail "table $ref is provisioned: unprovision it first (its files and its entry in the metastore would stay behind)"
  jq -e 'all(.authorizationStatus != "AUTHORIZED")' <<<"$r" >/dev/null || fail "table $ref is authorized: reject it first (its grants would stay in force)"
  for other in $(linked "$id" IS_PART_OF | jq -r 'select(.type == "SubdomainType") | .id'); do
    api DELETE "/entity/link/$other/HAS_PART/$id"; [ "$CODE" = 204 ] || fail "taking $ref out of its subdomain: HTTP $CODE: $BODY"
  done
  for other in $(linked "$id" DEPENDS_ON | jq -r .id); do
    api DELETE "/entity/link/$id/DEPENDS_ON/$other"; [ "$CODE" = 204 ] || fail "removing the lineage of $ref: HTTP $CODE: $BODY"
  done
  for other in $(linked "$id" IS_REQUIRED_BY | jq -r .id); do
    api DELETE "/entity/link/$other/DEPENDS_ON/$id"; [ "$CODE" = 204 ] || fail "removing the lineage of $ref: HTTP $CODE: $BODY"
  done
  api DELETE "/aggregate/$id"
  [ "$CODE" = 204 ] || fail "deleting $ref: HTTP $CODE: $BODY"
  echo "table $ref: deleted from Meta Catalog"
}

confirm() { # <what happens> <reference>
  $yes && return 0
  read -r -p "$1 Type $2 to go on: " answer
  [ "$answer" = "$2" ] || fail "not confirmed"
}

case $cmd in
  load) load "$@" ;;
  unload-model) unload_model "$1" ;;
  list)
    found=$(if [ $# -eq 1 ]; then tables "$1"; else
              entities TableType all | jq -sc 'map(. + {ref: (.values | "\(.domain)/\(.subdomain)/\(.layer)/\(.name)")}) | sort_by(.ref) | .[]'; fi)
    [ -n "$found" ] || { echo "(no table in Meta Catalog)"; exit 0; }
    { printf 'TABLE\tFORMAT\tPROVISIONED\tAUTHORIZED\n'
      jq -r '[.ref, .values.format, (.values.provisioningStatus // "-"), (.values.authorizationStatus // "-")] | @tsv' <<<"$found"
    } | column -t -s $'\t' ;;
  status)
    found=$(tables "$1")
    { printf 'TABLE\tRESOURCE\tPROVISIONED\tAUTHORIZED\tGRANTS\n'
      while IFS= read -r t; do status_of "$t"; done <<<"$found"
    } | column -t -s $'\t' ;;
  provision|authorize|reject|unprovision)
    found=$(tables "$1")
    [ "$cmd" != unprovision ] || confirm "This drops $(wc -l <<<"$found" | tr -d ' ') table(s) and deletes the files in their folders:"$'\n'"$(jq -r '"  " + .ref' <<<"$found")"$'\n' "$1"
    failed=0
    while IFS= read -r t; do run "$cmd" "$t" || failed=$((failed + 1)); done <<<"$found"
    [ "$failed" -eq 0 ] || fail "$failed table(s) failed" ;;
  delete)
    found=$(tables "$1")
    confirm "This takes $(wc -l <<<"$found" | tr -d ' ') table(s) out of Meta Catalog:"$'\n'"$(jq -r '"  " + .ref' <<<"$found")"$'\n' "$1"
    while IFS= read -r t; do delete "$t"; done <<<"$found" ;;
esac
