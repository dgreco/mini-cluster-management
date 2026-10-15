#!/usr/bin/env bash
# dp-tables.sh: the platform's tables in Meta Catalog, from the command line.
#
#   dp-tables.sh [-l <user>] load [<file.yaml> ...]
#   dp-tables.sh [-l <user>] domains
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
#              unless Meta Catalog has it, then each file of tables
#              (TableType, one document each, `dependsOn` for their lineage:
#              all of a file's tables are loaded together, or none when they
#              are all there). A table's domain and subdomain must be a pair
#              of the trait DomainMember in Meta Catalog (its x-subdomains;
#              Meta Catalog checks each against its enum, not the pair). Once
#              loaded, a table lives in Meta Catalog: change it there (UI or
#              API), not in its file.
# domains      sends the model file's trait DomainMember (the domains and
#              their subdomains) to Meta Catalog, once its three lists agree:
#              a new version of the trait, then of TableType, whose schema is
#              made from its traits when it is versioned. Tables keep the
#              version they were made with. Nothing when Meta Catalog has it
#              already.
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
#              with its schema, its two resources and its lineage (asks
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
# the header, from its second line to the first that is not a comment
usage() { awk 'NR > 1 && !/^#/ { exit } NR > 1' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
while getopts l:yh opt; do
  case $opt in l) export KEYCLOAK_USER=$OPTARG ;; y) yes=true ;; *) usage ;; esac
done
shift $((OPTIND - 1))
[ $# -ge 1 ] || usage
cmd=$1; shift
case $cmd in
  load) ;;
  domains) [ $# -eq 0 ] || usage ;;
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
# The tables of a file as JSON lines: {ref, domain, subdomain, schema}
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
    if entry.get("entityType") != "TableType":
        sys.exit(f"{sys.argv[1]}: a document of type {entry.get('entityType')!r}: only TableType "
                 "(domains and subdomains are the model's: its trait DomainMember)")
    print(json.dumps({"ref": "/".join(str(v.get(k)) for k in ("domain", "subdomain", "layer", "name")),
                      "domain": v.get("domain"), "subdomain": v.get("subdomain"),
                      "schema": any(p.get("entityType") == "TableSchemaType" for p in entry.get("parts") or [])}))
PY
}

# The pairs a table may name: x-subdomains of the trait DomainMember as Meta
# Catalog has it ({domain: [subdomains]}; the provisioning script asks the
# same), in PAIRS; once per run
PAIRS=""
live_pairs() {
  [ -z "$PAIRS" ] || return 0
  api GET /trait/DomainMember
  [ "$CODE" = 200 ] || fail "Meta Catalog has no trait DomainMember (HTTP $CODE): load the model first (dp-tables.sh load)"
  PAIRS=$(jq -c '.schema | if type == "string" then fromjson else . end | .properties.domain["x-subdomains"] | objects' <<<"$BODY")
  [ -n "$PAIRS" ] || fail "the trait DomainMember in Meta Catalog has no x-subdomains: dp-tables.sh domains sends the model's"
}

# Fails unless <domain>/<subdomain> is one of them
known_pair() { # <what> <domain> <subdomain>
  local own
  live_pairs
  own=$(jq -r --arg d "$2" 'if has($d) then .[$d] | join(", ") else "" end' <<<"$PAIRS")
  [ -n "$own" ] || fail "$1: domain '$2' is not one of the model's in Meta Catalog ($(jq -r 'keys | join(", ")' <<<"$PAIRS"))"
  jq -e --arg d "$2" --arg s "$3" '.[$d] | index($s) != null' <<<"$PAIRS" >/dev/null \
    || fail "$1: subdomain '$3' is not one of the domain $2's ($own)"
}

# The model file's trait or entity type as Meta Catalog takes a new version
# of it: its own schema (JSON text), its father and, for a type, its traits.
# A new version must carry these, not the schema Meta Catalog answers with
# (merged from the traits).
model_part() { # <Traits|EntityTypes> <name>
  "${PYTHON:-python3}" - "$MODEL" "$1" "$2" <<'PY'
import json, sys
try:
    import yaml
except ImportError:
    sys.exit("PyYAML is needed to read the model: pip3 install pyyaml (requirements.txt)")
model, section, name = sys.argv[1:4]
found = {}
for part in yaml.safe_load_all(open(model)):
    for t in (part or {}).get(section) or []:
        found[t["name"]] = t
t = found.get(name) or sys.exit(f"{model}: no {section} entry {name}")
out = {"name": t["name"], "schema": json.dumps(t.get("schema") or {"type": "object", "properties": {}}),
       "inheritsFrom": t.get("inheritsFrom")}
if section == "EntityTypes":
    out["traits"] = t.get("traits") or []
print(json.dumps(out))
PY
}

# What is wrong with a DomainMember schema, a line each (nothing: it is
# right). domain.enum lists the domains, domain.x-subdomains each domain's
# own subdomains, subdomain.enum every one of those once; every name fits a
# folder and a schema name; the smoke test's domain smoke has its test.
domain_faults() { # <schema JSON>
  jq -r '
    def name: type == "string" and test("^[a-z][a-z0-9]{0,23}$");
    (.properties.domain.enum // null) as $d
    | (.properties.domain["x-subdomains"] // null) as $x
    | (.properties.subdomain.enum // null) as $s
    | if ($d | type) != "array" or ($x | type) != "object" or ($s | type) != "array" then
        "it needs domain.enum (a list), domain.x-subdomains ({domain: [subdomains]}) and subdomain.enum (a list)"
      else
        ([$x[][]?] | unique) as $all
        | (($d + $s + $all) | map(select(name | not)) | unique | .[]
            | "\(tojson): names are lower-case letters and digits, a letter first, at most 24"),
          ($x | to_entries[] | select((.value | type) != "array" or (.value | length) == 0)
            | "domain \(.key): x-subdomains needs a list of its subdomains"),
          (if ($d | sort) != ($x | keys) then
             "domain.enum (\($d | join(", "))) is not the domains of x-subdomains (\($x | keys_unsorted | join(", ")))"
           else empty end),
          (if ($s | sort) != $all then
             "subdomain.enum (\($s | join(", "))) is not every subdomain of x-subdomains once (\($all | join(", ")))"
           else empty end),
          ($d | map(select(IN("if", "then", "else", "not"))) | .[]
            | "domain \(.): Meta Catalog refuses a schema with this keyword in it, as a key of x-subdomains too"),
          (if ($x.smoke // [] | index("test")) == null then
             "the domain smoke with its subdomain test is the smoke test'"'"'s: keep it" else empty end)
      end' <<<"$1"
}

# Fails unless the model file's trait DomainMember is right (domain_faults)
model_domains() {
  local faults
  faults=$(domain_faults "$(model_part Traits DomainMember | jq -r .schema)")
  [ -z "$faults" ] || fail "$MODEL, trait DomainMember:"$'\n'"$faults"
}

# Whether two schemas say the same of domain and subdomain (Meta Catalog
# answers with the keys of an object in another order)
same_domains() { # <schema JSON> <schema JSON>
  jq -en --argjson a "$1" --argjson b "$2" '[$a, $b] | map(.properties | {domain, subdomain}) | .[0] == .[1]' >/dev/null
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
    api GET /trait/DomainMember
    if [ "$CODE" = 200 ] && ! same_domains "$(jq -c '.schema | if type == "string" then fromjson else . end' <<<"$BODY")" \
         "$(model_part Traits DomainMember | jq -r .schema)"; then
      echo "model: its domains (the trait DomainMember) are not those of $MODEL: tables are checked against Meta Catalog's (dp-tables.sh domains sends the file's)"
    fi
  elif [ "$present" -eq 0 ]; then
    model_domains
    api POST /bulk-creation -H 'Content-Type: application/octet-stream' --data-binary @"$MODEL"
    [ "$CODE" = 204 ] || fail "loading the model: HTTP $CODE: $BODY"
    echo "model: loaded ($missing entity types, their traits, relationships and mappings)"
  else
    fail "model: $present of $((present + missing)) entity types are in Meta Catalog already; finish or undo that by hand (Meta Catalog UI)"
  fi
}

load_tables() { # <file> <documents>
  local entry ref filter found there=0 total=0 n have want
  while IFS= read -r entry; do
    total=$((total + 1))
    ref=$(jq -r .ref <<<"$entry")
    filter=$(selector "$ref")
    found=$(entities TableType "$filter")
    [ -z "$found" ] || there=$((there + 1))
    known_pair "$1: table $ref" "$(jq -r .domain <<<"$entry")" "$(jq -r .subdomain <<<"$entry")"
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
    load_tables "$f" "$entries"
  done
}

# ---------------------------------------------------------------- domains ----
domains() {
  local want live body merged type versioned=false
  model_domains
  want=$(model_part Traits DomainMember)
  api GET /trait/DomainMember
  [ "$CODE" = 200 ] || fail "Meta Catalog has no trait DomainMember (HTTP $CODE): load the model first (dp-tables.sh load)"
  live=$(jq -c '.schema | if type == "string" then fromjson else . end' <<<"$BODY")
  if ! same_domains "$live" "$(jq -r .schema <<<"$want")"; then
    body=$(jq -c 'if .inheritsFrom == null then del(.inheritsFrom) else . end' <<<"$want")
    api POST /trait/DomainMember/versions -H 'Content-Type: application/json' -d "$body"
    [[ $CODE == 20[01] ]] || fail "a new version of the trait DomainMember: HTTP $CODE: $BODY"
    echo "trait DomainMember: version $(jq -r '.version // "?"' <<<"$BODY"), domains $(jq -r '.schema | fromjson | .properties.domain.enum | join(", ")' <<<"$want")"
    versioned=true
  fi
  # a type's schema is merged from its traits' when it is made or versioned:
  # TableType takes the new trait with a version of its own
  api GET /entity-type/TableType
  [ "$CODE" = 200 ] || fail "Meta Catalog has no entity type TableType (HTTP $CODE): load the model first (dp-tables.sh load)"
  merged=$(jq -c '.schema | if type == "string" then fromjson else . end' <<<"$BODY")
  if $versioned || ! same_domains "$merged" "$(jq -r .schema <<<"$want")"; then
    type=$(model_part EntityTypes TableType)
    body=$(jq -c 'if .inheritsFrom == null then del(.inheritsFrom) else . end' <<<"$type")
    api POST /entity-type/TableType/versions -H 'Content-Type: application/json' -d "$body"
    [[ $CODE == 20[01] ]] || fail "a new version of TableType: HTTP $CODE: $BODY (the trait is up to date: dp-tables.sh domains again)"
    echo "entity type TableType: version $(jq -r '.version // "?"' <<<"$BODY"): new tables take these domains; tables made before keep theirs"
  else
    echo "domains: Meta Catalog has the model's already ($(jq -r '.schema | fromjson | .properties.domain.enum | join(", ")' <<<"$want"))"
  fi
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
  domains) domains ;;
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
