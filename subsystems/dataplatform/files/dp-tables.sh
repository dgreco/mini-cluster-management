#!/usr/bin/env bash
# dp-tables.sh: the platform's domains and tables in Meta Catalog, from the
# command line (make dataplatform-tables ARGS="...").
#
#   dp-tables.sh check [<tables.yaml>]                   the model and the file, offline (no login)
#   dp-tables.sh [-l <user>] [-y] model                  the model file's changes -> Meta Catalog (asks first)
#   dp-tables.sh [-l <user>] [-n] [-y] sync [<tables.yaml>]
#   dp-tables.sh [-l <user>] load <file.yaml> [...]      the model if missing, then what the files add
#   dp-tables.sh [-l <user>] list [<ref>]
#   dp-tables.sh [-l <user>] status <ref>
#   dp-tables.sh [-l <user>] provision|authorize|reject <ref>
#   dp-tables.sh [-l <user>] [-y] unprovision|delete <ref>
#   dp-tables.sh [-l <user>] [-n] [-y] remove [<file>]     every domain and table of the file out of Meta Catalog
#   dp-tables.sh [-l <user>] [-y] unload-model <model.yaml>
#
# <ref> is a domain (sales), a layer of it (sales/silver) or a table
# (sales/silver/orders_v1: its name with its version). The tables are
# metacatalog/tables/tables.yaml's, the model metacatalog/table-model.yaml's
# (TABLES_MODEL names another).
#
# sync brings every domain but the smoke test's (TABLES_SYNC_DOMAINS="d1 d2":
# those only) to the file: makes what is new, updates what changed (a new
# version in Meta Catalog), provisions and authorizes, and REMOVES what the
# file no longer has (rejected, unprovisioned: DROPPED with its files, and
# deleted). It shows the steps and asks (-y does not; -n only shows).
# Authorizing needs the admin group: -l admin. Every command:
# files/tables.py's header.
#
# Logs you in to Keycloak (device login of the client data-cli; -l <user>: a
# fresh login of that user) unless TABLES_TOKEN_FILE names a file holding an
# access token; Meta Catalog takes any token of the realm. The token is kept
# in .secrets/sso-metacatalog.token while it is valid (an hour), so the next
# commands ask for no login; make dataplatform-logout deletes it. Needs curl,
# jq and python3 with PyYAML (requirements.txt; PYTHON names another
# interpreter that has it).
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
# shellcheck source=lib-keycloak.sh
. "$HERE/lib-keycloak.sh"
CLIENT=${CLIENT:-data-cli}
# the header, from its second line to the first that is not a comment
usage() { awk 'NR > 1 && !/^#/ { exit } NR > 1' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
pass=()
while getopts l:ynh opt; do
  case $opt in l) export KEYCLOAK_USER=$OPTARG ;; y) pass+=(-y) ;; n) pass+=(-n) ;; *) usage ;; esac
done
shift $((OPTIND - 1))
[ $# -ge 1 ] || usage
case $1 in check|model|sync|load|list|status|provision|authorize|reject|unprovision|delete|remove|unload-model) ;; *) usage ;; esac

if [ "$1" != check ]; then
  # the cached token while it has a minute left (and is the -l user's), else a login
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
  export TABLES_TOKEN=$TOKEN METACATALOG_URL
fi
exec "${PYTHON:-python3}" "$HERE/tables.py" ${pass[@]+"${pass[@]}"} "$@"
