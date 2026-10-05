#!/usr/bin/env bash
# dp-logout.sh: log out of the data platform on this computer.
#
#   dp-logout.sh [-k]
#
# 1. Deletes what the command-line logins left here: the S3 keys of
#    make dataplatform-s3-login (.secrets/sso-*.env) and the access token of
#    make dataplatform-medallion (.secrets/sso-*.token). Keys already handed
#    to a running job or shell stay valid until they expire (at most 12 hours).
# 2. Ends the browser's Keycloak session: opens Keycloak's logout page (confirm
#    there), so the next login asks for a user and a password again. -k keeps
#    it (only the local files go).
# Web UIs have their own logout: the S3 browser's button, Grafana's, Argo CD's.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=lib-keycloak.sh
. "$HERE/lib-keycloak.sh"
SECRETS_DIR=${SECRETS_DIR:-$(cd "$HERE/../../.." && pwd)/.secrets}
keep=false
usage() { sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
while getopts kh opt; do
  case $opt in k) keep=true ;; *) usage ;; esac
done

gone=0
for f in "$SECRETS_DIR"/sso-*.env "$SECRETS_DIR"/sso-*.token; do
  [ -e "$f" ] || continue
  rm -f "$f"
  echo "removed ${f#"$SECRETS_DIR"/}"
  gone=$((gone + 1))
done
[ "$gone" -gt 0 ] || echo "no cached logins or keys here"

$keep && exit 0
url="$KEYCLOAK_ISSUER/protocol/openid-connect/logout"
echo "ending the browser's Keycloak session: confirm on $url"
if command -v open >/dev/null 2>&1; then open "$url"; elif command -v xdg-open >/dev/null 2>&1; then xdg-open "$url" >/dev/null 2>&1; fi
