#!/usr/bin/env bash
# dp-s3-login.sh: temporary S3 keys of your Keycloak group (single sign-on + STS).
#
#   dp-s3-login.sh [-l <user>] [-g <group> | -u] [-d <seconds>] [-o <file>]
#
# 1. Logs you in to Keycloak (device login of the client data-cli: open the URL
#    it prints) unless S3_LOGIN_TOKEN_FILE names a file holding an access token.
#    -l <user>: the login must be that user's (a fresh Keycloak login is asked
#    for; the browser's session of someone else is refused).
# 2. Exchanges the token at Ceph's STS (AssumeRoleWithWebIdentity) for keys of
#    a role: your own usr-<you> (-u) or a group's grp-<group> (-g), the roles
#    the lakehouses' policies (Meta Catalog) grant directories of the
#    platform bucket to. Without either: your own role if a lakehouse grants
#    you one (as Trino), else your only group's. It prints the directories the keys reach and
#    your other roles (a session has one role). Ceph refuses a group you
#    are not in and a role nobody made. The keys expire after -d seconds
#    (default 3600, at most 43200) whatever happens to your Keycloak session.
# 3. Writes them to .secrets/sso-<user>.env (or -o <file>) and checks that they
#    reach the platform bucket:
#      source .secrets/sso-alice.env
#      aws s3 ls s3://dataplatform/lakehouse/acme_retail/bronze/
#
# Needs curl, jq and python3.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=lib-keycloak.sh
. "$HERE/lib-keycloak.sh"
CLIENT=${CLIENT:-data-cli}
SECRETS_DIR=${SECRETS_DIR:-$(cd "$(dirname "$0")/../../.." && pwd)/.secrets}
group="" own=false duration=3600 out=""
usage() { sed -n '2,23p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
while getopts l:g:ud:o:h opt; do
  case $opt in
    l) export KEYCLOAK_USER=$OPTARG ;; g) group=$OPTARG ;; u) own=true ;; d) duration=$OPTARG ;; o) out=$OPTARG ;; *) usage ;;
  esac
done
shift $((OPTIND - 1))
[ $# -eq 0 ] || usage
case $duration in ''|*[!0-9]*) usage ;; esac

# ---------------------------------------------------------------- who ----
token=$(keycloak_login "$CLIENT" "${S3_LOGIN_TOKEN_FILE:-}")
claims=$(jwt_claims "$token")
user=$(jq -r .preferred_username <<<"$claims")
groups=$(jq -r '.groups // [] | .[]' <<<"$claims")
# The roles to try, in order. Without -u or -g: your own role first (as
# Trino does), else your only group's.
if $own; then
  candidates="${USER_ROLE_PREFIX}${user}"
elif [ -n "$group" ]; then
  candidates="${GROUP_ROLE_PREFIX}${group}"
elif [ "$(grep -c . <<<"$groups" || true)" -eq 1 ]; then
  candidates="${USER_ROLE_PREFIX}${user} ${GROUP_ROLE_PREFIX}${groups}"
else
  candidates="${USER_ROLE_PREFIX}${user}"
fi

# ---------------------------------------------------------------- STS ----
sts_keys() {  # <role>: prints AccessKeyId, SecretAccessKey, SessionToken, Expiration, one per line
  local resp
  resp=$(curl -sS -X POST "$S3_ENDPOINT/" \
    --data-urlencode Action=AssumeRoleWithWebIdentity --data-urlencode Version=2011-06-15 \
    --data-urlencode "RoleArn=${ROLE_ARN_PREFIX}$1" --data-urlencode "RoleSessionName=$user" \
    --data-urlencode "DurationSeconds=$duration" --data-urlencode "WebIdentityToken=$token")
  python3 -c '
import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read())
v = {e.tag.split("}")[-1]: (e.text or "") for e in root.iter()}
if "AccessKeyId" not in v:
    sys.exit("Ceph STS refused: %s %s" % (v.get("Code", "?"), v.get("Message", "")))
print("\n".join(v[k] for k in ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration")))
' <<<"$resp"
}
creds="" role=""
for r in $candidates; do
  if creds=$(sts_keys "$r" 2>/dev/null); then role=$r; break; fi
done
if [ -z "$role" ]; then
  last=${candidates##* }
  sts_keys "$last" >/dev/null || true
  if [ -z "$group" ] && ! $own && [ "$(grep -c . <<<"$groups" || true)" -ne 1 ]; then
    echo "$user has no role of their own and is in the groups: $(tr '\n' ' ' <<<"$groups")- choose one with -g <group>" >&2
    exit 2
  fi
  echo "no keys for $user as $(tr ' ' '/' <<<"$candidates") (does a lakehouse grant the role anything? is $user in the Keycloak group ${group:-'-'}? groups: $(tr '\n' ' ' <<<"$groups"))" >&2
  exit 1
fi
{ read -r ak; read -r sk; read -r st; read -r exp; } <<<"$creds"

# ---------------------------------------------------------------- write ----
out=${out:-$SECRETS_DIR/sso-$user.env}
mkdir -p "$(dirname "$out")"
umask 077
cat >"$out" <<EOF
# S3 keys of $user as $role (Keycloak single sign-on + Ceph STS), valid until $exp.
# New ones: make dataplatform-s3-login ARGS="$( $own && echo -u || echo "-g $group" )"
unset AWS_PROFILE
export AWS_ACCESS_KEY_ID=$ak
export AWS_SECRET_ACCESS_KEY=$sk
export AWS_SESSION_TOKEN=$st
export AWS_REGION=$S3_REGION
export AWS_DEFAULT_REGION=$S3_REGION
export AWS_ENDPOINT_URL=$S3_ENDPOINT
EOF

# the keys reach the platform bucket (GetBucketLocation, which every role
# may call; SigV4 with curl, no AWS CLI needed). The keys reach curl through
# a config file, never on its command line (visible in ps).
cfg=$(mktemp)
trap 'rm -f "$cfg"' EXIT
printf 'user = "%s:%s"\nheader = "x-amz-security-token: %s"\n' "$ak" "$sk" "$st" > "$cfg"
code=$(curl -s -o /dev/null -w '%{http_code}' --aws-sigv4 "aws:amz:$S3_REGION:s3" -K "$cfg" \
       "$S3_ENDPOINT/$S3_BUCKET?location" || true)
echo "$user as $role until $exp: $out" >&2
# what the keys open, and the user's other roles, as the repository's grants
# say (access.yaml and Meta Catalog's, read the way iam-sync reads them)
python3 - "$HERE/../gitops/iam/access.yaml" "$HERE/../gitops/manifests/governance/iam-sync/iam_sync.py" \
  "$role" "$user" "$USER_ROLE_PREFIX" "$GROUP_ROLE_PREFIX" $groups <<'PY' >&2 || true
import importlib.util, os, sys
access, sync, role, user, up, gp, *groups = sys.argv[1:]
os.environ["ACCESS_FILE"] = access
spec = importlib.util.spec_from_file_location("iam_sync", sync)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
doc = m.read_access()
def reach(r):
    if r.startswith(gp) and r[len(gp):] in doc["admins"]:
        return ["the whole bucket (admin)"]
    key = "users" if r.startswith(up) else "groups"
    name = r[len(up):] if r.startswith(up) else r[len(gp):]
    paths = {}
    for g in doc["grants"]:
        if name in g[key] and paths.get(g["path"]) != "rw":
            paths[g["path"]] = g["access"]
    # a directory under one already listed with as much access adds nothing
    top = [p for p in sorted(paths) if not any(p.startswith(q + "/") and (paths[q] == "rw" or paths[q] == paths[p])
                                              for q in paths if q != p)]
    return [f"{p}/ ({paths[p]})" for p in top]
print(f"the keys reach: {', '.join(reach(role)) or 'nothing: no lakehouse grants this role a directory'}")
mine = [up + user] + [gp + g for g in groups]
other = [f"{r} ({', '.join(reach(r))})" for r in mine if r != role and reach(r)]
if other:
    print("your other roles: " + "; ".join(other) + "   (-u: your own, -g <group>: a group's)")
PY
[ "$code" = 200 ] && echo "checked: the keys reach s3://$S3_BUCKET/ (the role's directories: the lakehouses' grants)" >&2 \
  || echo "warning: s3://$S3_BUCKET/ answered HTTP $code to the keys" >&2
echo "use them with:  source $out" >&2
