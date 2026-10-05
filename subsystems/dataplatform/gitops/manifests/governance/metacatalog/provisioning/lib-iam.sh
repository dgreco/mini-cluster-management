# The applied grants -> Ceph RGW's IAM. Sourced by lib-grants.sh (authorize,
# reject) and reconcile.sh (the hourly CronJob iam-reconcile); needs bash,
# curl and jq.
#
# The record of what is granted is the ConfigMap $GRANTS_CONFIGMAP of this
# namespace: one key per resource an administrator authorized in Meta
# Catalog, its value the resource's directory and grants,
#
#   lakehouse.acme_retail_gold: {"path": "lakehouse/acme_retail/gold",
#     "grants": [{"access": "rw", "users": [], "groups": ["data-engineers"]},
#                {"access": "ro", "users": [], "groups": ["data-analysts"]}]}
#
# Only these scripts write it (authorize puts the resource's key, reject
# removes it); iam-sync reads it (Trino's S3 mapping). It is what was
# approved, not Meta Catalog's current policy: an edited policy counts only
# once an administrator authorizes it again.
#
# From the whole record, every user and group named in a grant gets an STS
# role in the platform's account, usr-<user> or grp-<group>, whose policy
# `access` is the union of its grants (rw: read, write, delete under the
# directory; ro: read; both list it); a grant a directory above already
# covers is left out. Its trust: the principal's Keycloak token
# (AssumeRoleWithWebIdentity) and Trino on behalf of an authenticated user
# (AssumeRole by the RGW user $IAM_TRINO_USER). A role no grant names any
# more is deleted. The roles of $IAM_PLATFORM_ROLES (the admin groups', Meta
# Catalog's provisioner's) are Ansible's (make dataplatform-iam) and never
# touched: grants naming them are kept in the record for iam-sync only. The
# RGW user of $IAM_KEY_DIR may manage usr-* and grp-* roles and nothing
# else, the platform roles excepted (make dataplatform-ceph).
#
# Every run converges all roles, so a run that failed half-way is repaired
# by the next. Runs in parallel (the resources of one authorize, the hourly
# reconcile) converge too: each computes from the record at one version and
# goes again while the record changed under it.

IAM_NAME_RE='^[a-z0-9][a-z0-9-]{0,40}[a-z0-9]$'      # Keycloak users and groups, as check_access.py
IAM_SA=/var/run/secrets/kubernetes.io/serviceaccount

: "${GRANTS_CONFIGMAP:?}" "${MEDALLION_BUCKET:?}" "${MEDALLION_PREFIX:?}" \
  "${IAM_ENDPOINT:?}" "${IAM_REGION:?}" "${IAM_ACCOUNT_ID:?}" "${IAM_OIDC_PROVIDER:?}" \
  "${IAM_TRINO_USER:?}" "${IAM_MAX_SESSION:?}" "${IAM_KEY_DIR:?}" "${IAM_PLATFORM_ROLES?}"

# ---------------------------------------------------------------- Kubernetes ----
# Sets K8S_CODE and K8S_BODY (the token through a config on a file descriptor,
# never in the argument list).
k8s_api() { # <method> <path> [<body> <content type>]
  local out
  out=$(mktemp)
  K8S_CODE=$(curl -sS --max-time 30 -o "$out" -w '%{http_code}' -X "$1" --cacert "$IAM_SA/ca.crt" \
    -K <(printf 'header = "Authorization: Bearer %s"\n' "$(cat "$IAM_SA/token")") \
    ${3:+-H "Content-Type: $4" --data-binary "$3"} \
    "https://kubernetes.default.svc$2") || K8S_CODE=000
  K8S_BODY=$(cat "$out")
  rm -f "$out"
}

record_path() { echo "/api/v1/namespaces/$(cat "$IAM_SA/namespace")/configmaps/$GRANTS_CONFIGMAP"; }

# The record: sets RECORD ({resource: entry}; a value that is not JSON
# becomes {"invalid": <text>}, which record_check refuses) and RECORD_RV.
record_read() {
  k8s_api GET "$(record_path)"
  [ "$K8S_CODE" = 200 ] || fail "the record of grants (ConfigMap $GRANTS_CONFIGMAP) answered HTTP $K8S_CODE: ${K8S_BODY:0:300}"
  RECORD=$(jq -c '(.data // {}) | with_entries(.value |= (try fromjson catch {invalid: .}))' <<<"$K8S_BODY")
  RECORD_RV=$(jq -r .metadata.resourceVersion <<<"$K8S_BODY")
}

# Puts one resource's entry into the record, or removes it (null). One key
# per resource, by merge patch: parallel runs for other resources never
# conflict.
record_put() { # <resource> <entry JSON | null>
  local body
  [[ $1 =~ ^[-._a-zA-Z0-9]+$ ]] || fail "resource name '$1' cannot be a key of the record"
  body=$(jq -cn --arg k "$1" --argjson e "$2" '{data: {($k): (if $e == null then null else ($e | tojson) end)}}')
  k8s_api PATCH "$(record_path)" "$body" application/merge-patch+json
  [ "$K8S_CODE" = 200 ] || fail "writing the grants of $1 into the record answered HTTP $K8S_CODE: ${K8S_BODY:0:300}"
}

# Fails, naming them, on entries the platform cannot enforce: the scripts
# write only valid ones, so anything else was edited by hand.
record_check() {
  local problems
  problems=$(jq -r --arg prefix "$MEDALLION_PREFIX" --arg re "$IAM_NAME_RE" '
    to_entries[] | .key as $k | .value as $e
    | if ($e | type) != "object" or ($e.path | type) != "string" or ($e.grants | type) != "array" then
        "\($k): not {path, grants}"
      else
        (if ($e.path | test("^[a-z0-9._-]+(/[a-z0-9._-]+)*$") | not)
            or ($e.path | split("/") | any(. == "." or . == ".."))
            or ($e.path | startswith($prefix + "/") | not)
         then "\($k): path \($e.path | tojson) is not a directory under \($prefix)/" else empty end),
        ($e.grants[] | if type != "object" then "\($k): a grant is not an object" else
          (if (.access | IN("ro", "rw")) then empty else "\($k): access \(.access | tojson) (ro or rw)" end),
          (((.users // []), (.groups // [])) | if type != "array" then "\($k): users and groups are lists"
             else .[] | select(type != "string" or (test($re) | not)) | "\($k): \(tojson) is not a Keycloak name" end)
          end)
      end' <<<"$RECORD")
  [ -z "$problems" ] || fail "the record of grants holds what cannot be applied (edited by hand?); nothing changed:"$'\n'"$problems"
}

# ---------------------------------------------------------------- the desired roles ----
# {role: {kind, name, trust, policy}} from the record (jq; RECORD on input).
IAM_DESIRED_JQ='
def grants: [to_entries | sort_by(.key)[] | .value as $e | $e.grants[]
             | {path: $e.path, access, users: (.users // []), groups: (.groups // [])}];
# without the grants a directory above already covers (rw covers both)
def essential: . as $all | [.[] | . as $g
  | select([$all[] | .path as $above | select(($g.path | startswith($above + "/")) and (.access == "rw" or .access == $g.access))]
           | length == 0)]
  | unique;
def policy: ("arn:aws:s3:::" + $bucket) as $b | {Version: "2012-10-17", Statement: (
  [{Effect: "Allow", Action: ["s3:GetBucketLocation"], Resource: [$b]},
   {Effect: "Allow", Action: ["s3:ListBucket"], Resource: [$b],
    Condition: {StringLike: {"s3:prefix": [.[] | .path, .path + "/*"] | unique}}}]
  + ([.[] | select(.access == "ro") | "\($b)/\(.path)/*"] | unique
     | if length > 0 then [{Effect: "Allow", Action: ["s3:GetObject"], Resource: .}] else [] end)
  + ([.[] | select(.access == "rw") | "\($b)/\(.path)/*"] | unique
     | if length > 0 then [{Effect: "Allow", Action: ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
         "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], Resource: .}] else [] end))};
def trust($kind; $name): {Version: "2012-10-17", Statement: [
  {Sid: (if $kind == "user" then "TheKeycloakUser" else "MembersOfTheKeycloakGroup" end), Effect: "Allow",
   Principal: {Federated: ["arn:aws:iam::\($account):oidc-provider/\($idp)"]},
   Action: ["sts:AssumeRoleWithWebIdentity"],
   Condition: {StringEquals: {("\($idp):" + (if $kind == "user" then "preferred_username" else "groups" end)): $name}}},
  {Sid: "TrinoOnBehalfOfAuthenticatedUsers", Effect: "Allow",
   Principal: {AWS: ["arn:aws:iam::\($account):user/\($trino)"]}, Action: ["sts:AssumeRole"]}]};
grants as $g
| ([$g[].users[]] | unique | map({role: ("usr-" + .), kind: "user", name: .}))
  + ([$g[].groups[]] | unique | map({role: ("grp-" + .), kind: "group", name: .}))
| map(select(.role as $r | $platform | index($r) | not))
| map(. as $p | {key: .role, value: {kind, name, trust: trust(.kind; .name),
    policy: ([$g[] | select(if $p.kind == "user" then any(.users[]; . == $p.name) else any(.groups[]; . == $p.name) end)]
             | essential | policy)}})
| from_entries'

iam_desired() {
  jq -c --arg bucket "$MEDALLION_BUCKET" --arg account "$IAM_ACCOUNT_ID" --arg idp "$IAM_OIDC_PROVIDER" \
    --arg trino "$IAM_TRINO_USER" --argjson platform "$(jq -cn --arg p "$IAM_PLATFORM_ROLES" '$p | split(" ") | map(select(. != ""))')" \
    "$IAM_DESIRED_JQ" <<<"$RECORD"
}

# ---------------------------------------------------------------- RGW's IAM API ----
# One call: sets IAM_CODE and IAM_BODY. Parameters as name=value, URL-encoded
# here. The key is read at each call (a mounted Secret: a new key arrives
# without a restart) and handed to curl on a file descriptor. Content-Type is
# set explicitly so that it is signed: RGW refuses the request otherwise. No
# answer or a 5xx is retried for a minute.
iam_call() { # <Action> [<name>=<value> ...]
  local action=$1 out i p args=()
  shift
  for p in "$@"; do args+=(--data-urlencode "$p"); done
  out=$(mktemp)
  for i in 1 2 3 4 5 6; do
    IAM_CODE=$(curl -sS --max-time 30 -o "$out" -w '%{http_code}' \
      --aws-sigv4 "aws:amz:$IAM_REGION:iam" \
      -K <(printf 'user = "%s:%s"\n' "$(cat "$IAM_KEY_DIR/access-key")" "$(cat "$IAM_KEY_DIR/secret-key")") \
      -H 'Content-Type: application/x-www-form-urlencoded; charset=utf-8' \
      --data-urlencode "Action=$action" --data-urlencode "Version=2010-05-08" "${args[@]}" \
      "$IAM_ENDPOINT/") || IAM_CODE=000
    case $IAM_CODE in 000|5??) sleep 10 ;; *) break ;; esac
  done
  IAM_BODY=$(cat "$out")
  rm -f "$out"
}

# The text of the first <tag> of IAM_BODY, XML entities resolved (RGW sends
# policy documents as escaped JSON).
xml_text() { # <tag>
  sed -n "s:.*<$1>\([^<]*\)</$1>.*:\1:p" <<<"$IAM_BODY" | head -1 \
    | sed -e 's/&quot;/"/g' -e "s/&apos;/'/g" -e 's/&lt;/</g' -e 's/&gt;/>/g' -e 's/&amp;/\&/g'
}

iam_ok() { # <what> <HTTP codes...>: fails naming the call unless IAM_CODE is one of them
  local what=$1 c
  shift
  for c in "$@"; do [ "$IAM_CODE" = "$c" ] && return 0; done
  fail "RGW IAM: $what answered HTTP $IAM_CODE: $(xml_text Code) $(xml_text Message)"
}

# The usr-* and grp-* roles of the account (one per line), the platform's left out.
iam_roles() {
  local marker="" page r
  while :; do
    iam_call ListRoles ${marker:+"Marker=$marker"}
    iam_ok "listing the roles" 200
    page=$(grep -o '<RoleName>[^<]*</RoleName>' <<<"$IAM_BODY" | sed -e 's:<RoleName>::' -e 's:</RoleName>::') || true
    for r in $page; do
      [[ $r =~ ^(usr|grp)- && " $IAM_PLATFORM_ROLES " != *" $r "* ]] && echo "$r"
    done
    [ "$(xml_text IsTruncated)" = true ] || break
    marker=$(xml_text Marker)
    [ -n "$marker" ] || break
  done
  return 0
}

# The names of a role's policies (one per line).
iam_role_policies() { # <role>
  iam_call ListRolePolicies "RoleName=$1"
  iam_ok "listing the policies of $1" 200
  grep -o '<member>[^<]*</member>' <<<"$IAM_BODY" | sed -e 's:<member>::' -e 's:</member>::' || true
}

same_json() { # <a> <b>: equal as JSON, the order of every list ignored (anything unreadable differs)
  [ "$(jq -cn --argjson a "$1" --argjson b "$2" \
       'def canon: walk(if type == "array" then sort else . end); ($a | canon) == ($b | canon)' 2>/dev/null)" = true ]
}

# ---------------------------------------------------------------- converge ----
iam_change() { # <what>: counts a change and prints it (plan: what apply would do)
  IAM_CHANGES=$((IAM_CHANGES + 1))
  if [ "$IAM_MODE" = plan ]; then echo "would $*"; else echo "$*"; fi
}

iam_converge_role() { # <role> <spec JSON>
  local role=$1 trust policy current n
  trust=$(jq -c .trust <<<"$2")
  policy=$(jq -c .policy <<<"$2")
  iam_call GetRole "RoleName=$role"
  iam_ok "reading role $role" 200 404
  if [ "$IAM_CODE" = 404 ]; then
    iam_change "create role $role"
    if [ "$IAM_MODE" = apply ]; then
      iam_call CreateRole "RoleName=$role" "Path=/" "MaxSessionDuration=$IAM_MAX_SESSION" "AssumeRolePolicyDocument=$trust"
      iam_ok "creating role $role" 200
    fi
    current=null
  else
    current=$(xml_text AssumeRolePolicyDocument)
    n=$(xml_text MaxSessionDuration)
    [ "$n" = "$IAM_MAX_SESSION" ] || echo "note: $role allows sessions of ${n:-?} s, not $IAM_MAX_SESSION (not changed here)"
    if ! same_json "${current:-null}" "$trust"; then
      iam_change "update the trust of $role"
      [ "$IAM_MODE" = plan ] && printf '  now:  %s\n  then: %s\n' "$current" "$trust"
      if [ "$IAM_MODE" = apply ]; then
        iam_call UpdateAssumeRolePolicy "RoleName=$role" "PolicyDocument=$trust"
        iam_ok "updating the trust of $role" 200
      fi
    fi
    iam_call GetRolePolicy "RoleName=$role" "PolicyName=access"
    iam_ok "reading the policy of $role" 200 404
    if [ "$IAM_CODE" = 200 ]; then current=$(xml_text PolicyDocument); else current=null; fi
  fi
  if ! same_json "${current:-null}" "$policy"; then
    iam_change "put the policy of $role"
    [ "$IAM_MODE" = plan ] && printf '  now:  %s\n  then: %s\n' "$current" "$policy"
    if [ "$IAM_MODE" = apply ]; then
      iam_call PutRolePolicy "RoleName=$role" "PolicyName=access" "PolicyDocument=$policy"
      iam_ok "putting the policy of $role" 200
    fi
  fi
  # only the policy `access`: anything else was added by hand
  [ "$IAM_MODE" = plan ] && [ "$current" = null ] && return 0     # a role still to be made
  for n in $(iam_role_policies "$role" | grep -vx access || true); do
    iam_change "delete the policy $n of $role"
    if [ "$IAM_MODE" = apply ]; then
      iam_call DeleteRolePolicy "RoleName=$role" "PolicyName=$n"
      iam_ok "deleting the policy $n of $role" 200
    fi
  done
}

iam_drop_role() { # <role>
  local n
  iam_change "delete role $1 (no grant names it any more)"
  [ "$IAM_MODE" = apply ] || return 0
  for n in $(iam_role_policies "$1"); do
    iam_call DeleteRolePolicy "RoleName=$1" "PolicyName=$n"
    iam_ok "deleting the policy $n of $1" 200
  done
  iam_call DeleteRole "RoleName=$1"
  iam_ok "deleting role $1" 200
}

# Brings the roles to the record. plan: prints what apply would change.
# Sets IAM_CHANGES (the changes made or planned) and RECORD_RV (the version
# of the record it computed from).
iam_converge() { # plan|apply
  local desired roles existing role
  IAM_MODE=$1
  IAM_CHANGES=0
  record_read
  record_check
  desired=$(iam_desired)
  roles=$(jq -r 'keys[]' <<<"$desired")
  existing=$(iam_roles)
  for role in $roles; do
    iam_converge_role "$role" "$(jq -c --arg r "$role" '.[$r]' <<<"$desired")"
  done
  for role in $existing; do
    grep -qx "$role" <<<"$roles" && continue
    if [ "$(jq length <<<"$RECORD")" = 0 ]; then
      # an empty record is far likelier a lost one than every grant rejected
      echo "kept role $role: the record of grants is empty (if nothing is granted anywhere on purpose, delete it by hand)"
      continue
    fi
    iam_drop_role "$role"
  done
}

# apply, again while the record changed during the run (another authorize or
# reject, in this pod or another); one run at a time in this pod.
iam_apply() {
  local i rv
  exec 9>/tmp/metacatalog-iam.lock
  flock 9
  for i in 1 2 3 4 5; do
    iam_converge apply
    rv=$RECORD_RV
    k8s_api GET "$(record_path)"
    [ "$K8S_CODE" = 200 ] && [ "$(jq -r .metadata.resourceVersion <<<"$K8S_BODY")" = "$rv" ] && break
    echo "the record changed during the run: again"
  done
  flock -u 9
  if [ "$IAM_CHANGES" = 0 ]; then echo "roles: no change"; fi
}
