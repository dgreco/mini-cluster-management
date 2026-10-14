# Meta Catalog's access decisions -> the platform's data access model.
# Sourced by table.sh on authorize and reject, after lib-iam.sh; needs bash,
# curl and jq.
#
# A table's policy in Meta Catalog (its principals, permissions and grants)
# reaches each of its two resources as the grants in force there. Authorizing
# a resource writes them into the record of what is granted (the ConfigMap
# $GRANTS_CONFIGMAP, lib-iam.sh); what enforces them follows the record:
#
#   the folder   Ceph. The STS roles of the users and groups named get the
#                directory, here and at once (lib-iam.sh); iam-sync gives
#                Trino the role to use there (its S3 mapping).
#   the table    Trino. iam-sync writes the rules the Open Policy Agent
#                decides Trino's statements with, within a minute.
#
# The entries of a table:
#
#   folder.sales.orders.gold.daily_revenue
#     {"path": "lakehouse/sales/orders/gold/daily_revenue",
#      "grants": [{"access": "rw", "users": [], "groups": ["data-engineers"]},
#                 {"access": "ro", "users": [], "groups": ["data-analysts"]}]}
#   table.sales_orders_gold.daily_revenue
#     {"catalog": "lakehouse", "schema": "sales_orders_gold", "table": "daily_revenue",
#      "grants": [... the same shape ...]}
#
# READ -> ro (the files: read and list; the table: SELECT), WRITE -> rw (the
# files: also write and delete; the table: also INSERT, UPDATE, DELETE,
# MERGE; DDL stays Meta Catalog's provisioner's). GROUP and USER are Keycloak
# groups and users; SERVICE <id> is the service account of the Keycloak
# client <id> (service-account-<id>). Anything else fails the resource: a
# permission or principal this platform cannot enforce must not read as
# granted. Rejecting removes the entry.
#
# Only platform-admins may authorize and reject: the gate in front of Meta
# Catalog (metacatalog-proxy.yaml) refuses everyone else these two calls, in
# the API and in the UI. Every user of Meta Catalog may edit a table's
# policy; it counts once an administrator authorizes it. An unchanged entry
# changes nothing, so authorizing again is a no-op.

GRANTS_NAME_RE=$IAM_NAME_RE                     # users and groups, as check_access.py

# The grants of METACATALOG_ACCESS as entries of the record (JSON), or a
# failure naming what cannot be translated.
grants_entries() {
  local out
  if ! out=$(jq -c --arg re "$GRANTS_NAME_RE" '
    def who:
      if .type == "GROUP" then {kind: "groups", name: .id}
      elif .type == "USER" then {kind: "users", name: .id}
      elif .type == "SERVICE" then {kind: "users", name: ("service-account-" + .id)}
      else error("principal \(.id): type \(.type) has no meaning on this platform (GROUP, USER or SERVICE)") end
      | if (.name | test($re)) then . else error("principal \(.name): not a Keycloak name of this platform (lower-case letters, digits, -)") end;
    def level:
      map(if . == "READ" then "ro" elif . == "WRITE" then "rw"
          else error("permission \(.) has no meaning on this platform (READ or WRITE)") end)
      | if index("rw") then "rw" elif index("ro") then "ro" else empty end;
    [ .[] | (.principal | who) + {access: (.permissions | level)} ]
    | group_by(.access)
    | map({access: .[0].access,
           users: [.[] | select(.kind == "users") | .name] | unique,
           groups: [.[] | select(.kind == "groups") | .name] | unique})
    | sort_by(.access) | reverse
  ' <<<"${METACATALOG_ACCESS:-[]}" 2>&1); then
    fail "cannot authorize $RESOURCE: $(sed -E 's/^jq: error \(at [^)]*\): //' <<<"$out")"
  fi
  echo "$out"
}

# Puts this resource's entry (JSON, or "null" to remove it) into the record,
# after checking the record as it would then be. A folder's entry changes
# who reaches which files: Ceph's roles are brought to the record here. A
# table's changes no role: iam-sync reads it.
grants_write() { # <entry JSON | null>
  local before
  record_read
  before=$RECORD
  RECORD=$(jq -c --arg r "$RESOURCE" --argjson e "$1" 'if $e == null then del(.[$r]) else .[$r] = $e end' <<<"$before")
  record_check
  if [ "$RECORD" = "$before" ]; then
    echo "grants of $RESOURCE: unchanged in the record"
  else
    record_put "$RESOURCE" "$1"
    echo "grants of $RESOURCE: written to the record"
    # a reject took a present entry out: should the record now be empty, the
    # last grant went, and the roles go with it (lib-iam.sh)
    [ "$1" != null ] || IAM_ENTRY_REMOVED=true
  fi
  [ "$KIND" != folder ] || iam_apply
}
