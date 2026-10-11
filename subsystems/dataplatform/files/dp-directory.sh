#!/usr/bin/env bash
# People and groups of the platform, for its administrators, on the command
# line. They are kept in one place, the directory (Samba AD,
# governance/samba-ad), and the everyday way to manage them is the Keycloak
# console, which writes to it; this does the same without a browser, and is
# the way to make or remove a group (a group made in Keycloak exists in the
# directory only once it has a member, and one deleted in Keycloak comes back
# from the directory). Needs kubectl (context k3s: make k3s-kubeconfig); the
# work is done by adctl in the domain controller's pod.
#
#   dp-directory.sh user list
#   dp-directory.sh user add <name> --given-name <g> --surname <s> --mail <address> [--group <group>]...
#   dp-directory.sh user passwd <name>
#   dp-directory.sh user disable|enable|remove <name>
#   dp-directory.sh group list
#   dp-directory.sh group add|remove <name>
#   dp-directory.sh group members <name>
#   dp-directory.sh group join|leave <group> <user>...
#   dp-directory.sh inventory           everything, as JSON
#   dp-directory.sh backup              a backup now (the CronJob samba-ad-backup makes one every night)
#
# user add and user passwd ask for the password twice (or take one line of
# standard input when that is not a terminal): at least 12 characters with a
# capital, a small letter and a digit. People change their own in Keycloak
# (Account, Signing in). A user made here needs a name, a surname and an
# e-mail address of their own.
# What Keycloak sees, and when: a new user at their first login; a change of
# membership at once in Trino (iam-sync, 30 s) and in the user's next token
# (within 5 minutes); a new group within a minute; a disabled or removed user
# is refused at once.
set -euo pipefail

NS=governance
POD=samba-ad-0

die() { echo "$*" >&2; exit 1; }
adctl() { kubectl -n "$NS" exec -i "$POD" -c samba -- adctl "$@"; }
# the new password on standard output, never on a command line
password() {
  local first second
  if [ -t 0 ]; then
    read -r -s -p "Password of $1: " first; echo >&2
    read -r -s -p "Again: " second; echo >&2
    [ "$first" = "$second" ] || die "the two differ"
    printf '%s\n' "$first"
  else
    head -n 1
  fi
}

case "${1:-} ${2:-}" in
  "user add")
    [ -n "${3:-}" ] || die "usage: dp-directory.sh user add <name> --given-name <g> --surname <s> --mail <address> [--group <group>]..."
    secret=$(password "$3")
    printf '%s\n' "$secret" | adctl "$@" ;;
  "user passwd")
    [ -n "${3:-}" ] || die "usage: dp-directory.sh user passwd <name>"
    secret=$(password "$3")
    printf '%s\n' "$secret" | adctl user passwd "$3" ;;
  "backup ")
    kubectl -n "$NS" exec "$POD" -c samba -- ad-backup ;;
  "inventory " | user\ * | group\ *)
    adctl "$@" </dev/null ;;
  *)
    sed -n '2,31p' "$0" | sed 's/^# \{0,1\}//'
    exit 1 ;;
esac
