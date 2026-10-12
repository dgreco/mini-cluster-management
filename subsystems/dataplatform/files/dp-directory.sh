#!/usr/bin/env bash
# People and groups of the platform, for its administrators, on the command
# line. They are kept in one place, the directory (Samba AD,
# governance/samba-ad), which Keycloak only reads. The everyday way to manage
# them is the directory's web interface (LDAP Account Manager,
# https://directory.<platform domain>); this does the same without a browser,
# with kubectl instead of a directory login. Needs kubectl (context k3s:
# make k3s-kubeconfig); the work is done by adctl in the domain controller's
# pod.
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
#   dp-directory.sh ca                  the directory's certificate authority (PEM), for LDAP clients:
#                                       dp-directory.sh ca > samba-ad-ca.crt
#
# LDAP clients outside the cluster reach the directory at ldaps://ad.<platform
# domain>:636 and must trust that authority. A user logs in there as
# <user>@<AD domain>, as <NETBIOS domain>\<user>, or with the entry's DN.
#
# user add and user passwd ask for the password twice (or take one line of
# standard input when that is not a terminal): at least 12 characters with a
# capital, a small letter and a digit. Only an administrator can change it
# later, here or in LDAP Account Manager: Keycloak only reads the directory.
# A user needs a name, a surname and an e-mail address of their own, or
# Keycloak cannot log them in.
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
  "ca ")
    kubectl -n "$NS" get secret samba-ad-tls -o 'jsonpath={.data.ca\.crt}' | base64 -d ;;
  "inventory " | user\ * | group\ *)
    adctl "$@" </dev/null ;;
  *)
    sed -n '2,37p' "$0" | sed 's/^# \{0,1\}//'
    exit 1 ;;
esac
