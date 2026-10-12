# Shared by ad-entrypoint, ad-backup and ad-restore (sourced).
# shellcheck shell=bash
: "${AD_REALM:?the AD DNS domain}" "${AD_NETBIOS_DOMAIN:?the NetBIOS domain name}"

ad_state=/var/lib/samba            # the volume: everything of the domain
ad_run=/run/samba-ad               # this container's own: the certificate's copy, the s3cmd settings
ad_realm_uc=${AD_REALM^^}
ad_realm_lc=${AD_REALM,,}

# The domain controller's name is the volume's (.dc-name, written by the
# provision and by a restore): samba-tool domain backup restore refuses the
# name of a controller the backup knows, so a restored domain always comes
# back under the next name, and the pod's name says nothing.
ad_dc_name() {
  [ -s "$ad_state/.dc-name" ] || { echo "$ad_state/.dc-name is missing: this volume holds no provisioned domain" >&2; return 1; }
  cat "$ad_state/.dc-name"
}

# dc1 -> dc2, dc9 -> dc10; a name without a number gets 2
ad_next_name() {
  local name=${1,,} stem number
  stem=${name%%[0-9]*}
  number=${name#"$stem"}
  if [[ $number =~ ^[0-9]+$ ]]; then echo "$stem$((10#$number + 1))"; else echo "${name}2"; fi
}

# smb.conf, written from the environment at every start: none of it is state.
# NT ACLs go into user.NTACL: Samba's default, security.NTACL, needs
# CAP_SYS_ADMIN, which Pod Security baseline refuses (samba-tool ntacl
# sysvolcheck reads security.NTACL itself and cannot be used here).
ad_render_conf() {  # <dc name> [<dns forwarder>]
  local dc=$1 forwarder=${2:-}
  install -d -m 0755 /etc/samba
  {
    echo "[global]"
    echo "    netbios name = ${dc^^}"
    echo "    realm = $ad_realm_uc"
    echo "    workgroup = $AD_NETBIOS_DOMAIN"
    echo "    server role = active directory domain controller"
    [ -z "$forwarder" ] || echo "    dns forwarder = $forwarder"
    echo "    acl_xattr:security_acl_name = user.NTACL"
    echo "    ldap server require strong auth = yes"
    # A password set by an administrator counts at once. Samba's default lets
    # the one before it log in for another hour (meant for desktops that
    # cached it; an LDAP bind, so every Keycloak login, takes it too).
    echo "    old password allowed period = 0"
    echo "    tls enabled = yes"
    echo "    tls keyfile = $ad_run/tls/tls.key"
    echo "    tls certfile = $ad_run/tls/tls.crt"
    echo "    tls cafile = $ad_run/tls/ca.crt"
    echo "[sysvol]"
    echo "    path = $ad_state/sysvol"
    echo "    read only = No"
    echo "[netlogon]"
    echo "    path = $ad_state/sysvol/$ad_realm_lc/scripts"
    echo "    read only = No"
  } > /etc/samba/smb.conf
}

# Everything of the volume but the file system's own lost+found and the
# marker that says why it is being emptied
ad_empty_volume() {  # <marker to keep>
  find "$ad_state" -mindepth 1 -maxdepth 1 ! -name lost+found ! -name "$1" -exec rm -rf {} +
}

# s3cmd with the backup key (files of the Secret backup-s3), never on a
# command line: S3_ENDPOINT (https://host[:port]), S3_REGION, S3_BUCKET,
# S3_PREFIX from the environment
ad_s3() {
  local dir=${AD_BACKUP_SECRETS_DIR:-/run/secrets/backup-s3} host tls=True
  : "${S3_ENDPOINT:?}" "${S3_REGION:?}" "${S3_BUCKET:?}" "${S3_PREFIX:?}"
  host=${S3_ENDPOINT#*://}
  case $S3_ENDPOINT in http://*) tls=False ;; esac
  if [ ! -s "$ad_run/s3cfg" ]; then
    install -d -m 0700 "$ad_run"
    ( umask 077
      printf '[default]\naccess_key = %s\nsecret_key = %s\nhost_base = %s\nhost_bucket = %s\nuse_https = %s\nbucket_location = %s\n' \
        "$(cat "$dir/access-key")" "$(cat "$dir/secret-key")" "$host" "$host" "$tls" "$S3_REGION" > "$ad_run/s3cfg" )
  fi
  s3cmd --config="$ad_run/s3cfg" --no-progress "$@"
}

# The backups there, oldest first (their names sort by date)
ad_s3_backups() {
  ad_s3 ls "s3://$S3_BUCKET/$S3_PREFIX" | awk '{print $NF}' | grep -E '/samba-ad-[0-9TZ]+\.tar\.bz2$' | sort || true
}
