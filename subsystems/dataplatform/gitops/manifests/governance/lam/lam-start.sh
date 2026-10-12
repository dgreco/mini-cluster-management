#!/bin/bash
# Start of the LDAP Account Manager container. Its configuration is Git's, put
# in place at every start: the server profile (lam.conf, from the ConfigMap)
# with the search account's password read from its Secret, and the global
# settings the directory's password rule needs. LAM's own configuration
# passwords (master and profile) are random at each start: nobody edits the
# configuration in the UI, a change there would be gone at the next start.
set -euo pipefail
php -r '
  $profile = json_decode(file_get_contents("/etc/lam/lam.conf"), true);
  if (!is_array($profile)) { fwrite(STDERR, "lam.conf is not JSON\n"); exit(1); }
  $profile["loginSearchPassword"] = trim(file_get_contents("/run/secrets/lam/bind-password"));
  file_put_contents("/var/lib/ldap-account-manager/config/lam.conf", json_encode($profile, JSON_PRETTY_PRINT));
  $file = "/etc/ldap-account-manager/config.cfg";
  $global = json_decode(file_get_contents($file), true);
  $global["passwordMinLength"] = getenv("LAM_MIN_PASSWORD_LENGTH");
  $global["passwordMinClasses"] = "3";
  $global["logDestination"] = "NONE";
  file_put_contents($file, json_encode($global, JSON_PRETTY_PRINT));
'
chmod 600 /var/lib/ldap-account-manager/config/lam.conf
LAM_PASSWORD=$(head -c 32 /dev/urandom | base64)
export LAM_PASSWORD
# the image's own start: it hashes that password into both files, sets the
# server and suffixes from LDAP_* (the same values as lam.conf's) and runs Apache
exec /usr/local/bin/start.sh
