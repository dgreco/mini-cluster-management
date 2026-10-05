# Keycloak helpers for the data platform's command-line scripts (sourced by
# dp-s3-login.sh and dp-spark-submit.sh). Needs curl, jq and python3.
#
#   keycloak_login <client> [<token file>]
#     Prints an access token of the realm: read from <token file> when given
#     (tests, automation), else through the device login of <client> (the user
#     opens the printed URL in a browser; with PKCE, which Keycloak requires
#     there too of a client set to S256). With KEYCLOAK_USER set (every
#     script's -l <user>), the browser flow is used instead: the scripts open
#     Keycloak's login form with the name filled in and a fresh login forced
#     (prompt=login, login_hint; PKCE), whatever session the browser has, and
#     catch the answer on http://127.0.0.1:$KEYCLOAK_CALLBACK_PORT/callback
#     (18765, registered on the clients data-cli and spark). A token of
#     anybody else is refused.
#   jwt_claims <token>
#     Prints the token's claims (JSON).

# the site's settings (KEYCLOAK_ISSUER and the others; make configure renders it)
# shellcheck source=site.env
. "$(dirname "${BASH_SOURCE[0]}")/site.env"
KEYCLOAK_CALLBACK_PORT=${KEYCLOAK_CALLBACK_PORT:-18765}

# A PKCE verifier and its S256 challenge, separated by a space.
pkce_pair() {
  python3 -c 'import base64, hashlib, secrets; v = secrets.token_urlsafe(64); print(v, base64.urlsafe_b64encode(hashlib.sha256(v.encode()).digest()).decode().rstrip("="))'
}

# The browser flow with PKCE and a loopback redirect: prints the access token.
keycloak_browser_login() {
  local client=$1 want=$2 redirect pair verifier challenge state url tmp pid code resp
  redirect="http://127.0.0.1:$KEYCLOAK_CALLBACK_PORT/callback"
  pair=$(pkce_pair); verifier=${pair% *}; challenge=${pair#* }
  state=$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')
  url=$(python3 -c 'import sys, urllib.parse as u; print(sys.argv[1] + "/protocol/openid-connect/auth?" + u.urlencode({
      "client_id": sys.argv[2], "response_type": "code", "scope": "openid", "redirect_uri": sys.argv[3],
      "code_challenge": sys.argv[4], "code_challenge_method": "S256", "state": sys.argv[5],
      "prompt": "login", "login_hint": sys.argv[6]}))' "$KEYCLOAK_ISSUER" "$client" "$redirect" "$challenge" "$state" "$want")
  tmp=$(mktemp)
  # one request on the loopback port: the code comes back with our state
  python3 - "$KEYCLOAK_CALLBACK_PORT" "$state" "$tmp" <<'PY' &
import http.server, sys, time, urllib.parse
port, state, out = int(sys.argv[1]), sys.argv[2], sys.argv[3]
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        if u.path != "/callback":
            self.send_response(404); self.end_headers(); return
        ok = q.get("state", [""])[0] == state and "code" in q
        why = ("an answer with another state (not this login's): refused" if q.get("state", [""])[0] != state
               else q.get("error_description", q.get("error", ["no code in the answer"]))[0])
        with open(out, "w") as f:
            f.write(q["code"][0] if ok else "ERROR " + why)
        self.send_response(200); self.send_header("Content-Type", "text/plain; charset=utf-8"); self.end_headers()
        self.wfile.write(("Logged in. You can close this tab." if ok else "Login failed: see the terminal.").encode())
        self.server.done = True
    def log_message(self, *a):
        pass
try:
    srv = http.server.HTTPServer(("127.0.0.1", port), H)
except OSError as e:
    open(out, "w").write(f"ERROR port {port}: {e} (KEYCLOAK_CALLBACK_PORT)"); sys.exit(1)
srv.done, srv.timeout, end = False, 5, time.time() + 300
while not srv.done and time.time() < end:
    srv.handle_request()
if not srv.done:
    open(out, "w").write("ERROR no answer within 5 minutes")
PY
  pid=$!
  sleep 0.3
  echo "Log in as $want in the browser (a login form with the name filled in):" >&2
  echo "  $url" >&2
  if command -v open >/dev/null 2>&1; then open "$url" 2>/dev/null; elif command -v xdg-open >/dev/null 2>&1; then xdg-open "$url" >/dev/null 2>&1; fi
  wait "$pid" 2>/dev/null
  code=$(cat "$tmp"); rm -f "$tmp"
  case $code in ''|ERROR*) echo "login failed: ${code#ERROR }" >&2; return 1 ;; esac
  resp=$(curl -sS -d client_id="$client" -d grant_type=authorization_code -d code="$code" \
              --data-urlencode "redirect_uri=$redirect" -d code_verifier="$verifier" \
              "$KEYCLOAK_ISSUER/protocol/openid-connect/token")
  jq -e -r .access_token <<<"$resp" 2>/dev/null \
    || { echo "login failed: $(jq -r '.error_description // .error // .' <<<"$resp")" >&2; return 1; }
}

keycloak_login() {
  local client=$1 token_file=${2:-} want=${KEYCLOAK_USER:-} token="" who
  if [ -n "$token_file" ]; then
    token=$(cat "$token_file")
  elif [ -n "$want" ]; then
    token=$(keycloak_browser_login "$client" "$want") || return 1
  else
    token=$(keycloak_device_login "$client") || return 1
  fi
  if [ -n "$want" ]; then
    who=$(jwt_claims "$token" | jq -r '.preferred_username // ""')
    if [ "$who" != "$want" ]; then
      echo "that login is $who's, not $want's: log in as $want on the form (not another account)." >&2
      return 1
    fi
  fi
  printf '%s\n' "$token"
}

# The device flow: prints the access token once the user confirmed the code.
keycloak_device_login() {
  local client=$1 dev interval resp token="" pair verifier challenge
  pair=$(pkce_pair); verifier=${pair% *}; challenge=${pair#* }
  dev=$(curl -sS -d client_id="$client" -d scope=openid \
             -d code_challenge="$challenge" -d code_challenge_method=S256 \
             "$KEYCLOAK_ISSUER/protocol/openid-connect/auth/device")
  if ! jq -e .device_code <<<"$dev" >/dev/null 2>&1; then
    echo "login failed: $(jq -r '.error_description // .error' <<<"$dev" 2>/dev/null || printf '%s' "$dev")" >&2
    return 1
  fi
  echo "Log in at: $(jq -r .verification_uri_complete <<<"$dev")   (code $(jq -r .user_code <<<"$dev"))" >&2
  interval=$(jq -r '.interval // 5' <<<"$dev")
  while [ -z "$token" ]; do
    sleep "$interval"
    resp=$(curl -sS -d client_id="$client" -d grant_type=urn:ietf:params:oauth:grant-type:device_code \
                -d device_code="$(jq -r .device_code <<<"$dev")" -d code_verifier="$verifier" \
                "$KEYCLOAK_ISSUER/protocol/openid-connect/token")
    case $(jq -r '.error // "ok"' <<<"$resp") in
      ok) token=$(jq -r .access_token <<<"$resp") ;;
      authorization_pending) ;;
      slow_down) interval=$((interval + 5)) ;;
      *) echo "login failed: $(jq -r '.error_description // .error' <<<"$resp")" >&2; return 1 ;;
    esac
  done
  printf '%s\n' "$token"
}

jwt_claims() {
  cut -d. -f2 <<<"$1" | tr '_-' '/+' \
    | awk '{ l = length($0) % 4; if (l) $0 = $0 substr("===", 1, 4 - l); print }' | base64 -d 2>/dev/null
}
