# Trino over its HTTP protocol, as the provisioner's Keycloak service account.
# Sourced by table.sh; needs bash, curl and jq (all in Meta Catalog's image).
#
#   trino_login              gets an access token (client credentials of
#                            $PROVISIONER_CLIENT_ID, audience trino); once per run
#   trino <sql>              runs one statement to its end: prints each result
#                            row as a JSON array, fails with Trino's message
#
# The token and the client secret reach curl through a config on a file
# descriptor, never its argument list: arguments are visible to every
# process of the container (ps).

TRINO_TOKEN=""

trino_login() {
  local resp secret
  # a mounted Secret: a new value arrives without a restart
  secret=${PROVISIONER_CLIENT_SECRET:-$(cat "$PROVISIONER_CLIENT_SECRET_FILE")}
  resp=$(curl -sS --max-time 30 -K <(printf 'user = "%s:%s"\n' "$PROVISIONER_CLIENT_ID" "$secret") \
    -d grant_type=client_credentials "$KEYCLOAK_TOKEN_URL") || fail "Keycloak at $KEYCLOAK_TOKEN_URL does not answer"
  TRINO_TOKEN=$(jq -r '.access_token // empty' <<<"$resp" 2>/dev/null) || true
  [ -n "$TRINO_TOKEN" ] || fail "no token for $PROVISIONER_CLIENT_ID from Keycloak: $(jq -r '.error_description // .error // .' <<<"$resp" 2>/dev/null || echo "$resp")"
}

# One HTTP exchange of the protocol; prints the body. 502/503/504 (the
# Gateway or a coordinator restarting) are retried for a minute.
trino_http() { # <method> <url> [<body>]
  local i code body
  body=$(mktemp)
  for i in $(seq 1 12); do
    code=$(curl -sS --max-time 60 -o "$body" -w '%{http_code}' -X "$1" \
      -K <(printf 'header = "Authorization: Bearer %s"\n' "$TRINO_TOKEN") \
      -H "X-Trino-Source: metacatalog" -H "X-Trino-Catalog: $LAKEHOUSE_CATALOG" \
      ${3:+--data-binary "$3"} "$2") || code=000
    case $code in
      200) cat "$body"; rm -f "$body"; return 0 ;;
      000|502|503|504) sleep 5 ;;
      *) fail "Trino answered HTTP $code: $(head -c 500 "$body")" ;;
    esac
  done
  rm -f "$body"
  fail "Trino at $TRINO_URL does not answer (HTTP $code)"
}

trino() { # <sql>
  local resp next
  [ -n "$TRINO_TOKEN" ] || trino_login
  resp=$(trino_http POST "$TRINO_URL/v1/statement" "$1")
  while :; do
    if jq -e '.error' >/dev/null <<<"$resp"; then
      fail "Trino: $(jq -r '.error.message' <<<"$resp")  [$1]"
    fi
    jq -c '.data // [] | .[]' <<<"$resp"
    next=$(jq -r '.nextUri // empty' <<<"$resp")
    [ -n "$next" ] || return 0
    # the first answers only say "queued": a short pause, not a busy loop
    jq -e '.data' >/dev/null <<<"$resp" || sleep 0.5
    resp=$(trino_http GET "$next")
  done
}

# A SQL string literal; only ever given values the caller has already
# checked against a pattern, so this is belt and braces.
sql_str() { printf "'%s'" "${1//\'/\'\'}"; }
