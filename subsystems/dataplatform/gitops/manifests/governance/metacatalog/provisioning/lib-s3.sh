# The platform bucket over S3, as Meta Catalog's provisioner. Sourced by
# table.sh after lib-trino.sh; needs bash, curl and jq.
#
#   s3_login                 temporary keys for the provisioner's own STS role
#                            (usr-service-account-$PROVISIONER_CLIENT_ID, rw on
#                            the prefix: make dataplatform-iam), from its
#                            Keycloak token (AssumeRoleWithWebIdentity); once
#                            per run
#   s3_mkdir <directory>     makes the directory: an empty object <directory>/
#   s3_keys <prefix>         every key under the prefix, one per line
#   s3_rmtree <directory>    deletes every object under <directory>/; sets
#                            S3_DELETED
#
# There is no permanent key: the keys last 15 minutes and are the role's.
# Keys and tokens reach curl through a config on a file descriptor, never
# its argument list.

: "${S3_ENDPOINT:?}" "${STS_ENDPOINT:?}" "${IAM_REGION:?}" "${IAM_ACCOUNT_ID:?}" "${LAKEHOUSE_BUCKET:?}"

S3_ACCESS_KEY="" S3_SECRET_KEY="" S3_SESSION_TOKEN=""

# The text of the first <tag> of a document.
s3_xml() { # <tag> <document>
  sed -n "s:.*<$1>\([^<]*\)</$1>.*:\1:p" <<<"$2" | head -1
}

s3_login() {
  local role resp code out
  [ -n "$TRINO_TOKEN" ] || trino_login
  role="arn:aws:iam::$IAM_ACCOUNT_ID:role/usr-service-account-$PROVISIONER_CLIENT_ID"
  out=$(mktemp)
  code=$(curl -sS --max-time 30 -o "$out" -w '%{http_code}' -X POST \
    --data-urlencode Action=AssumeRoleWithWebIdentity --data-urlencode Version=2011-06-15 \
    --data-urlencode "RoleArn=$role" --data-urlencode RoleSessionName=metacatalog \
    --data-urlencode DurationSeconds=900 \
    --data-urlencode "WebIdentityToken@"<(printf '%s' "$TRINO_TOKEN") "$STS_ENDPOINT/") || code=000
  resp=$(cat "$out"); rm -f "$out"
  [ "$code" = 200 ] || fail "Ceph STS refuses the provisioner its role $role (HTTP $code): $(s3_xml Code "$resp") $(s3_xml Message "$resp")"
  S3_ACCESS_KEY=$(s3_xml AccessKeyId "$resp")
  S3_SECRET_KEY=$(s3_xml SecretAccessKey "$resp")
  S3_SESSION_TOKEN=$(s3_xml SessionToken "$resp")
  [ -n "$S3_ACCESS_KEY" ] && [ -n "$S3_SECRET_KEY" ] && [ -n "$S3_SESSION_TOKEN" ] || fail "Ceph STS answered without keys for $role"
}

# One request: sets S3_CODE and S3_BODY. The path is what follows the
# bucket, already URL-encoded. No answer or a 5xx is retried for a minute.
s3_call() { # <method> <path and query> [<curl arguments>...]
  local method=$1 path=$2 out i
  shift 2
  [ -n "$S3_ACCESS_KEY" ] || s3_login
  out=$(mktemp)
  for i in 1 2 3 4 5 6; do
    S3_CODE=$(curl -sS --max-time 60 -o "$out" -w '%{http_code}' -X "$method" \
      --aws-sigv4 "aws:amz:$IAM_REGION:s3" \
      -K <(printf 'user = "%s:%s"\nheader = "x-amz-security-token: %s"\n' "$S3_ACCESS_KEY" "$S3_SECRET_KEY" "$S3_SESSION_TOKEN") \
      "$@" "$S3_ENDPOINT/$LAKEHOUSE_BUCKET/$path") || S3_CODE=000
    case $S3_CODE in 000|5??) sleep 10 ;; *) break ;; esac
  done
  S3_BODY=$(cat "$out"); rm -f "$out"
}

# A key as it goes into a URL: everything but letters, digits, - . _ ~ and
# the slashes between its parts.
s3_quote() { jq -rRn --arg k "$1" '$k | @uri | gsub("%2F"; "/")'; }

s3_mkdir() { # <directory>
  s3_call PUT "$(s3_quote "$1")/" -H 'Content-Length: 0'
  [ "$S3_CODE" = 200 ] || fail "making s3://$LAKEHOUSE_BUCKET/$1/ answered HTTP $S3_CODE: $(s3_xml Code "$S3_BODY") $(s3_xml Message "$S3_BODY")"
}

s3_keys() { # <prefix>
  local token="" query
  while :; do
    query="list-type=2&prefix=$(jq -rRn --arg k "$1" '$k | @uri')${token:+&continuation-token=$(jq -rRn --arg k "$token" '$k | @uri')}"
    s3_call GET "?$query"
    [ "$S3_CODE" = 200 ] || fail "listing s3://$LAKEHOUSE_BUCKET/$1 answered HTTP $S3_CODE: $(s3_xml Code "$S3_BODY") $(s3_xml Message "$S3_BODY")"
    { grep -o '<Key>[^<]*</Key>' <<<"$S3_BODY" || true; } \
      | sed -e 's:^<Key>::' -e 's:</Key>$::' -e 's/&quot;/"/g' -e "s/&apos;/'/g" -e 's/&lt;/</g' -e 's/&gt;/>/g' -e 's/&amp;/\&/g'
    [ "$(s3_xml IsTruncated "$S3_BODY")" = true ] || break
    token=$(s3_xml NextContinuationToken "$S3_BODY")
    [ -n "$token" ] || break
  done
}

s3_rmtree() { # <directory>
  local key keys
  S3_DELETED=0
  [[ $1 == */* ]] || fail "refusing to empty s3://$LAKEHOUSE_BUCKET/$1/: not a directory below the bucket's top level"
  keys=$(s3_keys "$1/")
  while IFS= read -r key; do
    [ -n "$key" ] || continue
    [[ $key == "$1/"* ]] || fail "the listing of $1/ holds $key: nothing deleted any further"
    s3_call DELETE "$(s3_quote "$key")"
    case $S3_CODE in 200|204|404) S3_DELETED=$((S3_DELETED + 1)) ;;
      *) fail "deleting s3://$LAKEHOUSE_BUCKET/$key answered HTTP $S3_CODE: $(s3_xml Code "$S3_BODY") $(s3_xml Message "$S3_BODY")" ;;
    esac
  done <<<"$keys"
}
