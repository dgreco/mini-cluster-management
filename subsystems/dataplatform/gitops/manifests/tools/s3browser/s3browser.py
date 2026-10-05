#!/usr/bin/env python3
"""s3browser: a read-only S3 browser for the platform's administrators.

Nobody's key is stored here. An administrator logs in with Keycloak (client
s3browser, authorization code with PKCE); the app keeps the login's tokens
server-side and exchanges the access token at Ceph's STS
(AssumeRoleWithWebIdentity) for ROLE_ARN, the admin role, so Ceph checks the
token, its groups and the role's policy, and logs every request under the
administrator's name (the session name). Only members of ADMIN_GROUPS are let
in; Ceph would refuse anybody else anyway.

What it shows:
- the buckets: the account's own (ListBuckets) and every other bucket on Ceph
  listed in INDEX_KEY of INDEX_BUCKET, the index make dataplatform-ceph writes
  (radosgw-admin sees every bucket; an STS session only its account's). The
  admin role may read those because make dataplatform-ceph also adds a
  read-only statement for it to their bucket policies.
- a bucket's directories and objects (ListObjectsV2 with '/' as delimiter,
  paged), an object's metadata (HeadObject), and a download link: a
  presigned GET valid PRESIGN_SECONDS, made with the session's keys.
Nothing is written: no upload, no delete.

Logout ends the app's session and the Keycloak session (RP-initiated logout
with id_token_hint), then comes back to /logged-out.

Standard library only: SigV4 signing, OIDC and the pages are here.
"""
import base64
import datetime
import hashlib
import hmac
import html
import http.cookies
import http.server
import json
import os
import secrets
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

# The site's values come from the Deployment (s3browser.yaml); main() refuses to
# start without them
REQUIRED = ("ISSUER", "PUBLIC_URL", "S3_ENDPOINT", "S3_REGION", "ROLE_ARN", "ADMIN_GROUPS", "INDEX_BUCKET", "CLIENT_SECRET")
ISSUER = os.environ.get("ISSUER", "").rstrip("/")
CLIENT_ID = os.environ.get("CLIENT_ID", "s3browser")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "")
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "").rstrip("/")
REGION = os.environ.get("S3_REGION", "")
ROLE_ARN = os.environ.get("ROLE_ARN", "")
ADMIN_GROUPS = [g for g in os.environ.get("ADMIN_GROUPS", "").split(",") if g]
INDEX_BUCKET = os.environ.get("INDEX_BUCKET", "")
INDEX_KEY = os.environ.get("INDEX_KEY", "system/admin-browser/buckets.json")
PRESIGN_SECONDS = int(os.environ.get("PRESIGN_SECONDS", "300"))
STS_SECONDS = int(os.environ.get("STS_SECONDS", "3600"))
SESSION_IDLE = int(os.environ.get("SESSION_IDLE", "28800"))    # 8 h without a request ends a session
PAGE = int(os.environ.get("PAGE", "500"))
PORT = int(os.environ.get("PORT", "8080"))
COOKIE = "s3browser"

sessions: dict = {}          # session id -> {user, id_token, refresh_token, access_token, creds, seen}
pending: dict = {}           # state -> {verifier, created, next}
lock = threading.Lock()


def log(msg: str) -> None:
    print(time.strftime("%Y-%m-%dT%H:%M:%S ") + msg, flush=True)


# ---------------------------------------------------------------- HTTP client ----
def http_req(method: str, url: str, data=None, headers=None, timeout=30):
    """(status, headers, body bytes); never raises on an HTTP error status."""
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def form_post(url: str, **fields):
    status, _, body = http_req("POST", url, urllib.parse.urlencode(fields).encode(),
                               {"Content-Type": "application/x-www-form-urlencoded"})
    try:
        return status, json.loads(body)
    except ValueError:
        return status, {"error": body[:300].decode(errors="replace")}


def jwt_claims(token: str) -> dict:
    """Claims of a token the app got itself from Keycloak's token endpoint (TLS):
    read, not verified; Ceph verifies the signature at STS."""
    part = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))


# ---------------------------------------------------------------- SigV4 ----
def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _signing_key(secret: str, day: str) -> bytes:
    k = _sign(("AWS4" + secret).encode(), day)
    for part in (REGION, "s3", "aws4_request"):
        k = _sign(k, part)
    return k


def _canon_uri(bucket: str, key: str = "") -> str:
    path = "/" + bucket + ("/" + key if key else "")
    return urllib.parse.quote(path, safe="/-_.~")


def _canon_query(params: dict) -> str:
    return "&".join(f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(str(v), safe='-_.~')}"
                    for k, v in sorted(params.items()))


def s3(creds: dict, method: str, bucket: str = "", key: str = "", params=None):
    """A signed S3 request (path style, empty body): (status, headers, body)."""
    params = params or {}
    host = urllib.parse.urlparse(S3_ENDPOINT).netloc
    now = datetime.datetime.now(datetime.timezone.utc)
    amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    payload = hashlib.sha256(b"").hexdigest()
    hdrs = {"host": host, "x-amz-content-sha256": payload, "x-amz-date": amz}
    if creds.get("SessionToken"):
        hdrs["x-amz-security-token"] = creds["SessionToken"]
    uri = _canon_uri(bucket, key) if bucket else "/"
    signed = ";".join(sorted(hdrs))
    canonical = "\n".join([method, uri, _canon_query(params),
                           "".join(f"{k}:{hdrs[k]}\n" for k in sorted(hdrs)), signed, payload])
    scope = f"{day}/{REGION}/s3/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    sig = hmac.new(_signing_key(creds["SecretAccessKey"], day), to_sign.encode(), hashlib.sha256).hexdigest()
    hdrs["Authorization"] = (f"AWS4-HMAC-SHA256 Credential={creds['AccessKeyId']}/{scope}, "
                             f"SignedHeaders={signed}, Signature={sig}")
    del hdrs["host"]
    q = _canon_query(params)
    return http_req(method, S3_ENDPOINT + uri + ("?" + q if q else ""), headers=hdrs)


def presign(creds: dict, bucket: str, key: str, filename: str) -> str:
    """A presigned GET URL valid PRESIGN_SECONDS, which downloads as filename."""
    host = urllib.parse.urlparse(S3_ENDPOINT).netloc
    now = datetime.datetime.now(datetime.timezone.utc)
    amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    scope = f"{day}/{REGION}/s3/aws4_request"
    params = {"X-Amz-Algorithm": "AWS4-HMAC-SHA256", "X-Amz-Credential": f"{creds['AccessKeyId']}/{scope}",
              "X-Amz-Date": amz, "X-Amz-Expires": str(PRESIGN_SECONDS), "X-Amz-SignedHeaders": "host",
              "response-content-disposition": f'attachment; filename="{filename}"'}
    if creds.get("SessionToken"):
        params["X-Amz-Security-Token"] = creds["SessionToken"]
    uri = _canon_uri(bucket, key)
    canonical = "\n".join(["GET", uri, _canon_query(params), f"host:{host}\n", "host", "UNSIGNED-PAYLOAD"])
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    params["X-Amz-Signature"] = hmac.new(_signing_key(creds["SecretAccessKey"], day), to_sign.encode(),
                                         hashlib.sha256).hexdigest()
    return f"{S3_ENDPOINT}{uri}?{_canon_query(params)}"


def xml(body: bytes) -> ET.Element:
    """An S3 answer, its tags without the XML namespace."""
    root = ET.fromstring(body)
    for e in root.iter():
        e.tag = e.tag.split("}", 1)[-1]
    return root


def s3_error(body: bytes) -> str:
    try:
        return xml(body).findtext("Code") or "error"
    except ET.ParseError:
        return body[:120].decode(errors="replace") or "error"


# ---------------------------------------------------------------- STS ----
def sts(access_token: str, user: str) -> dict:
    status, _, body = http_req("POST", S3_ENDPOINT + "/", urllib.parse.urlencode({
        "Action": "AssumeRoleWithWebIdentity", "Version": "2011-06-15", "RoleArn": ROLE_ARN,
        "RoleSessionName": user, "DurationSeconds": str(STS_SECONDS), "WebIdentityToken": access_token}).encode(),
        {"Content-Type": "application/x-www-form-urlencoded"})
    root = ET.fromstring(body)
    v = {e.tag.split("}")[-1]: (e.text or "") for e in root.iter()}
    if status != 200 or "AccessKeyId" not in v:
        raise PermissionError(f"Ceph STS refused {ROLE_ARN.rsplit('/', 1)[-1]}: {v.get('Code', status)}")
    # RGW writes nanoseconds (2026-09-29T14:12:20.284249210Z): the seconds are enough
    exp = datetime.datetime.strptime(v["Expiration"][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
    return {"AccessKeyId": v["AccessKeyId"], "SecretAccessKey": v["SecretAccessKey"],
            "SessionToken": v["SessionToken"], "expires": exp.timestamp()}


def credentials(sess: dict) -> dict:
    """The session's STS keys, renewed with the refresh token when they run out."""
    if sess["creds"]["expires"] - time.time() > 60:
        return sess["creds"]
    status, tok = form_post(f"{ISSUER}/protocol/openid-connect/token", grant_type="refresh_token",
                            client_id=CLIENT_ID, client_secret=CLIENT_SECRET, refresh_token=sess["refresh_token"])
    if status != 200:
        raise LookupError("login expired")
    sess["access_token"] = tok["access_token"]
    sess["refresh_token"] = tok.get("refresh_token", sess["refresh_token"])
    sess["id_token"] = tok.get("id_token", sess["id_token"])
    sess["creds"] = sts(sess["access_token"], sess["user"])
    return sess["creds"]


# ---------------------------------------------------------------- pages ----
STYLE = """
:root{--bg:#fff;--fg:#1d2330;--muted:#667085;--line:#e4e7ec;--accent:#2f5bd3;--band:#f5f7fa;--bad:#b42318}
@media (prefers-color-scheme:dark){:root{--bg:#12151c;--fg:#e6e8ee;--muted:#98a2b3;--line:#2a303c;--accent:#7ea2ff;--band:#1a1f29;--bad:#ff8a80}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif}
header{display:flex;gap:16px;align-items:center;justify-content:space-between;padding:12px 20px;border-bottom:1px solid var(--line);background:var(--band)}
header b{font-size:15px}header .who{color:var(--muted);font-size:13px}main{padding:16px 20px;max-width:1200px}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--muted);font-weight:500;font-size:12px;text-transform:uppercase;letter-spacing:.03em}td.n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
.crumbs{margin:0 0 12px;font-size:15px;word-break:break-all}.muted{color:var(--muted)}.err{color:var(--bad)}
button,.btn{font:inherit;padding:6px 12px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--fg);cursor:pointer}
dl{display:grid;grid-template-columns:max-content 1fr;gap:6px 16px}dt{color:var(--muted)}dd{margin:0;word-break:break-all}
@media (max-width:640px){header{flex-wrap:wrap}td.d{display:none}th.d{display:none}}
"""


def human(n) -> str:
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


def page(title: str, body: str, sess=None) -> bytes:
    who = ""
    if sess:
        left = max(0, int((sess["creds"]["expires"] - time.time()) / 60))
        who = (f'<div class="who">{html.escape(sess["user"])} as {html.escape(ROLE_ARN.rsplit("/", 1)[-1])}'
               f' &middot; keys renew in {left} min &nbsp;<form method="post" action="/logout" style="display:inline">'
               f'<button type="submit">Log out</button></form></div>')
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<title>{html.escape(title)} - S3 browser</title><style>{STYLE}</style></head><body>'
            f'<header><b><a href="/">S3 browser</a></b>{who}</header><main>{body}</main></body></html>').encode()


def bucket_index(creds: dict) -> list:
    """[{name, owner, objects, size, note}] of every bucket: the index of
    make dataplatform-ceph, completed with the account's own ListBuckets."""
    out = {}
    status, _, body = s3(creds, "GET", INDEX_BUCKET, INDEX_KEY)
    if status == 200:
        for b in json.loads(body).get("buckets", []):
            out[b["name"]] = b
    status, _, body = s3(creds, "GET")
    if status == 200:
        for e in xml(body).iter("Bucket"):
            name = e.findtext("Name")
            out.setdefault(name, {"name": name, "owner": "(this account)"})
    return sorted(out.values(), key=lambda b: b["name"])


def page_buckets(sess: dict) -> bytes:
    rows = []
    for b in bucket_index(credentials(sess)):
        note = f' <span class="muted">{html.escape(b["note"])}</span>' if b.get("note") else ""
        rows.append(f'<tr><td><a href="/b/{urllib.parse.quote(b["name"])}/">{html.escape(b["name"])}</a>{note}</td>'
                    f'<td>{html.escape(str(b.get("owner", "")))}</td><td class="n">{b.get("objects", "")}</td>'
                    f'<td class="n">{human(b["size"]) if "size" in b else ""}</td></tr>')
    body = ('<p class="crumbs">Buckets</p><table><tr><th>Bucket</th><th>Owner</th><th class="n">Objects</th>'
            '<th class="n">Size</th></tr>' + "".join(rows) + "</table>"
            '<p class="muted">Other buckets appear here after make dataplatform-ceph (the index and the '
            'read grant of the admin role are written there).</p>')
    return page("Buckets", body, sess)


def crumbs(bucket: str, prefix: str) -> str:
    parts = [f'<a href="/">Buckets</a> / <a href="/b/{urllib.parse.quote(bucket)}/">{html.escape(bucket)}</a>']
    acc = ""
    for p in [p for p in prefix.split("/") if p]:
        acc += p + "/"
        parts.append(f'<a href="/b/{urllib.parse.quote(bucket)}/?prefix={urllib.parse.quote(acc)}">{html.escape(p)}</a>')
    return '<p class="crumbs">' + " / ".join(parts) + "</p>"


def page_listing(sess: dict, bucket: str, prefix: str, token: str) -> bytes:
    params = {"list-type": "2", "delimiter": "/", "prefix": prefix, "max-keys": str(PAGE)}
    if token:
        params["continuation-token"] = token
    status, _, body = s3(credentials(sess), "GET", bucket, params=params)
    if status != 200:
        return page(bucket, crumbs(bucket, prefix) + f'<p class="err">Ceph refused the listing: {html.escape(s3_error(body))}</p>', sess)
    root = xml(body)
    rows = []
    for cp in root.iter("CommonPrefixes"):
        p = cp.findtext("Prefix")
        rows.append(f'<tr><td><a href="/b/{urllib.parse.quote(bucket)}/?prefix={urllib.parse.quote(p)}">'
                    f'{html.escape(p[len(prefix):])}</a></td><td class="n"></td><td class="d"></td></tr>')
    for c in root.iter("Contents"):
        k = c.findtext("Key")
        if k == prefix:
            continue
        rows.append(f'<tr><td><a href="/o/{urllib.parse.quote(bucket)}?key={urllib.parse.quote(k)}">{html.escape(k[len(prefix):])}</a></td>'
                    f'<td class="n">{human(c.findtext("Size"))}</td>'
                    f'<td class="d">{html.escape((c.findtext("LastModified") or "")[:19].replace("T", " "))}</td></tr>')
    more = ""
    if root.findtext("IsTruncated") == "true":
        nxt = root.findtext("NextContinuationToken")
        more = (f'<p><a class="btn" href="/b/{urllib.parse.quote(bucket)}/?prefix={urllib.parse.quote(prefix)}'
                f'&token={urllib.parse.quote(nxt)}">Next {PAGE}</a></p>')
    body = (crumbs(bucket, prefix) + '<table><tr><th>Name</th><th class="n">Size</th><th class="d">Modified (UTC)</th></tr>'
            + ("".join(rows) or '<tr><td colspan="3" class="muted">empty</td></tr>') + "</table>" + more)
    return page(f"{bucket}/{prefix}", body, sess)


def page_object(sess: dict, bucket: str, key: str) -> bytes:
    status, hdrs, body = s3(credentials(sess), "HEAD", bucket, key)
    prefix = key.rsplit("/", 1)[0] + "/" if "/" in key else ""
    if status != 200:
        return page(key, crumbs(bucket, prefix) + f'<p class="err">Ceph refused: HTTP {status}</p>', sess)
    h = {k.lower(): v for k, v in hdrs.items()}
    meta = "".join(f"<dt>{html.escape(k[11:])}</dt><dd>{html.escape(v)}</dd>" for k, v in sorted(h.items()) if k.startswith("x-amz-meta-"))
    body = (crumbs(bucket, prefix) + f'<h3 style="word-break:break-all">{html.escape(key.rsplit("/", 1)[-1])}</h3><dl>'
            f'<dt>Key</dt><dd>{html.escape(key)}</dd><dt>Size</dt><dd>{human(h.get("content-length"))} '
            f'({html.escape(h.get("content-length", "?"))} bytes)</dd><dt>Type</dt><dd>{html.escape(h.get("content-type", ""))}</dd>'
            f'<dt>Modified</dt><dd>{html.escape(h.get("last-modified", ""))}</dd><dt>ETag</dt><dd>{html.escape(h.get("etag", ""))}</dd>'
            f'{meta}</dl><p><a class="btn" href="/download/{urllib.parse.quote(bucket)}?key={urllib.parse.quote(key)}">Download</a> '
            f'<span class="muted">a link valid {PRESIGN_SECONDS // 60} minutes, made with your session\'s keys</span></p>')
    return page(key, body, sess)


# ---------------------------------------------------------------- handler ----
class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "s3browser"

    def log_message(self, fmt, *args):
        pass

    def send(self, status: int, body: bytes = b"", ctype="text/html; charset=utf-8", headers=None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self' "
                         + ISSUER.split("/realms/")[0] + "; frame-ancestors 'none'")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def redirect(self, url: str, cookie: str = ""):
        self.send(302, b"", headers={"Location": url, **({"Set-Cookie": cookie} if cookie else {})})

    def session(self):
        c = http.cookies.SimpleCookie(self.headers.get("Cookie", ""))
        sid = c[COOKIE].value if COOKIE in c else ""
        with lock:
            sess = sessions.get(sid)
            if sess and time.time() - sess["seen"] > SESSION_IDLE:
                sessions.pop(sid, None)
                sess = None
            if sess:
                sess["seen"] = time.time()
        return sid, sess

    def login(self):
        state, verifier = secrets.token_urlsafe(24), secrets.token_urlsafe(64)
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
        with lock:
            for s in [s for s, p in pending.items() if time.time() - p["created"] > 600]:
                pending.pop(s)
            pending[state] = {"verifier": verifier, "created": time.time(), "next": self.path if self.path != "/login" else "/"}
        self.redirect(f"{ISSUER}/protocol/openid-connect/auth?" + urllib.parse.urlencode({
            "client_id": CLIENT_ID, "response_type": "code", "scope": "openid", "redirect_uri": PUBLIC_URL + "/callback",
            "state": state, "code_challenge": challenge, "code_challenge_method": "S256"}))

    def callback(self, q: dict):
        state = q.get("state", [""])[0]
        with lock:
            p = pending.pop(state, None)
        if not p or "code" not in q:
            return self.send(400, page("Login failed", '<p class="err">The login answer did not match a login of this '
                                       'browser (or it was refused).</p><p><a href="/">Try again</a></p>'))
        status, tok = form_post(f"{ISSUER}/protocol/openid-connect/token", grant_type="authorization_code",
                                client_id=CLIENT_ID, client_secret=CLIENT_SECRET, code=q["code"][0],
                                redirect_uri=PUBLIC_URL + "/callback", code_verifier=p["verifier"])
        if status != 200:
            return self.send(502, page("Login failed", f'<p class="err">Keycloak: {html.escape(str(tok.get("error_description", tok.get("error"))))}</p>'))
        claims = jwt_claims(tok["access_token"])
        user, groups = claims.get("preferred_username", "?"), claims.get("groups") or []
        if not set(groups) & set(ADMIN_GROUPS):
            log(f"refused {user}: not in {ADMIN_GROUPS}")
            logout = f"{ISSUER}/protocol/openid-connect/logout?" + urllib.parse.urlencode(
                {"id_token_hint": tok.get("id_token", ""), "post_logout_redirect_uri": PUBLIC_URL + "/logged-out"})
            return self.send(403, page("Not an administrator", f'<p class="err">{html.escape(user)} is not in '
                                       f'{html.escape(", ".join(ADMIN_GROUPS))}: this browser is for the platform\'s administrators.</p>'
                                       f'<p><a class="btn" href="{html.escape(logout)}">Log out and use another account</a></p>'))
        try:
            creds = sts(tok["access_token"], user)
        except (PermissionError, ET.ParseError) as e:
            return self.send(403, page("Refused", f'<p class="err">{html.escape(str(e))}</p>'))
        sid = secrets.token_urlsafe(32)
        with lock:
            for s in [s for s, v in sessions.items() if time.time() - v["seen"] > SESSION_IDLE]:
                sessions.pop(s)
            sessions[sid] = {"user": user, "id_token": tok.get("id_token", ""), "refresh_token": tok.get("refresh_token", ""),
                             "access_token": tok["access_token"], "creds": creds, "seen": time.time()}
        log(f"login {user}")
        self.redirect(p["next"], f"{COOKIE}={sid}; Path=/; HttpOnly; Secure; SameSite=Lax")

    def logout(self):
        sid, sess = self.session()
        with lock:
            sessions.pop(sid, None)
        clear = f"{COOKIE}=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0"
        if sess:
            log(f"logout {sess['user']}")
            return self.redirect(f"{ISSUER}/protocol/openid-connect/logout?" + urllib.parse.urlencode(
                {"id_token_hint": sess["id_token"], "post_logout_redirect_uri": PUBLIC_URL + "/logged-out"}), clear)
        self.redirect("/logged-out", clear)

    def do_POST(self):
        if self.path == "/logout":
            return self.logout()
        self.send(404, page("Not found", "<p>Not found.</p>"))

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        if u.path == "/healthz":
            return self.send(200, b"ok", "text/plain")
        if u.path == "/callback":
            return self.callback(q)
        if u.path == "/logged-out":
            return self.send(200, page("Logged out", '<p>You are logged out of the S3 browser and of Keycloak.</p>'
                                       '<p><a class="btn" href="/">Log in</a></p>'))
        if u.path == "/logout":
            return self.logout()
        sid, sess = self.session()
        if not sess:
            return self.login()
        try:
            if u.path == "/":
                return self.send(200, page_buckets(sess))
            if u.path.startswith("/b/"):
                bucket = urllib.parse.unquote(u.path[3:]).rstrip("/")
                return self.send(200, page_listing(sess, bucket, q.get("prefix", [""])[0], q.get("token", [""])[0]))
            if u.path.startswith("/o/"):
                return self.send(200, page_object(sess, urllib.parse.unquote(u.path[3:]), q.get("key", [""])[0]))
            if u.path.startswith("/download/"):
                bucket, key = urllib.parse.unquote(u.path[10:]), q.get("key", [""])[0]
                log(f"download {sess['user']} s3://{bucket}/{key}")
                return self.redirect(presign(credentials(sess), bucket, key, key.rsplit("/", 1)[-1].replace('"', "")))
        except LookupError:
            with lock:
                sessions.pop(sid, None)
            return self.login()
        except (PermissionError, ET.ParseError, OSError) as e:
            return self.send(502, page("Error", f'<p class="err">{html.escape(str(e))}</p>', sess))
        self.send(404, page("Not found", "<p>Not found.</p>", sess))


def main() -> None:
    missing = [name for name in REQUIRED if not os.environ.get(name)]
    if missing:
        sys.exit(f"not set: {', '.join(missing)}")
    log(f"s3browser on :{PORT}: {PUBLIC_URL}, role {ROLE_ARN}, admins {ADMIN_GROUPS}, S3 {S3_ENDPOINT}")
    http.server.ThreadingHTTPServer(("", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
