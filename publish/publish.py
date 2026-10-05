#!/usr/bin/env python3
"""The public copy of this repository (make publish-check | publish | publish-push).

The repository holds two layers:
  - the site layer: inventory/hosts.ini, inventory/known_hosts,
    inventory/group_vars/all/site.yml, and the files make configure renders
    from them (every <file> next to a <file>.in): this installation's names,
    addresses and keys. It stays in the deployment repository, which Argo CD
    reads, and is never published;
  - everything else: code, templates, docs, comments. It names no real site
    value (docs and examples use the example site of publish/example/).

The public copy is the committed tree (git archive HEAD) with publish/example/
in place of the site layer and the rendered files rendered again from it.
Before anything is committed or pushed it must hold none of the real site's
values (in any form: escaped, URL-encoded, a network prefix), node names, host
keys, the e-mail addresses of this repository's commits or the home directory,
and gitleaks must find nothing. The real site files are hashed before and
after: nothing here writes them.

  publish.py check [--no-gitleaks] [--no-syntax]
      build in a temporary directory, check, delete it (CI: no commit, no push)
  publish.py build [-m MESSAGE] [--dir DIR]
      build in DIR (default ../<this repository's name>-github, a Git
      repository of its own, created on first use), check, commit there
  publish.py push [--dir DIR] [--repo OWNER/NAME]
      check DIR's history, create the public GitHub repository on first use
      (gh), push

Commits of the public copy are authored with the GitHub account's noreply
address (gh api user), set in DIR's own Git configuration.
Standard library plus PyYAML (requirements.txt); make, Ansible, git; gh for
build and push; gitleaks or Docker for the secret scan.
"""
import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.parse

import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
EXAMPLE = os.path.join(ROOT, "publish", "example")
# the site layer: repository path -> its example in publish/example/
SITE_LAYER = {
    "inventory/group_vars/all/site.yml": "site.yml",
    "inventory/hosts.ini": "hosts.ini",
    "inventory/known_hosts": "known_hosts",
}
# in the copy's .git: the directory is one publish.py made, whose files it may delete
MARKER = "publish-copy"
GITLEAKS_IMAGE = "ghcr.io/gitleaks/gitleaks:v8.28.0@sha256:cdbb7c955abce02001a9f6c9f602fb195b7fadc1e812065883f695d1eeaba854"
# paths that must never be in the public copy, whatever .gitignore says
NEVER = re.compile(r"(^|/)(\.secrets|backups|bench-results|\.render)/|\.(age|pem|key|p12|pfx|kubeconfig)$|(^|/)id_(rsa|ecdsa|ed25519)")
# a node name that is also a common identifier (a variable, an option) is
# let through where it is used as one
CODE_WORD = re.compile(r"\b{w}\s*(=|\[|\.\w+\(|\)|,)|\bdef {w}\(|\b{w}\(|\({w}\b|-{w}="
                       r"|(==|!=|,)\s*{w}\b|\breturn\s+{w}\b")


def run(cmd, cwd=ROOT, check=True, capture=True, env=None):
    r = subprocess.run(cmd, cwd=cwd, text=True, capture_output=capture, env=env)
    if check and r.returncode != 0:
        out = ((r.stdout or "") + (r.stderr or "")).strip()
        if len(out) > 5000:
            out = out[:1500] + "\n[...]\n" + out[-3500:]
        sys.exit(f"FAILED: {' '.join(cmd)} (in {cwd})\n{out}")
    return r


def fail(msg):
    sys.exit(f"publish: {msg}")


# ---------------------------------------------------------------- the real site --

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f) or {}


def hosts_ini(path):
    """{inventory name: {var: value}} of every host line"""
    hosts = {}
    for line in open(path):
        line = line.strip()
        if not line or line[0] in "#;[":
            continue
        name, *pairs = line.split()
        hosts.setdefault(name, {}).update(p.split("=", 1) for p in pairs if "=" in p)
    return hosts


def known_hosts(path):
    """(hosts, key blobs) of a known_hosts file"""
    names, blobs = set(), set()
    for line in open(path):
        f = line.split()
        if len(f) >= 3 and not f[0].startswith("#"):
            names.update(f[0].split(","))
            blobs.add(f[2])
    return names, blobs


def forbidden():
    """({substring: why}, {node name: why}) of the real site, minus what the example shares"""
    subs = {}

    def add(text, why):
        text = str(text).strip()
        if len(text) >= 4:
            subs[text] = why
            if "." in text:
                subs[re.escape(text)] = why + ", regex-escaped"
            q = urllib.parse.quote(text, safe="")
            if q != text:
                subs[q] = why + ", URL-encoded"

    real = load_yaml(os.path.join(ROOT, "inventory/group_vars/all/site.yml"))
    example = load_yaml(os.path.join(EXAMPLE, "site.yml"))
    for key, value in real.items():
        if value == example.get(key):
            continue
        for v in value if isinstance(value, list) else [value]:
            if not isinstance(v, str) or not v:
                continue
            if key == "site_git_host_keys":
                host, _, blob = (v.split() + ["", "", ""])[:3]
                add(host, key)
                add(blob, key)
                continue
            add(v, key)
            m = re.match(r"^(\d+\.\d+\.\d+\.\d+)/(\d+)$", v)
            if m:   # an address or network: the address, the network's prefix
                add(m.group(1), key + " address")
                octets = max(int(m.group(2)) // 8, 1)
                subs[".".join(m.group(1).split(".")[:octets]) + "."] = key + " prefix"
            if key in ("site_dns_zone", "site_lan_domain", "site_platform_domain") and "." in v:
                label = v.split(".")[0] if key != "site_platform_domain" else v.split(".")[-2]
                if len(label) >= 5:   # the distinctive label, in whatever encoding
                    subs[label] = key + " label"
            if key == "site_git_repo":
                m = re.match(r"^[^@]+@([^:]+):", v)
                if m:
                    add(m.group(1), key + " host")

    # the inventory: names, addresses, host keys the example does not share
    real_hosts = hosts_ini(os.path.join(ROOT, "inventory/hosts.ini"))
    example_hosts = hosts_ini(os.path.join(EXAMPLE, "hosts.ini"))
    example_values = {v for h in example_hosts.values() for v in h.values()}
    nodes = {}
    for name, hv in real_hosts.items():
        if name not in example_hosts:
            nodes[name] = "inventory host"
        for var in ("ansible_host", "node_ip"):
            if var in hv and hv[var] not in example_values:
                add(hv[var], f"{name} {var}")
    names, blobs = known_hosts(os.path.join(ROOT, "inventory/known_hosts"))
    for b in blobs:
        add(b, "known_hosts key")
    for n in names:
        if n not in example_values and n not in example_hosts:
            add(n, "known_hosts host")

    # who made this repository's commits, where it lives
    mails = run(["git", "log", "--all", "--format=%ae%n%ce"]).stdout.split()
    for m in set(mails):
        if not m.endswith("@users.noreply.github.com"):
            add(m, "commit e-mail")
    for url in run(["git", "remote", "-v"]).stdout.split():
        if "@" in url or "://" in url:
            add(url, "private remote")
    home = os.path.expanduser("~")
    if home.startswith(("/Users/", "/home/")):   # a person's (CI runs as root: /root is a word)
        add(home, "home directory")
    return subs, nodes


# ---------------------------------------------------------------- the copy --

def site_hashes():
    out = {}
    for path in SITE_LAYER:
        with open(os.path.join(ROOT, path), "rb") as f:
            out[path] = hashlib.sha256(f.read()).hexdigest()
    out["git status"] = run(["git", "status", "--porcelain"]).stdout
    return out


def default_dir():
    return os.path.join(os.path.dirname(ROOT), os.path.basename(ROOT) + "-github")


def prepare(dest, create):
    """dest: empty but for its .git, a Git repository of its own outside this one"""
    dest = os.path.realpath(dest)
    if dest == ROOT or dest.startswith(ROOT + os.sep) or ROOT.startswith(dest + os.sep):
        fail(f"{dest} overlaps this repository: the copy must live outside it")
    if not os.path.exists(dest):
        if not create:
            fail(f"{dest} does not exist (make publish first)")
        os.makedirs(dest)
    if not os.path.isdir(os.path.join(dest, ".git")):
        if os.listdir(dest):
            fail(f"{dest} is not empty and not a Git repository")
        run(["git", "init", "-q", "-b", "main"], cwd=dest)
        with open(os.path.join(dest, ".git", MARKER), "w") as f:
            f.write("a public copy made by publish/publish.py: its files are replaced at each build\n")
    if not os.path.exists(os.path.join(dest, ".git", MARKER)):
        # every file but .git is deleted below: only in a copy this script made
        fail(f"{dest} is a Git repository this script did not create (no .git/{MARKER}): refusing to empty it")
    top = run(["git", "rev-parse", "--show-toplevel"], cwd=dest).stdout.strip()
    if os.path.realpath(top) != dest:
        fail(f"{dest} is inside another Git repository ({top})")
    # URLs only: the remote names (origin) and the (fetch)/(push) words are
    # the same in every repository
    def urls(cwd):
        return {f[1] for f in (l.split() for l in run(["git", "remote", "-v"], cwd=cwd).stdout.splitlines())
                if len(f) >= 2}
    for url in urls(dest) & urls(ROOT):
        fail(f"{dest} has this repository's remote {url}: refusing")
    for entry in os.listdir(dest):
        if entry != ".git":
            p = os.path.join(dest, entry)
            shutil.rmtree(p) if os.path.isdir(p) and not os.path.islink(p) else os.remove(p)
    return dest


def export(dest):
    """the committed tree, the example site, the rendered files rendered again"""
    p1 = subprocess.Popen(["git", "archive", "--format=tar", "HEAD"], cwd=ROOT, stdout=subprocess.PIPE)
    subprocess.run(["tar", "-x", "-f", "-", "-C", dest], stdin=p1.stdout, check=True)
    p1.stdout.close()
    if p1.wait() != 0:
        fail("git archive failed")
    for path, example in SITE_LAYER.items():
        shutil.copyfile(os.path.join(EXAMPLE, example), os.path.join(dest, path))
    # make in the copy must not inherit this make's command line (ARGS="--no-gitleaks"
    # would reach ansible-playbook)
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MFLAGS", "MAKELEVEL", "MAKEOVERRIDES")}
    # the collections this repository's lint installed, for syntax-check
    env["ANSIBLE_COLLECTIONS_PATH"] = os.pathsep.join(
        [os.path.join(ROOT, ".ansible", "collections"), os.path.expanduser("~/.ansible/collections")])
    print("rendering the templates with the example site (make configure)")
    run(["make", "configure", "ARGS="], cwd=dest, env=env)
    run(["git", "add", "-A"], cwd=dest)
    return env


# ---------------------------------------------------------------- the checks --

def scan(dest):
    files = [f for f in run(["git", "ls-files", "-z"], cwd=dest).stdout.split("\0") if f]
    if len(files) < 50:
        fail(f"only {len(files)} files in the copy: something went wrong")
    subs, nodes = forbidden()
    sub_re = re.compile("|".join(re.escape(s) for s in sorted(subs, key=len, reverse=True)), re.I)
    node_re = re.compile(r"(?<![\w-])(" + "|".join(re.escape(n) for n in nodes) + r")(?![\w-])", re.I) if nodes else None
    code = {n: re.compile(CODE_WORD.pattern.format(w=re.escape(n))) for n in nodes}
    hits = []
    for path in files:
        if NEVER.search(path):
            hits.append(f"{path}: a path that is never published")
        if sub_re.search(path) or (node_re and node_re.search(path)):
            hits.append(f"{path}: the path names the real site")
        try:
            with open(os.path.join(dest, path), encoding="utf-8") as f:
                lines = f.read().splitlines()
        except UnicodeDecodeError:
            with open(os.path.join(dest, path), "rb") as f:
                data = f.read().decode("latin-1")
            if sub_re.search(data):
                hits.append(f"{path}: (binary) {subs.get(sub_re.search(data).group(0), 'real value')}")
            continue
        except (IsADirectoryError, FileNotFoundError):
            continue
        for n, line in enumerate(lines, 1):
            for m in sub_re.finditer(line):
                why = next((w for s, w in subs.items() if s.lower() == m.group(0).lower()), "real value")
                hits.append(f"{path}:{n}: {why}")
            if node_re:
                for m in node_re.finditer(line):
                    name = next(k for k in nodes if k.lower() == m.group(1).lower())
                    if not code[name].search(line[max(0, m.start() - 6):m.end() + 12]):
                        hits.append(f"{path}:{n}: node name {m.group(1)!r}")
    if hits:
        print(f"The copy holds values of the real site ({len(hits)}):", file=sys.stderr)
        for h in hits[:200]:
            print("  " + h, file=sys.stderr)
        fail("not published: write these with the example site's values (publish/example/)")
    print(f"no real site value in the copy ({len(files)} files, {len(subs)} values, {len(nodes)} node names)")


def gitleaks(dest, mode):
    if shutil.which("gitleaks"):
        cmd = ["gitleaks", mode, dest]
    elif shutil.which("docker"):
        # the files belong to another uid in the container: git wants them declared safe
        cmd = ["docker", "run", "--rm", "-v", f"{dest}:/scan:ro", "-e", "GIT_CONFIG_COUNT=1",
               "-e", "GIT_CONFIG_KEY_0=safe.directory", "-e", "GIT_CONFIG_VALUE_0=*",
               GITLEAKS_IMAGE, mode, "/scan"]
    else:
        fail("gitleaks not found: install it (brew install gitleaks) or Docker, or pass --no-gitleaks")
    r = run(cmd + ["--no-banner", "--redact", "--verbose", "--exit-code", "1"], check=False)
    if r.returncode != 0:
        print((r.stdout + r.stderr)[-6000:], file=sys.stderr)
        fail(f"gitleaks ({mode}) found something, or did not run")
    print(f"gitleaks ({mode}): no leaks")


def checks(dest, env, args):
    scan(dest)
    print("the example site is consistent (site_check.py)")
    run(["python3", "subsystems/dataplatform/files/site_check.py"], cwd=dest)
    if not args.no_syntax:
        print("every playbook parses with the example inventory (make syntax-check)")
        run(["make", "syntax-check", "ARGS="], cwd=dest, env=env)
    if not args.no_gitleaks:
        gitleaks(dest, "dir")


def build(args, dest, create):
    if run(["git", "status", "--porcelain", "--untracked-files=no"]).stdout.strip():
        fail("uncommitted changes: the copy is made from HEAD, commit first")
    before = site_hashes()
    try:
        dest = prepare(dest, create)
        head = run(["git", "rev-parse", "--short", "HEAD"]).stdout.strip()
        print(f"public copy of {head} in {dest}")
        env = export(dest)
        checks(dest, env, args)
    finally:
        if site_hashes() != before:
            fail("THE REAL SITE FILES OR THE WORKING TREE CHANGED during the build: check git status")
    return dest


# ---------------------------------------------------------------- commit, push --

def identity(dest):
    """the GitHub account's name and noreply address, in dest's Git configuration"""
    email = run(["git", "config", "--local", "user.email"], cwd=dest, check=False).stdout.strip()
    if email.endswith("@users.noreply.github.com"):
        return email
    r = run(["gh", "api", "user", "--jq", '"\\(.id) \\(.login) \\(.name // .login)"'], check=False)
    if r.returncode != 0:
        fail("gh api user failed: log in with gh auth login (the copy's commits use the noreply address)")
    uid, login, name = r.stdout.strip().split(" ", 2)
    email = f"{uid}+{login}@users.noreply.github.com"
    run(["git", "config", "--local", "user.name", name], cwd=dest)
    run(["git", "config", "--local", "user.email", email], cwd=dest)
    return email


def commit(dest, message):
    if run(["git", "diff", "--cached", "--quiet"], cwd=dest, check=False).returncode == 0:
        print("nothing changed since the last published commit")
        return
    first = run(["git", "rev-parse", "--verify", "-q", "HEAD"], cwd=dest, check=False).returncode != 0
    identity(dest)
    msg = message or ("Initial public release" if first else "Update")
    run(["git", "commit", "-q", "-m", msg], cwd=dest)
    stat = run(["git", "show", "--stat", "--format=%h %an <%ae> %s", "HEAD"], cwd=dest).stdout.splitlines()
    print("\n".join(stat[:1] + stat[-1:]))


def push(args):
    dest = os.path.realpath(args.dir)
    if not os.path.isdir(os.path.join(dest, ".git")):
        fail(f"{dest} is not a public copy yet: make publish first")
    if run(["git", "status", "--porcelain"], cwd=dest).stdout.strip():
        fail(f"{dest} has uncommitted changes: make publish again")
    email = identity(dest)
    authors = set(run(["git", "log", "--format=%ae%n%ce"], cwd=dest).stdout.split())
    if authors - {email}:
        fail(f"commits in {dest} by other addresses than {email}: {', '.join(sorted(authors - {email}))}")
    scan(dest)
    gitleaks(dest, "git")
    remotes = run(["git", "remote"], cwd=dest).stdout.split()
    if "origin" not in remotes:
        repo = args.repo or (run(["gh", "api", "user", "--jq", ".login"]).stdout.strip() + "/"
                             + os.path.basename(ROOT))
        print(f"creating the public repository github.com/{repo}")
        run(["gh", "repo", "create", repo, "--public", "--source", dest, "--remote", "origin",
             "--description", "Ansible for a Turing Pi 2 home cluster: Ceph (cephadm), HA k3s, "
             "and a Trino/Spark data platform installed by Argo CD"], cwd=dest)
    url = run(["git", "remote", "get-url", "origin"], cwd=dest).stdout.strip()
    if url.startswith("https://github.com/"):
        # gh's login, for this repository only (gh repo create picks HTTPS by default)
        run(["git", "config", "--local", "--replace-all", "credential.https://github.com.helper", ""], cwd=dest)
        run(["git", "config", "--local", "--add", "credential.https://github.com.helper",
             "!gh auth git-credential"], cwd=dest)
    print(f"pushing to {url}")
    run(["git", "push", "-u", "origin", "main"], cwd=dest, capture=False)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("check", "build"):
        p = sub.add_parser(name)
        p.add_argument("--no-gitleaks", action="store_true", help="skip the secret scan (CI has no Docker)")
        p.add_argument("--no-syntax", action="store_true", help="skip make syntax-check in the copy")
        if name == "build":
            p.add_argument("--dir", default=default_dir())
            p.add_argument("-m", "--message", help="the public commit's message")
    p = sub.add_parser("push")
    p.add_argument("--dir", default=default_dir())
    p.add_argument("--repo", help="OWNER/NAME on GitHub, first push only (default: your account/" +
                   os.path.basename(ROOT) + ")")
    args = ap.parse_args()
    if args.cmd == "check":
        tmp = tempfile.mkdtemp(prefix="publish-check-")
        try:
            build(args, tmp, create=True)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        print("the public copy would be clean")
    elif args.cmd == "build":
        dest = build(args, args.dir, create=True)
        commit(dest, args.message)
        print(f"next: make publish-push (pushes {dest})")
    else:
        push(args)


if __name__ == "__main__":
    main()
