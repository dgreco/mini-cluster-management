#!/usr/bin/env python3
"""Build every Argo CD Application of the data platform locally and check it.

make dataplatform-render. Needs kubectl (its built-in kustomize) and helm;
charts are fetched from their repositories, the cluster is never contacted.

Checks:
- every YAML file under gitops/ parses;
- every Application in gitops/apps (and the root app) points at the repository
  and branch of inventory/group_vars/all/site.yml;
- every Kustomize path builds (kubectl kustomize --enable-helm) and every chart
  renders (helm template) with its values files;
- (the site's values themselves are make configure's and make site-check's:
  the files carrying them are rendered from .in templates, and no other file
  may hold one);
- metrics reach VictoriaMetrics only through OpenTelemetry: its render has no
  scrape configuration (promscrape), no vmagent is rendered anywhere, and no
  rendered object but the OTel cluster collector names a write endpoint of it;

Rendered output goes to .render/ (git-ignored) for inspection, with a report of
the run, .render/report.html (a single file: the checks, every Application by
sync wave with its sources and objects, the files carrying the platform's
values, the errors).
"""
import collections
import datetime
import glob
import html
import os
import re
import subprocess
import sys

try:
    import yaml
except ImportError:
    sys.exit("PyYAML is needed: pip3 install pyyaml")

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
GITOPS = os.path.join(ROOT, "subsystems", "dataplatform", "gitops")
OUT = os.path.join(ROOT, ".render")
KUBE_VERSION = "1.36.0"


def site_settings() -> dict:
    """The site's settings (inventory/group_vars/all/site.yml: plain YAML)."""
    with open(os.path.join(ROOT, "inventory", "group_vars", "all", "site.yml")) as f:
        return yaml.safe_load(f) or {}


SITE = site_settings()
REPO = SITE["site_git_repo"]
REVISION = SITE["site_git_revision"]
DOMAIN = SITE["site_platform_domain"]
S3_ENDPOINT = "https://" + SITE["site_s3_host"]
S3_REGION = SITE["site_s3_region"]
ACCOUNT = SITE["site_rgw_account_id"]
errors: list = []
# what the report shows: the global checks, the applications, where each
# error was raised
REPORT: dict = {"checks": [], "apps": [], "where": []}


def error(msg: str) -> None:
    errors.append(msg)
    REPORT["where"].append(REPORT.get("section", ""))
    print(f"  ERROR {msg}")


def run(cmd: list, out_file: str) -> list:
    """Runs a render command, keeps its output in out_file; the objects, or
    None when it failed."""
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        error(f"{' '.join(cmd)}\n{r.stderr.strip()[:2000]}")
        return None
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w") as f:
        f.write(r.stdout)
    docs = [d for d in yaml.safe_load_all(r.stdout) if d]
    print(f"  ok    {len(docs):4d} objects -> {os.path.relpath(out_file, ROOT)}")
    return docs


def ours(path: str) -> bool:
    """A file of the repository: not a chart kustomize --enable-helm
    downloaded next to a kustomization (charts/, git-ignored)."""
    return "/charts/" not in path


def check_yaml() -> tuple:
    paths = [p for p in glob.glob(os.path.join(GITOPS, "**", "*.y*ml"), recursive=True) if ours(p)]
    for path in paths:
        try:
            list(yaml.safe_load_all(open(path)))
        except yaml.YAMLError as e:
            error(f"{os.path.relpath(path, ROOT)}: {e}")
    return f"{len(paths)} files under gitops/", []


def render_app(app_file: str) -> None:
    app = yaml.safe_load(open(app_file))
    name = app["metadata"]["name"]
    spec = app["spec"]
    sources = spec.get("sources") or [spec["source"]]
    namespace = spec.get("destination", {}).get("namespace", "default")
    values_root = None
    for s in sources:
        if s.get("ref") == "values":
            values_root = ROOT
    wave = app['metadata'].get('annotations', {}).get('argocd.argoproj.io/sync-wave', '0')
    print(f"{name}  (wave {wave})")
    root = os.path.basename(app_file) == "root-app.yaml"     # the app of apps: no wave, it holds them all
    entry = {"name": name, "wave": None if root else int(wave), "namespace": spec.get("destination", {}).get("namespace", ""),
             "sources": [], "kinds": collections.Counter(), "objects": 0, "files": [], "first_error": len(errors)}
    REPORT["apps"].append(entry)
    REPORT["section"] = name

    def took(docs, out_file=None, source=""):
        entry["sources"].append(source)
        if docs is None:
            return
        entry["objects"] += len(docs)
        entry["kinds"].update(d.get("kind", "?") for d in docs)
        if out_file:
            entry["files"].append(os.path.relpath(out_file, ROOT))

    for i, s in enumerate(sources):
        if s.get("repoURL") == REPO:
            if s.get("targetRevision") != REVISION:
                error(f"{name}: targetRevision {s.get('targetRevision')} != {REVISION} (dataplatform_git_revision)")
            if s.get("path"):
                path = os.path.join(ROOT, s["path"])
                short = os.path.relpath(path, GITOPS)
                if not os.path.isdir(path):
                    error(f"{name}: path {s['path']} does not exist")
                    took(None, source=f"{short} (missing)")
                    continue
                if not any(os.path.isfile(os.path.join(path, k)) for k in ("kustomization.yaml", "kustomization.yml")):
                    docs = [d for f in sorted(glob.glob(os.path.join(path, "*.y*ml"))) for d in yaml.safe_load_all(open(f)) if d]
                    print(f"  ok    {len(docs):4d} objects in {s['path']} (plain directory)")
                    took(docs, source=f"{short} (plain directory)")
                    continue
                out = os.path.join(OUT, name, f"{i}-kustomize.yaml")
                took(run(["kubectl", "kustomize", "--enable-helm", path], out), out, f"{short} (kustomize)")
            continue
        if s.get("repoURL", "").startswith("git@") or s.get("repoURL", "").startswith("https://github.com"):
            if s.get("path") and not s.get("chart"):
                # a directory of another Git repository (e.g. upstream CRDs): not rendered here
                print(f"  skip  {s['repoURL']} {s.get('targetRevision')} {s['path']}")
                took(None, source=f"{s['repoURL'].rsplit('/', 1)[-1]} {s.get('targetRevision')} {s['path']} (not rendered)")
                continue
        if s.get("chart"):
            repo = s["repoURL"]
            chart_ref = repo if repo.startswith("oci://") else s["chart"]
            cmd = ["helm", "template", s.get("helm", {}).get("releaseName", name), chart_ref,
                   "--version", str(s["targetRevision"]), "--namespace", namespace,
                   "--kube-version", KUBE_VERSION, "--include-crds"]
            if not repo.startswith("oci://"):
                cmd += ["--repo", repo]
            for vf in s.get("helm", {}).get("valueFiles", []):
                if vf.startswith("$values/"):
                    if values_root is None:
                        error(f"{name}: $values used without a ref: values source")
                        continue
                    vf = os.path.join(values_root, vf[len("$values/"):])
                if not os.path.isfile(vf):
                    error(f"{name}: values file {vf} missing")
                    continue
                cmd += ["-f", vf]
            if s.get("helm", {}).get("valuesObject"):
                vo = os.path.join(OUT, name, f"{i}-valuesObject.yaml")
                os.makedirs(os.path.dirname(vo), exist_ok=True)
                yaml.safe_dump(s["helm"]["valuesObject"], open(vo, "w"))
                cmd += ["-f", vo]
            out = os.path.join(OUT, name, f"{i}-{s['chart']}.yaml")
            host = re.sub(r"^(oci|https?)://([^/]+).*$", r"\2", repo)
            took(run(cmd, out), out, f"chart {s['chart']} {s['targetRevision']} ({host})")


# the OTel cluster collector is VictoriaMetrics' only writer; VictoriaMetrics
# scrapes nothing itself
VM_APP = "observability-victoriametrics"
VM_WRITER_APP = "observability-otel-cluster"
VM_WRITE = re.compile(r"victoria-metrics(\.[a-z.-]+)?(:8428)?/(api/v1/(write|import)|write|influx|opentelemetry|datadog|newrelic|opentsdb)")


def check_metrics_path(rendered: list) -> None:
    """VictoriaMetrics does not scrape, and nothing but the collector writes to it."""
    writer_seen = False
    for name in rendered:
        for f in sorted(glob.glob(os.path.join(OUT, name, "*.yaml"))):
            text = open(f).read()
            if name == VM_APP and "promscrape" in text.lower():
                error(f"{name}: VictoriaMetrics would scrape (promscrape in {os.path.relpath(f, ROOT)}): "
                      "metrics go through the OTel cluster collector only")
            if re.search(r"^kind: VMAgent$|victoriametrics/vmagent", text, re.M):
                error(f"{name}: a vmagent in {os.path.relpath(f, ROOT)}: metrics go through the OTel cluster collector only")
            if VM_WRITE.search(text):
                if name == VM_WRITER_APP:
                    writer_seen = True
                else:
                    error(f"{name}: writes to VictoriaMetrics ({VM_WRITE.search(text).group(0)} in "
                          f"{os.path.relpath(f, ROOT)}): only the OTel cluster collector does")
    if VM_WRITER_APP in rendered and not writer_seen:
        error(f"{VM_WRITER_APP}: no remote write to VictoriaMetrics found in its render")
    return ("VictoriaMetrics scrapes nothing, no vmagent, the OTel cluster collector is its only writer",
            [f"writer: {VM_WRITER_APP}" + ("" if writer_seen else " (its remote write is missing)")])


def checked(title: str, fn, label: str) -> None:
    """Runs a global check and keeps its result for the report."""
    REPORT["section"] = title
    first = len(errors)
    summary, items = fn()
    REPORT["checks"].append({"title": title, "summary": summary, "items": items, "label": label,
                             "errors": errors[first:]})


# ---------------------------------------------------------------- report ----
REPORT_CSS = """
:root { --bg: #f7f7f5; --fg: #1c1c1a; --muted: #6a6a64; --line: #e3e2dc; --card: #fff; --code: #f0efea;
        --ok: #1d7446; --ok-bg: #e4f3ea; --bad: #b3261e; --bad-bg: #fbe8e6; --accent: #3451c7; }
@media (prefers-color-scheme: dark) {
  :root { --bg: #151514; --fg: #ebeae5; --muted: #9b9a91; --line: #2d2c29; --card: #1d1d1b; --code: #272724;
          --ok: #72d19b; --ok-bg: #15301f; --bad: #f28b82; --bad-bg: #3b1a17; --accent: #93a6ff; } }
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--fg);
       font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
main { max-width: 1120px; margin: 0 auto; padding: 32px 16px 64px; }
h1 { font-size: 24px; margin: 0 8px 4px 0; display: inline-block; }
h2 { font-size: 17px; margin: 36px 0 12px; padding-bottom: 6px; border-bottom: 1px solid var(--line); }
.meta { color: var(--muted); font-size: 13px; }
.badge { display: inline-block; padding: 2px 10px; border-radius: 999px; font-weight: 600; font-size: 13px;
         vertical-align: 4px; }
.ok { color: var(--ok); background: var(--ok-bg); } .bad { color: var(--bad); background: var(--bad-bg); }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-top: 20px; }
.card { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; }
.card .n { font-size: 22px; font-weight: 650; }
.card .l { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
.scroll { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; background: var(--card); border: 1px solid var(--line); }
th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid var(--line); vertical-align: top; font-size: 14px; }
th { font-size: 12px; color: var(--muted); font-weight: 600; text-transform: uppercase; letter-spacing: .04em; }
tr.wave td { background: var(--code); font-weight: 650; font-size: 13px; padding: 5px 12px; }
code { font: 12.5px ui-monospace, SFMono-Regular, Menlo, monospace; background: var(--code); padding: 1px 5px;
       border-radius: 4px; overflow-wrap: anywhere; }
.kinds, .src { color: var(--muted); font-size: 12.5px; }
code.name { white-space: nowrap; overflow-wrap: normal; }
.num { text-align: right; font-variant-numeric: tabular-nums; }
details { margin-top: 4px; } summary { cursor: pointer; color: var(--accent); font-size: 13px; }
ul.list { margin: 6px 0 0; padding-left: 18px; font-size: 13px; }
.err { color: var(--bad); font-size: 13px; white-space: pre-wrap; overflow-wrap: anywhere; }
"""


def write_report(only: list) -> str:
    """.render/report.html: this run's result, one self-contained page."""
    e = html.escape

    def git(*args):
        try:            # the CI image may have no git: CI_COMMIT_SHORT_SHA then
            r = subprocess.run(["git", "-C", ROOT, *args], capture_output=True, text=True)
        except OSError:
            return ""
        return r.stdout.strip() if r.returncode == 0 else ""
    commit = os.environ.get("CI_COMMIT_SHORT_SHA") or git("rev-parse", "--short", "HEAD") or "?"
    dirty = " + local changes" if git("status", "--porcelain", "--", "subsystems/dataplatform", "inventory") else ""
    when = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    apps = sorted(REPORT["apps"], key=lambda a: (a["wave"] is not None, a["wave"] or 0, a["name"]))
    ok = not errors
    by_where = collections.defaultdict(list)
    for where, msg in zip(REPORT["where"], errors):
        by_where[where].append(msg)

    def plural(n, word):
        return f"{n} {word}{'' if n == 1 else 's'}"

    def items(values, label):
        if not values:
            return ""
        lis = "".join(f"<li><code>{e(v)}</code></li>" for v in values)
        return f"<details><summary>{e(plural(len(values), label))}</summary><ul class='list'>{lis}</ul></details>"

    def errs(values):
        return "".join(f"<div class='err'>{e(v)}</div>" for v in values)

    out = [f"<!doctype html><html lang='en'><head><meta charset='utf-8'>"
           f"<meta name='viewport' content='width=device-width, initial-scale=1'>"
           f"<title>Data platform render</title><style>{REPORT_CSS}</style></head><body><main>"]
    out.append(f"<h1>Data platform render</h1><span class='badge {'ok' if ok else 'bad'}'>"
               f"{'passed' if ok else plural(len(errors), 'error')}</span>")
    out.append(f"<div class='meta'>{e(REVISION)} @ <code>{e(commit)}</code>{e(dirty)} · {e(when)}"
               f"{' · only ' + e(', '.join(only)) if only else ''}</div>")
    checks_ok = sum(1 for c in REPORT["checks"] if not c["errors"])
    cards = [("Applications", len(apps)), ("Objects", sum(a["objects"] for a in apps)),
             ("Sync waves", len({a["wave"] for a in apps if a["wave"] is not None})),
             ("Checks passed", f"{checks_ok}/{len(REPORT['checks'])}"), ("Errors", len(errors))]
    out.append("<div class='cards'>" + "".join(
        f"<div class='card'><div class='n'>{e(str(n))}</div><div class='l'>{e(l)}</div></div>" for l, n in cards) + "</div>")

    out.append("<h2>Platform values</h2><div class='scroll'><table><tr><th>Setting</th><th>Value</th></tr>")
    for k, v in (("Repository", REPO), ("Revision", REVISION), ("Domain", DOMAIN), ("S3 endpoint", S3_ENDPOINT),
                 ("S3 region", S3_REGION), ("RGW account", ACCOUNT),
                 ("Kubernetes (render)", KUBE_VERSION)):
        out.append(f"<tr><td>{e(k)}</td><td><code>{e(v)}</code></td></tr>")
    out.append("</table></div>")

    out.append("<h2>Checks</h2><div class='scroll'><table><tr><th>Check</th><th>Result</th><th>Detail</th></tr>")
    for c in REPORT["checks"]:
        badge = "<span class='badge ok'>ok</span>" if not c["errors"] else f"<span class='badge bad'>{plural(len(c['errors']), 'error')}</span>"
        out.append(f"<tr><td>{e(c['title'])}</td><td>{badge}</td><td>{e(c['summary'])}"
                   f"{items(c['items'], c['label'])}{errs(c['errors'])}</td></tr>")
    out.append("</table></div>")

    out.append("<h2>Applications, in sync order</h2><div class='scroll'><table>"
               "<tr><th>Application</th><th>Namespace</th><th>Sources</th><th class='num'>Objects</th><th>Kinds</th><th>Result</th></tr>")
    wave = "start"
    for a in apps:
        if a["wave"] != wave:
            wave = a["wave"]
            names = [x["name"] for x in apps if x["wave"] == wave]
            head = ("The root application: it creates every Application below, wave by wave" if wave is None
                    else f"Wave {wave} · {plural(len(names), 'application')}")
            out.append(f"<tr class='wave'><td colspan='6'>{e(head)}</td></tr>")
        mine = by_where.get(a["name"], [])
        kinds = ", ".join(f"{k} {n}" for k, n in sorted(a["kinds"].items(), key=lambda kv: (-kv[1], kv[0])))
        srcs = "<br>".join(f"<span class='src'>{e(s)}</span>" for s in a["sources"] if s)
        badge = "<span class='badge ok'>ok</span>" if not mine else f"<span class='badge bad'>{plural(len(mine), 'error')}</span>"
        # nothing rendered (a directory of another repository): no count
        objects = a["objects"] if a["files"] or a["objects"] else "–"
        out.append(f"<tr><td><code class='name'>{e(a['name'])}</code>{items(a['files'], 'rendered file')}</td>"
                   f"<td>{e(a['namespace'] or '–')}</td><td>{srcs}</td><td class='num'>{objects}</td>"
                   f"<td class='kinds'>{e(kinds)}</td><td>{badge}{errs(mine)}</td></tr>")
    out.append("</table></div>")

    if errors:
        out.append("<h2>Errors</h2><div class='scroll'><table><tr><th>Where</th><th>Error</th></tr>")
        for where, msg in zip(REPORT["where"], errors):
            out.append(f"<tr><td>{e(where or '-')}</td><td class='err'>{e(msg)}</td></tr>")
        out.append("</table></div>")
    out.append("<p class='meta'>make dataplatform-render (subsystems/dataplatform/files/render.py); "
               "the rendered objects are in .render/&lt;application&gt;/.</p></main></body></html>")
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, "report.html")
    with open(path, "w") as f:
        f.write("\n".join(out))
    return path


def otel_config(out: str) -> None:
    """--otel-config <file>: the cluster collector's configuration as rendered
    (the ConfigMap's `relay`), for `otelcol-contrib validate` (CI)."""
    rendered = os.path.join(OUT, "observability-otel-cluster", "0-opentelemetry-collector.yaml")
    with open(rendered) as f:
        docs = [d for d in yaml.safe_load_all(f) if d and d.get("kind") == "ConfigMap" and "relay" in d.get("data", {})]
    if not docs:
        sys.exit(f"no collector ConfigMap in {rendered}: run the render first")
    with open(out, "w") as f:
        f.write(docs[0]["data"]["relay"])
    print(f"collector configuration -> {out}")


def main() -> None:
    if sys.argv[1:2] == ["--otel-config"]:
        otel_config(sys.argv[2])
        return
    print(f"repository {REPO} @ {REVISION}, domain {DOMAIN}")
    checked("YAML parses", check_yaml, "file")
    apps = [os.path.join(GITOPS, "app-of-apps", "root-app.yaml")] + sorted(glob.glob(os.path.join(GITOPS, "apps", "*.yaml")))
    only = sys.argv[1:]
    rendered = []
    for app_file in apps:
        if only and os.path.splitext(os.path.basename(app_file))[0] not in only:
            continue
        render_app(app_file)
        rendered.append(yaml.safe_load(open(app_file))["metadata"]["name"])
    checked("Metrics path", lambda: check_metrics_path(rendered), "detail")
    print()
    report = write_report(only)
    print(f"report: {os.path.relpath(report, ROOT)}")
    if errors:
        sys.exit(f"{len(errors)} error(s)")
    print("all applications render")


if __name__ == "__main__":
    main()
