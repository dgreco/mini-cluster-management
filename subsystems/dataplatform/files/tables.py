#!/usr/bin/env python3
"""The platform's domains and tables in Meta Catalog: what dp-tables.sh runs
(make dataplatform-tables ARGS="..."), after it logged in.

  tables.py check [<file>]               the model and a file of tables, offline
  tables.py [-y] model                   the model file's changes -> Meta Catalog, as versions
  tables.py [-n] [-y] sync [<file>]      Meta Catalog's domains and tables -> the file's
  tables.py load <file> [<file> ...]     the model when Meta Catalog lacks it, then what a file adds
  tables.py list [<ref>]                 the tables, provisioned and authorized?
  tables.py status <ref>                 each table's folder and metastore table, for whom
  tables.py provision|authorize|reject <ref>
  tables.py [-y] unprovision|delete <ref>
  tables.py [-n] [-y] remove [<file>]    every domain and table of the file out of Meta Catalog
  tables.py [-y] unload-model <model.yaml>

<ref> is a domain (`sales`), a layer of it (`sales/silver`) or a table
(`sales/silver/orders_v1`: its name with its version). The file of tables is
metacatalog/tables/tables.yaml unless another is named; the model
metacatalog/table-model.yaml (TABLES_MODEL names another).

sync    brings the domains and tables of its scope to the file's:
        - makes what is new in the file (one upload: a new table's lineage
          and a new domain's tables come with it, existing ones by id);
        - gives a table whose description, columns, lineage or policy differ
          the file's (one new version of it: Meta Catalog's whole-aggregate
          update, which keeps what runs wrote; a column is only ever added),
          and a domain its description and exactly the tables that name it;
        - provisions what is new, changed or not provisioned everywhere, then
          authorizes it (rejects it when the file gives it no grant);
        - removes what is in its scope and not in the file: a table is
          rejected, unprovisioned (DROPPED, its files DELETED), taken out of
          its domain and deleted, dependants first; then a domain with no
          table left. Shown first and asked (-y does not ask; -n only shows).
        Its scope is every domain but the smoke test's (smoke, smoke*);
        TABLES_SYNC_DOMAINS="d1 d2" is those domains only. Refused before any
        step: a file with no table (it would remove all), a domain of the
        file outside the scope, a file that is not right (check), a table
        whose format changed or whose column went or changed its type (a
        new version is another table), a table outside the scope that
        depends on one that would go. Authorizing needs the admin group.
        A step that fails stops the run; run it again, it does what is left.
load    sends the model when Meta Catalog lacks it, then makes the domains
        and tables of the files that Meta Catalog lacks; nothing else.
delete  takes unprovisioned, rejected tables out of Meta Catalog, with their
        schema, their resources and the links to them; a domain ref also
        takes the domain itself once no table names it.
remove  takes every table and domain of the file that Meta Catalog has out
        of it, as sync takes out what a file no longer has: rejected,
        unprovisioned (DROPPED, its files DELETED), deleted, dependants
        first, then the domains. The model stays; sync makes them all again
        (the lakehouse example's reset). Refused when a table the file does
        not define depends on one of them. Shown first and asked.

Environment: METACATALOG_URL, TABLES_TOKEN (a token of the realm; the wrapper
logs in), TABLES_MODEL, TABLES_SYNC_DOMAINS. Standard library plus PyYAML.
"""
import importlib.util
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
METACATALOG_DIR = os.path.normpath(os.path.join(HERE, "..", "metacatalog"))
DEFAULT_FILE = os.path.join(METACATALOG_DIR, "tables", "tables.yaml")
DEFAULT_MODEL = os.path.join(METACATALOG_DIR, "table-model.yaml")

DOMAIN_TYPE = "DomainType"
TABLE_TYPE = "TableType"
SCHEMA_TYPE = "TableSchemaType"
RESOURCE_TYPES = ("S3FolderType", "HiveTableType")
LAYERS = ("BRONZE", "SILVER", "GOLD")
FORMATS = ("parquet", "iceberg")
SMOKE = "smoke"                       # the smoke test's domains: smoke, smoke*
DOMAIN_NAME = re.compile(r"^[a-z][a-z0-9]{0,23}$")
TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]{0,50}$")
COLUMN_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
REF = re.compile(r"^([a-z][a-z0-9]{0,23})(?:/(bronze|silver|gold)(?:/([a-z][a-z0-9_]{0,50}_v[1-9][0-9]{0,2}))?)?$")
PERMISSIONS = ("READ", "WRITE")
PRINCIPAL_TYPES = ("GROUP", "USER", "SERVICE")
# what a run writes: never sent, never compared
RUN_FIELDS = ("provisioningStatus", "provisioningResult", "authorizationStatus", "authorizationResult")
TABLE_FIELDS = ("description", "format", "principals", "permissions", "grants")


class Refused(Exception):
    pass


# ---------------------------------------------------------------- names ----
def table_ref(values: dict) -> str:
    """<domain>/<layer>/<name>_v<version>: a table's identity."""
    return f"{values['domain']}/{str(values['layer']).lower()}/{values['name']}_v{values['version']}"


def trino_name(values: dict) -> str:
    return f"lakehouse.{values['domain']}_{str(values['layer']).lower()}.{values['name']}_v{values['version']}"


def in_scope(domain: str, scope=None) -> bool:
    """sync's scope: the domains of TABLES_SYNC_DOMAINS, or all but the smoke test's."""
    if scope is None:
        scope = os.environ.get("TABLES_SYNC_DOMAINS", "").split()
    return domain in scope if scope else not domain.startswith(SMOKE)


def selects(ref: str, tref: str) -> bool:
    """Whether a ref (domain, domain/layer, table) names a table."""
    return tref == ref or tref.startswith(ref + "/")


# ---------------------------------------------------------------- the file ----
def read_docs(path: str) -> list:
    with open(path) as f:
        return [d for d in yaml.safe_load_all(f) if d is not None]


def columns_of(document: dict) -> list:
    parts = document.get("parts") or []
    if len(parts) != 1 or not isinstance(parts[0], dict):
        return []
    return ((parts[0].get("values") or {}).get("columns")) or []


def resources_expected(doc_or_values) -> int:
    """A table with a schema has a folder and a metastore table; one without
    (a folder of files only) its folder alone."""
    return 2 if doc_or_values else 1


def check(docs: list) -> dict:
    """{"domains": {name: document}, "tables": {ref: document}, "by_ref": {document ref: table ref or domain name},
    "lineage": {table ref: [table refs]}} of a right file; Refused naming every fault."""
    faults, domains, tables, by_ref, kinds = [], {}, {}, {}, {}
    for i, d in enumerate(docs, 1):
        at = f"document {i}"
        if not isinstance(d, dict):
            faults.append(f"{at}: not a mapping")
            continue
        ref, kind, v = d.get("ref"), d.get("entityType"), d.get("values")
        at = f"document {i} ({ref})"
        if not isinstance(ref, str) or not ref:
            faults.append(f"{at}: no ref")
            continue
        if ref in kinds:
            faults.append(f"{at}: ref {ref!r} twice")
            continue
        if not isinstance(v, dict):
            faults.append(f"{at}: no values")
            continue
        unknown = set(d) - {"ref", "entityType", "values", "parts", "dependsOn"}
        if unknown:
            faults.append(f"{at}: unknown keys {sorted(unknown)}")
        kinds[ref] = kind
        if kind == DOMAIN_TYPE:
            name = v.get("name")
            if not isinstance(name, str) or not DOMAIN_NAME.match(name):
                faults.append(f"{at}: domain name {name!r}: lower-case letters and digits, a letter first, at most 24")
                continue
            if not isinstance(v.get("description"), str) or not v["description"].strip():
                faults.append(f"{at}: a domain has a description")
            if set(v) - {"name", "description"}:
                faults.append(f"{at}: a domain has a name and a description only, not {sorted(set(v) - {'name', 'description'})}")
            if d.get("parts") or d.get("dependsOn"):
                faults.append(f"{at}: a domain has no parts and no dependsOn (its tables name it)")
            if name in domains:
                faults.append(f"{at}: domain {name} twice")
            domains[name] = d
            by_ref[ref] = name
        elif kind == TABLE_TYPE:
            bad = []
            if not isinstance(v.get("domain"), str) or not DOMAIN_NAME.match(v["domain"]):
                bad.append(f"domain {v.get('domain')!r}")
            if v.get("layer") not in LAYERS:
                bad.append(f"layer {v.get('layer')!r} (one of {', '.join(LAYERS)})")
            if not isinstance(v.get("name"), str) or not TABLE_NAME.match(v["name"]):
                bad.append(f"name {v.get('name')!r} (lower-case letters, digits and _, a letter first)")
            ver = v.get("version")
            if not isinstance(ver, int) or isinstance(ver, bool) or not 1 <= ver <= 999:
                bad.append(f"version {ver!r} (1 to 999)")
            if v.get("format") not in FORMATS:
                bad.append(f"format {v.get('format')!r} (one of {', '.join(FORMATS)})")
            if not isinstance(v.get("description"), str) or not v["description"].strip():
                bad.append("no description")
            if set(v) - {"name", "domain", "layer", "version", *TABLE_FIELDS}:
                bad.append(f"unknown values {sorted(set(v) - {'name', 'domain', 'layer', 'version', *TABLE_FIELDS})}")
            if bad:
                faults.append(f"{at}: " + "; ".join(bad))
                continue
            faults += [f"{at}: {p}" for p in policy_faults(v)]
            parts = d.get("parts") or []
            if parts:
                cols = columns_of(d)
                if len(parts) != 1 or parts[0].get("entityType") != SCHEMA_TYPE or not cols:
                    faults.append(f"{at}: at most one part, a {SCHEMA_TYPE} with its columns (none: a folder only)")
                names = []
                for c in cols:
                    if not isinstance(c, dict) or not COLUMN_NAME.match(str(c.get("name", ""))) \
                            or not isinstance(c.get("type"), str) or not isinstance(c.get("description"), str):
                        faults.append(f"{at}: column {c!r}: name (lower-case letters, digits, _), type and description")
                        continue
                    names.append(c["name"])
                if len(names) != len(set(names)):
                    faults.append(f"{at}: a column named twice")
            tref = table_ref(v)
            if tref in tables:
                faults.append(f"{at}: table {tref} twice")
            tables[tref] = d
            by_ref[ref] = tref
        else:
            faults.append(f"{at}: entityType {kind!r}, not {DOMAIN_TYPE} or {TABLE_TYPE}")
    lineage = {}
    for tref, d in tables.items():
        deps = d.get("dependsOn") or []
        lineage[tref] = []
        if not isinstance(deps, list):
            faults.append(f"{d['ref']}: dependsOn is a list of refs")
            continue
        for r in deps:
            if kinds.get(r) != TABLE_TYPE:
                faults.append(f"{d['ref']}: dependsOn {r!r} is no table of this file")
            elif r not in by_ref:
                continue                    # a table with faults of its own, named above
            elif by_ref[r] == tref:
                faults.append(f"{d['ref']}: depends on itself")
            else:
                lineage[tref].append(by_ref[r])
        if d["values"]["domain"] not in domains:
            faults.append(f"{d['ref']}: domain {d['values']['domain']!r} is not a domain of this file")
    faults += cycle_faults(lineage)
    if faults:
        raise Refused("\n".join(faults))
    return {"domains": domains, "tables": tables, "by_ref": by_ref, "lineage": lineage}


def policy_faults(v: dict) -> list:
    """What is wrong with a table's policy (principals, permissions, grants)."""
    out, ids = [], set()
    for p in v.get("principals") or []:
        if not isinstance(p, dict) or not isinstance(p.get("id"), str) or p.get("type") not in PRINCIPAL_TYPES:
            out.append(f"principal {p!r}: an id and a type ({', '.join(PRINCIPAL_TYPES)})")
        else:
            ids.add(p["id"])
    perms = {p.get("name") for p in (v.get("permissions") or []) if isinstance(p, dict)}
    if perms - set(PERMISSIONS):
        out.append(f"permissions {sorted(perms - set(PERMISSIONS))}: only {', '.join(PERMISSIONS)} mean something here")
    for g in v.get("grants") or []:
        if not isinstance(g, dict):
            out.append(f"grant {g!r}: principals and permissions")
            continue
        for p in g.get("principals") or []:
            if p not in ids:
                out.append(f"grant names {p!r}, which is not among the principals")
        for p in g.get("permissions") or []:
            if p not in perms:
                out.append(f"grant gives {p!r}, which is not among the permissions")
    return out


def cycle_faults(lineage: dict) -> list:
    seen, active, out = set(), set(), []

    def visit(n, path):
        if n in active:
            out.append("lineage goes round: " + " -> ".join(path + [n]))
            return
        if n in seen:
            return
        active.add(n)
        for m in lineage.get(n, []):
            visit(m, path + [n])
        active.discard(n)
        seen.add(n)

    for n in sorted(lineage):
        visit(n, [])
    return out


def domain_tables(f: dict) -> dict:
    """{domain: [table refs]} of the file."""
    out = {name: [] for name in f["domains"]}
    for tref, d in sorted(f["tables"].items()):
        out[d["values"]["domain"]].append(tref)
    return out


# ---------------------------------------------------------------- the plan ----
def authored(values: dict) -> dict:
    return {k: v for k, v in (values or {}).items() if k not in RUN_FIELDS and v is not None}


def table_differences(want: dict, have: dict) -> list:
    """What differs between a file's table and Meta Catalog's ({values,
    columns, part}); Refused for what cannot change."""
    wv, hv = authored(want["values"]), authored(have["values"])
    ref = table_ref(wv)
    if wv.get("format") != hv.get("format"):
        raise Refused(f"{ref}: format {hv.get('format')} in Meta Catalog, {wv.get('format')} in the file: "
                      "another format is another table (give it a new version)")
    if bool(want.get("parts")) != bool(have.get("part")):
        raise Refused(f"{ref}: a schema added or taken away: a folder of files and a table are two tables "
                      "(give it a new version)")
    diff = [k for k in ("description", "principals", "permissions", "grants") if wv.get(k) != hv.get(k)]
    wcols = {c["name"]: c for c in columns_of(want)}
    hcols = {c["name"]: c for c in have.get("columns") or []}
    gone = sorted(set(hcols) - set(wcols))
    if gone:
        raise Refused(f"{ref}: column(s) {', '.join(gone)} taken out: a column is never dropped (give the table a new version)")
    retyped = sorted(n for n in wcols if n in hcols and str(wcols[n]["type"]).replace(" ", "").lower()
                     != str(hcols[n]["type"]).replace(" ", "").lower())
    if retyped:
        raise Refused(f"{ref}: column(s) {', '.join(retyped)} retyped: a column keeps its type (give the table a new version)")
    if columns_of(want) != (have.get("columns") or []):
        diff.append("columns")
    return diff


def has_grants(values: dict) -> bool:
    return any(g.get("principals") and g.get("permissions") for g in values.get("grants") or [])


def removal_order(tables: dict, targets: list) -> list:
    """Table refs of targets, each after every target that depends on it
    (Meta Catalog refuses to delete a table something depends on); Refused
    when a table that stays depends on one that goes."""
    lineage = {t: set(v.get("lineage") or []) for t, v in tables.items()}
    for t in sorted(set(lineage) - set(targets)):
        hit = sorted(lineage[t] & set(targets))
        if hit:
            raise Refused(f"{t} depends on {', '.join(hit)}: remove it first, or with them")
    left, out = set(targets), []
    while left:
        free = sorted(t for t in left if not any(t in lineage.get(u, ()) for u in left if u != t))
        if not free:
            raise Refused("the lineage of " + ", ".join(sorted(left)) + " goes round")
        out += free
        left -= set(free)
    return out


def dependants_first(tables: dict, targets: list) -> list:
    """targets, each after the targets that depend on it (reject and
    unprovision go that way; a table outside may still depend on one)."""
    lineage = {t: set(v.get("lineage") or []) for t, v in tables.items()}
    left, out = set(targets), []
    while left:
        free = sorted(t for t in left if not any(t in lineage.get(u, ()) for u in left if u != t))
        out += free or sorted(left)
        left -= set(free or left)
    return out


def plan(f: dict, live: dict, scope=None) -> dict:
    """What sync does: {"create": [document refs], "update": [(table ref, [what])],
    "domains": [domain names], "provision": [...], "authorize": [...],
    "reject": [...], "remove": [table refs, in order], "remove_domains": [...]}.
    live: {"domains": {name: {id, values, dependencies: [table refs]}},
    "tables": {table ref: {id, values, part, columns, lineage: [table refs],
    resources: [{type, provisioningStatus, authorizationStatus}]}}}."""
    if not f["tables"]:
        raise Refused("the file holds no table: sync would remove every table of its scope")
    outside = sorted(n for n in f["domains"] if not in_scope(n, scope))
    if outside:
        raise Refused(f"domain(s) {', '.join(outside)} of the file are outside sync's scope "
                      "(the smoke test's are its own: TABLES_SYNC_DOMAINS names a scope)")
    out = {"create": [], "update": [], "domains": [], "provision": [], "authorize": [], "reject": [],
           "remove": [], "remove_domains": []}
    for name, d in f["domains"].items():
        if name not in live["domains"]:
            out["create"].append(d["ref"])
    for tref, d in sorted(f["tables"].items()):
        have = live["tables"].get(tref)
        granted = has_grants(d["values"])
        if have is None:
            out["create"].append(d["ref"])
            out["provision"].append(tref)
            (out["authorize"] if granted else out["reject"]).append(tref)
            continue
        diff = table_differences(d, have)
        if sorted(have.get("lineage") or []) != sorted(f["lineage"][tref]):
            diff.append("lineage")
        resources = have.get("resources") or []
        want = resources_expected(d.get("parts"))
        if diff:
            out["update"].append((tref, diff))
        if diff or len(resources) != want or any(r.get("provisioningStatus") != "PROVISIONED" for r in resources):
            out["provision"].append(tref)
        if granted:
            if diff or len(resources) != want or any(r.get("authorizationStatus") != "AUTHORIZED" for r in resources):
                out["authorize"].append(tref)
        elif any(r.get("authorizationStatus") == "AUTHORIZED" for r in resources):
            out["reject"].append(tref)
    wanted = domain_tables(f)
    for name, d in f["domains"].items():
        have = live["domains"].get(name)
        if have is not None and (authored(have["values"]).get("description") != d["values"]["description"]
                                 or sorted(have.get("dependencies") or []) != sorted(wanted[name])):
            out["domains"].append(name)
    gone = [t for t, v in live["tables"].items() if t not in f["tables"] and in_scope(v["values"]["domain"], scope)]
    out["remove"] = removal_order(live["tables"], gone) if gone else []
    out["remove_domains"] = sorted(n for n in live["domains"] if n not in f["domains"] and in_scope(n, scope))
    return out


def upload_document(f: dict, refs: list, ids: dict) -> str:
    """The documents of refs for POST /aggregate/yaml: dependsOn within the
    upload by ref, on what Meta Catalog has by id (ids: table ref -> id); a
    new domain depends on its tables."""
    wanted = domain_tables(f)
    in_upload = set(refs)
    ref_of = {tref: d["ref"] for tref, d in f["tables"].items()}
    docs = []
    for r in refs:
        key = f["by_ref"][r]
        d = f["domains"].get(key) or f["tables"][key]
        deps = wanted[key] if d["entityType"] == DOMAIN_TYPE else f["lineage"][key]
        out = {"ref": r, "entityType": d["entityType"], "values": d["values"]}
        named = [ref_of[t] if ref_of[t] in in_upload else ids[t] for t in deps]
        if named:
            out["dependsOn"] = named
        if d.get("parts"):
            out["parts"] = d["parts"]
        docs.append(out)
    return yaml.safe_dump_all(docs, sort_keys=False, allow_unicode=True)


def update_document(d: dict, entity_id: str, part_id, dependencies: list) -> str:
    """The document of PUT /aggregate/{id}/yaml: the file's values, the
    existing part by its id, the dependencies by id (exactly these)."""
    out = {"id": entity_id, "entityType": d["entityType"], "values": d["values"], "dependencies": dependencies}
    if d.get("parts"):
        out["parts"] = [{"id": part_id, "entityType": SCHEMA_TYPE, "values": d["parts"][0]["values"]}]
    return yaml.safe_dump(out, sort_keys=False, allow_unicode=True)


def describe(p: dict) -> list:
    lines = []
    if p["create"]:
        lines.append(f"make {len(p['create'])}: {', '.join(p['create'])}")
    for tref, diff in p["update"]:
        lines.append(f"update {tref}: {', '.join(diff)} as the file says (a new version)")
    for name in p["domains"]:
        lines.append(f"domain {name}: description and tables as the file says")
    for op in ("provision", "authorize", "reject"):
        if p[op]:
            lines.append(f"{op} {len(p[op])}: {', '.join(p[op])}")
    for t in p["remove"]:
        lines.append(f"remove {t}: reject, unprovision (DROP the table, DELETE its files), delete")
    for n in p["remove_domains"]:
        lines.append(f"remove the domain {n}")
    return lines


# ---------------------------------------------------------------- Meta Catalog ----
class Api:
    def __init__(self, url: str, token: str):
        self.base = url.rstrip("/") + "/metacatalog/v1"
        self.token = token

    def call(self, method: str, path: str, body=None, content_type="application/json", ok=(200, 201, 202, 204)):
        data = None
        if body is not None:
            data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"Authorization": f"Bearer {self.token}", "Content-Type": content_type,
                                              "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                text, code = r.read().decode(), r.status
        except urllib.error.HTTPError as e:
            text, code = e.read().decode(errors="replace"), e.code
        if code not in ok:
            raise Refused(f"Meta Catalog answered {method} {path} with HTTP {code}: {text[:600]}")
        if not text.strip():
            return None
        try:
            return json.loads(text)
        except ValueError:
            return text

    def entities(self, type_name: str, query=None) -> list:
        q = {"entityTypeName": type_name}
        if query:
            q["queryPath"] = query
        out = []
        for e in self.call("GET", "/entity?" + urllib.parse.urlencode(q)) or []:
            v = e.get("values")
            out.append({"id": e["id"], "values": json.loads(v) if isinstance(v, str) else (v or {})})
        return out

    def linked(self, entity_id: str, relation: str) -> list:
        return [e["id"] for e in self.call("GET", f"/entity/link/{entity_id}/{relation}") or []]

    def aggregate(self, entity_id: str) -> dict:
        return yaml.safe_load(self.call("GET", f"/aggregate/{entity_id}/yaml?retrieveMappedEntities=false"))

    def run(self, entity_id: str, operation: str, say: str) -> str:
        sid = self.call("POST", f"/aggregate/{entity_id}/{operation}?async=true")["scheduleId"]
        print(f"{operation} {say} ", end="", flush=True)
        status = "?"
        for _ in range(600):
            status = (self.call("GET", f"/procedure/{sid}") or {}).get("status", "?")
            if status != "RUNNING":
                break
            print(".", end="", flush=True)
            time.sleep(3)
        print(f" {status}")
        return status


def resources_of(api: Api, values: dict) -> list:
    layer = str(values["layer"]).lower()
    table = f"{values['name']}_v{values['version']}"
    q = f'$ ? (@.domain == "{values["domain"]}" && @.layer == "{layer}" && @.table == "{table}")'
    return [{"type": t, **e["values"]} for t in RESOURCE_TYPES for e in api.entities(t, q)]


def model_missing(api: Api) -> bool:
    try:
        api.call("GET", f"/entity-type/{TABLE_TYPE}")
        return False
    except Refused as e:
        if "HTTP 404" in str(e) or "HTTP 400" in str(e):
            return True
        raise


def read_live(api: Api, with_resources: bool = True) -> dict:
    if model_missing(api):
        raise Refused(f"Meta Catalog does not have the model ({TABLE_TYPE}): make dataplatform-tables ARGS=model")
    tables, ids = {}, {}
    rows = api.entities(TABLE_TYPE)
    for e in rows:
        ids[e["id"]] = table_ref(e["values"])
    for e in rows:
        tref = ids[e["id"]]
        agg = api.aggregate(e["id"])
        parts = agg.get("parts") or []
        tables[tref] = {"id": e["id"], "values": e["values"],
                        "part": parts[0]["id"] if parts else None,
                        "columns": ((parts[0].get("values") or {}).get("columns") or []) if parts else [],
                        "lineage": [ids[i] for i in agg.get("dependencies") or [] if i in ids],
                        "resources": resources_of(api, e["values"]) if with_resources else []}
    domains = {}
    for e in api.entities(DOMAIN_TYPE):
        agg = api.aggregate(e["id"])
        domains[e["values"]["name"]] = {"id": e["id"], "values": e["values"],
                                        "dependencies": [ids[i] for i in agg.get("dependencies") or [] if i in ids]}
    return {"domains": domains, "tables": tables, "ids": ids}


def confirm(question: str, word: str, yes: bool) -> None:
    if yes:
        return
    if not sys.stdin.isatty():
        raise Refused(f"{question} needs an answer: run it in a terminal, or with -y")
    if input(f"{question} Type {word} to go on: ").strip() != word:
        raise Refused("not confirmed: nothing more done")


def wait_rederived(api: Api, values: dict, seconds: int = 90) -> None:
    """After an update Meta Catalog derives the table's resources again, in
    events of its own, with their mapped values only (the statuses go): a
    run before that would be wiped by it."""
    for _ in range(seconds // 3):
        rs = resources_of(api, values)
        if rs and all(not r.get("provisioningStatus") and not r.get("authorizationStatus") for r in rs):
            return
        time.sleep(3)


def result_line(r: dict, field: str) -> str:
    text = str(r.get(field) or "")
    text = re.sub(r"^[A-Za-z]+ [A-Za-z0-9]+ id=[0-9a-f-]+ \(script task\): ", "", text)
    text = re.sub(r"^Script \S+ exited with status [0-9]+: ", "", text)
    lines = [x for x in text.split("\n") if x.strip()]
    return lines[-1] if lines else "-"


def run_all(api: Api, op: str, refs: list, live: dict) -> None:
    """Runs op on each table; stops at the first that does not succeed."""
    field = "provisioningResult" if op in ("provision", "unprovision") else "authorizationResult"
    for tref in refs:
        t = live["tables"][tref]
        if api.run(t["id"], op, tref) != "SUCCEEDED":
            for r in resources_of(api, t["values"]):
                print(f"  {r['type']}: {result_line(r, field)}")
            raise Refused(f"{op} {tref} failed: nothing more done (status shows why; run it again once fixed)")


def delete_tables(api: Api, refs: list, live: dict) -> None:
    """Unprovisioned, rejected tables out of Meta Catalog, dependants first,
    each after the links to it (a domain's, another table's) are taken out."""
    for tref in removal_order(live["tables"], refs):
        t = live["tables"][tref]
        rs = resources_of(api, t["values"])
        if any(r.get("provisioningStatus") == "PROVISIONED" for r in rs):
            raise Refused(f"{tref} is provisioned: unprovision it first (its files and its entry in the metastore would stay)")
        if any(r.get("authorizationStatus") == "AUTHORIZED" for r in rs):
            raise Refused(f"{tref} is authorized: reject it first (its grants would stay in force)")
        for other in api.linked(t["id"], "IS_REQUIRED_BY"):
            api.call("DELETE", f"/entity/link/{other}/DEPENDS_ON/{t['id']}")
        api.call("DELETE", f"/aggregate/{t['id']}")
        print(f"table {tref}: deleted from Meta Catalog")
        for d in live["domains"].values():
            if tref in d["dependencies"]:
                d["dependencies"].remove(tref)


# ---------------------------------------------------------------- commands ----
def model_plan_module():
    spec = importlib.util.spec_from_file_location("model_plan", os.path.join(HERE, "model_plan.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def model_calls(api: Api, model: str) -> tuple:
    """(calls, notes) that bring Meta Catalog's model to the file's."""
    mp = model_plan_module()
    live = {"traits": api.call("GET", "/trait"), "types": api.call("GET", "/entity-type"),
            "mappings": api.call("GET", "/mapping"), "links": api.call("GET", "/trait/relationships")}
    with open(model) as fh:
        sections = list(yaml.safe_load_all(fh))
    try:
        steps = mp.plan(mp.read_model(sections), mp.read_live(live))
    except mp.PlanError as e:
        raise Refused(f"{model}: {e}")
    return [s for s in steps if s.get("method")], [s["note"] for s in steps if s.get("note")]


def cmd_model(api: Api, model: str, yes: bool) -> int:
    calls, notes = model_calls(api, model)
    for n in notes:
        print("model: " + n)
    if not calls:
        print(f"model: Meta Catalog has the model of {model} already")
        return 0
    print(f"model: {len(calls)} change(s) of {model} for Meta Catalog:")
    for s in calls:
        print("  " + s["say"])
    confirm("Send them to Meta Catalog?", "model", yes)
    send_model(api, calls)
    return 0


def send_model(api: Api, calls: list) -> None:
    for s in calls:
        api.call(s["method"], s["path"], s["body"])
        print(f"{s['say']}: done")


def ensure_model(api: Api, model: str) -> None:
    """The model, when Meta Catalog has none of it (a platform built anew)."""
    if model_missing(api):
        calls, _ = model_calls(api, model)
        print(f"model: Meta Catalog has no {TABLE_TYPE}: sending {model} ({len(calls)} definitions)")
        send_model(api, calls)


def cmd_check(path: str, model: str) -> int:
    f = check(read_docs(path))
    read_docs(model)
    print(f"{path}: {len(f['domains'])} domain(s), {len(f['tables'])} table(s)")
    for name, trefs in domain_tables(f).items():
        print(f"  {name}: {', '.join(trefs) or 'no table'}")
    return 0


def create(api: Api, f: dict, refs: list, live: dict) -> dict:
    if refs:
        api.call("POST", "/aggregate/yaml", upload_document(f, refs, {t: v["id"] for t, v in live["tables"].items()}),
                 content_type="application/octet-stream")
        print(f"made {', '.join(refs)}")
        time.sleep(3)                                  # the resources are derived a moment later
        live = read_live(api)
    return live


def cmd_sync(api: Api, path: str, model: str, dry: bool, yes: bool) -> int:
    f = check(read_docs(path))
    if not dry:
        ensure_model(api, model)
    elif model_missing(api):
        print(f"sync: Meta Catalog has no model yet: sync would send {model} first, then make everything of {path}")
        return 0
    live = read_live(api)
    p = plan(f, live)
    steps = describe(p)
    if not steps:
        print(f"sync: Meta Catalog's tables are those of {path} already, provisioned and authorized")
        return 0
    print(f"sync: {len(steps)} step(s) to make Meta Catalog's tables those of {path}:")
    for s in steps:
        print("  " + s)
    if dry:
        return 0
    if p["remove"] or p["remove_domains"]:
        confirm(f"{len(p['remove'])} table(s) are DROPPED, their files DELETED, and taken out of Meta Catalog.", "sync", yes)
    else:
        confirm("Do it?", "sync", yes)
    live = create(api, f, p["create"], live)
    ids = {t: v["id"] for t, v in live["tables"].items()}
    for tref, diff in p["update"]:
        have = live["tables"][tref]
        document = update_document(f["tables"][tref], have["id"], have["part"], [ids[t] for t in f["lineage"][tref]])
        api.call("PUT", f"/aggregate/{have['id']}/yaml", document, content_type="application/octet-stream")
        print(f"updated {tref} ({', '.join(diff)})")
    wanted = domain_tables(f)
    for name in p["domains"]:
        d = live["domains"][name]
        api.call("PUT", f"/aggregate/{d['id']}/yaml",
                 update_document(f["domains"][name], d["id"], None, [ids[t] for t in wanted[name]]),
                 content_type="application/octet-stream")
        print(f"domain {name}: depends on {', '.join(wanted[name]) or 'nothing'}")
    for tref, _ in p["update"]:
        wait_rederived(api, f["tables"][tref]["values"])
    for op in ("provision", "authorize", "reject"):
        run_all(api, op, p[op], live)
    remove_all(api, p["remove"], p["remove_domains"], live)
    return 0


def remove_all(api: Api, trefs: list, domains: list, live: dict) -> None:
    """Tables out of Meta Catalog: rejected where authorized, unprovisioned
    where provisioned (dependants first), deleted; then the domains no table
    names any more."""
    if trefs:
        order = dependants_first(live["tables"], trefs)
        for op, field, state in (("reject", "authorizationStatus", "AUTHORIZED"), ("unprovision", "provisioningStatus", "PROVISIONED")):
            run_all(api, op, [t for t in order
                              if any(r.get(field) == state for r in resources_of(api, live["tables"][t]["values"]))], live)
        delete_tables(api, trefs, live)
    for name in domains:
        if any(v["values"]["domain"] == name for v in read_live(api, with_resources=False)["tables"].values()):
            print(f"domain {name}: tables still name it, kept")
            continue
        api.call("DELETE", f"/aggregate/{live['domains'][name]['id']}")
        print(f"domain {name}: deleted from Meta Catalog")


def cmd_remove(api: Api, path: str, dry: bool, yes: bool) -> int:
    f = check(read_docs(path))
    if model_missing(api):
        print(f"remove: Meta Catalog has no model: nothing of {path} to remove")
        return 0
    live = read_live(api)
    trefs = sorted(t for t in f["tables"] if t in live["tables"])
    domains = sorted(d for d in f["domains"] if d in live["domains"])
    if not trefs and not domains:
        print(f"remove: Meta Catalog has nothing of {path}")
        return 0
    removal_order(live["tables"], trefs)      # refused when a table that stays depends on one
    print(f"remove: {len(trefs)} table(s) and {len(domains)} domain(s) of {path} out of Meta Catalog:")
    for t in dependants_first(live["tables"], trefs):
        print(f"  remove {t}: reject, unprovision (DROP the table, DELETE its files), delete")
    for d in domains:
        print(f"  remove domain {d}")
    if dry:
        return 0
    confirm(f"{len(trefs)} table(s) are DROPPED, their files DELETED, and taken out of Meta Catalog.", "remove", yes)
    remove_all(api, trefs, domains, live)
    return 0


def cmd_load(api: Api, paths: list, model: str) -> int:
    ensure_model(api, model)
    for path in paths:
        f = check(read_docs(path))
        live = read_live(api, with_resources=False)
        missing = [d["ref"] for n, d in f["domains"].items() if n not in live["domains"]]
        missing += [d["ref"] for t, d in sorted(f["tables"].items()) if t not in live["tables"]]
        if not missing:
            print(f"load: Meta Catalog has everything of {path} already")
            continue
        create(api, f, missing, live)
    return 0


def selected(live: dict, ref: str) -> list:
    if not REF.match(ref):
        raise Refused(f"'{ref}' is not <domain>[/<layer>[/<name>_v<n>]]")
    found = sorted(t for t in live["tables"] if selects(ref, t))
    if not found and ref not in live["domains"]:
        raise Refused(f"no table {ref} in Meta Catalog (make dataplatform-tables ARGS=list shows them)")
    return found


def cmd_list(api: Api, ref: str = "") -> int:
    live = read_live(api)
    rows = [("TABLE", "FORMAT", "TRINO", "FOLDER", "METASTORE")]
    for tref, t in sorted(live["tables"].items()):
        if ref and not selects(ref, tref):
            continue
        by = {r["type"]: r for r in t["resources"]}

        def st(r):
            return f"{(r or {}).get('provisioningStatus') or '-'}/{(r or {}).get('authorizationStatus') or '-'}"
        rows.append((tref, t["values"].get("format", "?"), trino_name(t["values"]),
                     st(by.get("S3FolderType")), st(by.get("HiveTableType")) if t["part"] else "(no schema)"))
    for name, d in sorted(live["domains"].items()):
        if not ref or ref.split("/")[0] == name:
            print(f"domain {name}: {len(d['dependencies'])} table(s)")
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    for r in rows:
        print("  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip())
    return 0


def cmd_status(api: Api, ref: str) -> int:
    live = read_live(api)
    for tref in selected(live, ref):
        for r in resources_of(api, live["tables"][tref]["values"]):
            grants = ", ".join(g["principal"]["id"] + "=" + "+".join(g["permissions"]) for g in r.get("effectiveGrants") or [])
            print(f"{tref}  {'folder' if r.get('kind') == 'folder' else r.get('format', '?') + ' table'}  "
                  f"{r.get('provisioningStatus') or '-'}  {r.get('authorizationStatus') or '-'}  {grants or '-'}")
            for field in ("provisioningResult", "authorizationResult"):
                if r.get(field):
                    print(f"    {field}: {result_line(r, field)}")
    return 0


def cmd_operation(api: Api, op: str, ref: str, yes: bool) -> int:
    live = read_live(api)
    refs = selected(live, ref)
    if op == "unprovision":
        confirm(f"unprovision DROPS {len(refs)} table(s) and DELETES their files ({', '.join(refs)}).", "unprovision", yes)
    if op == "delete":
        domain = ref if "/" not in ref and ref in live["domains"] else None
        confirm(f"delete takes {len(refs)} table(s){' and the domain ' + domain if domain else ''} out of Meta Catalog.",
                "delete", yes)
        delete_tables(api, refs, live)
        if domain:
            left = [t for t, v in read_live(api, with_resources=False)["tables"].items() if v["values"]["domain"] == domain]
            if left:
                print(f"domain {domain}: tables still name it ({', '.join(sorted(left))}), kept")
            else:
                api.call("DELETE", f"/aggregate/{live['domains'][domain]['id']}")
                print(f"domain {domain}: deleted from Meta Catalog")
        return 0
    order = dependants_first(live["tables"], refs)
    run_all(api, op, order if op in ("reject", "unprovision") else list(reversed(order)), live)
    return 0


def model_of(path: str) -> dict:
    out = {"traits": [], "types": [], "relationships": [], "mappings": []}
    for part in read_docs(path):
        out["traits"] += [t["name"] for t in part.get("Traits") or []]
        out["types"] += [t["name"] for t in part.get("EntityTypes") or []]
        out["relationships"] += [(r["sourceTrait"], r["relationshipType"], r["targetTrait"]) for r in part.get("Relationships") or []]
        out["mappings"] += [(m["sourceEntityType"], m["targetEntityType"]) for m in part.get("Mappings") or []]
    return out


def cmd_unload_model(api: Api, path: str, yes: bool) -> int:
    m = model_of(path)
    for t in m["types"]:
        try:
            left = api.entities(t)
        except Refused as e:
            if "HTTP 404" in str(e) or "HTTP 400" in str(e):
                continue
            raise
        if left:
            raise Refused(f"{len(left)} entities of type {t} are in Meta Catalog: delete them first "
                          "(an aggregate goes with all its parts and resources)")
    confirm(f"This takes the model of {path} out of Meta Catalog: {len(m['mappings'])} mappings, {len(m['types'])} entity types, "
            f"{len(m['relationships'])} relationships, {len(m['traits'])} traits.", os.path.basename(path), yes)
    gone = 0
    for mp in api.call("GET", "/mapping") or []:
        if (mp.get("sourceEntityType"), mp.get("targetEntityType")) in m["mappings"]:
            api.call("DELETE", f"/mapping/{mp['id']}")
            gone += 1
    for t in m["types"]:
        try:
            api.call("DELETE", f"/entity-type/{t}")
            gone += 1
        except Refused as e:
            if "HTTP 404" not in str(e):
                raise
    for s, rel, t in m["relationships"]:
        try:
            api.call("DELETE", f"/trait/link/{s}/{rel}/{t}")
            gone += 1
        except Refused as e:
            if "HTTP 404" not in str(e) and "HTTP 400" not in str(e):
                raise
    for t in m["traits"]:
        try:
            api.call("DELETE", f"/trait/{t}")
            gone += 1
        except Refused as e:
            if "HTTP 404" not in str(e):
                raise
    print(f"model of {os.path.basename(path)}: {gone} definitions taken out of Meta Catalog")
    return 0


COMMANDS = {"check": (0, 1), "model": (0, 0), "sync": (0, 1), "load": (1, 99), "list": (0, 1), "status": (1, 1),
            "provision": (1, 1), "authorize": (1, 1), "reject": (1, 1), "unprovision": (1, 1), "delete": (1, 1),
            "unload-model": (1, 1), "remove": (0, 1)}


def main(argv: list) -> int:
    args, dry, yes = [], False, False
    for a in argv:
        if a == "-n":
            dry = True
        elif a == "-y":
            yes = True
        else:
            args.append(a)
    model = os.environ.get("TABLES_MODEL") or DEFAULT_MODEL
    if not args or args[0] not in COMMANDS or not COMMANDS[args[0]][0] <= len(args) - 1 <= COMMANDS[args[0]][1]:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 2
    cmd, rest = args[0], args[1:]
    try:
        if cmd == "check":
            return cmd_check(rest[0] if rest else DEFAULT_FILE, model)
        api = Api(os.environ["METACATALOG_URL"], os.environ["TABLES_TOKEN"])
        if cmd == "model":
            return cmd_model(api, model, yes)
        if cmd == "sync":
            return cmd_sync(api, rest[0] if rest else DEFAULT_FILE, model, dry, yes)
        if cmd == "load":
            return cmd_load(api, rest, model)
        if cmd == "list":
            return cmd_list(api, rest[0] if rest else "")
        if cmd == "status":
            return cmd_status(api, rest[0])
        if cmd == "remove":
            return cmd_remove(api, rest[0] if rest else DEFAULT_FILE, dry, yes)
        if cmd == "unload-model":
            return cmd_unload_model(api, rest[0], yes)
        return cmd_operation(api, cmd, rest[0], yes)
    except Refused as e:
        print(e, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
