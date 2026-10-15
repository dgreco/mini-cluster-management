#!/usr/bin/env python3
"""What `make dataplatform-tables ARGS=sync` does to bring Meta Catalog's
tables to a file of tables (metacatalog/tables/tables.yaml): the steps, in
the order they must run.

  tables_plan.py <tables.yaml> <live.json> <scope.json>

live.json is the tables Meta Catalog has, as dp-tables.sh gathers them: a
list of {id, values, part: {id, values} | null, depends: [<identity>],
resources: [{kind, provisioningStatus, authorizationStatus}]}, an identity
being <domain>/<subdomain>/<layer>/<name>. scope.json says which tables the
file owns: {"only": [domains]} or {"except": [domains]}. Prints one JSON
object per line:

  {"op": ..., "say": ..., ...}   a step: create (with the documents to load),
                                 update (an entity's new values, and the
                                 resource whose re-derivation to wait for),
                                 link, unlink, provision, authorize, reject,
                                 remove (reject, unprovision, delete)
  {"note": ...}                  something sync leaves as it is
and nothing when Meta Catalog has the file's tables already. Refuses (exit
1, every reason on standard error) before any step when the file says
something sync cannot do, or would remove every table.

For each table of the file:
  - not in Meta Catalog: create; then provision; then authorize, or nothing
    when it has no grants
  - in Meta Catalog: its values (the four status fields left aside) and its
    schema's values are put back as the file says when they differ. Meta
    Catalog derives the folder again when a table's values change, and the
    metastore table when its schema's do, and the derived resource loses its
    status then: such a table is provisioned and authorized (or rejected)
    again. So is one whose resources are not all provisioned, or not all
    authorized, when it has grants
  - lineage: the DEPENDS_ON links between tables of the scope are those of
    the file's dependsOn (links to tables outside it are left alone)
and every table of the scope that the file does not have is removed:
rejected, unprovisioned (dropped, its files deleted) and deleted.

Refused: a document that is not a TableType, two documents with one ref or
one table, a table outside the scope, a dependsOn that names no ref of the
file, no table at all; for a table Meta Catalog has, another format, a
schema added or taken away, a column whose type changed, resources missing
(a derivation that failed). A column taken out of the file stays in the
table (provisioning never drops one): a note says so.

Standard library, plus PyYAML to read the file (requirements.txt).
"""
import json
import re
import sys

STATUS = ("provisioningStatus", "provisioningResult", "authorizationStatus", "authorizationResult")
NAME = re.compile(r"^[a-z][a-z0-9]{0,23}$")
TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def identity(values: dict) -> str:
    return "/".join(str(values.get(k)) for k in ("domain", "subdomain", "layer", "name"))


def in_scope(domain: str, scope: dict) -> bool:
    if "only" in scope:
        return domain in scope["only"]
    return domain not in scope.get("except", [])


def own(values: dict) -> dict:
    """A table's values without what the machinery writes"""
    return {k: v for k, v in values.items() if k not in STATUS}


def columns(values) -> dict:
    return {c.get("name"): c.get("type") for c in ((values or {}).get("columns") or [])}


def plan(entries: list, live: list, scope: dict) -> tuple:
    """(steps, refusals) for the file's documents (parsed) and Meta Catalog's tables"""
    refuse, steps, notes = [], [], []
    wanted, refs = {}, {}
    for e in entries:
        if not isinstance(e, dict):
            refuse.append(f"a document that is not a mapping: {str(e)[:80]}")
            continue
        v = e.get("values") or {}
        ident = identity(v)
        if e.get("entityType") != "TableType":
            refuse.append(f"{ident}: a document of type {e.get('entityType')!r}, only TableType")
            continue
        if not (all(NAME.match(str(v.get(k, ""))) for k in ("domain", "subdomain"))
                and v.get("layer") in ("bronze", "silver", "gold") and TABLE_NAME.match(str(v.get("name", "")))):
            refuse.append(f"{ident}: domain and subdomain are lower-case letters and digits, the layer bronze, silver or "
                          "gold, the name lower-case letters, digits and _")
            continue
        if not in_scope(v.get("domain"), scope):
            refuse.append(f"{ident}: the domain {v.get('domain')} is not this file's (it is the smoke test's own)")
            continue
        if ident in wanted:
            refuse.append(f"{ident}: two documents for one table")
            continue
        ref = e.get("ref") or v.get("name")
        if ref in refs:
            refuse.append(f"{ident}: the ref {ref!r} is the one of {refs[ref]} too")
            continue
        refs[ref] = ident
        wanted[ident] = e
    for ident, e in wanted.items():
        for r in e.get("dependsOn") or []:
            if r not in refs:
                refuse.append(f"{ident}: dependsOn {r!r} names no table of the file")
    if not wanted and not refuse:
        refuse.append("the file holds no table: sync would remove every table of the scope "
                      "(dp-tables.sh delete takes tables out on purpose)")

    have = {identity(t["values"]): t for t in live if in_scope(t["values"].get("domain"), scope)}
    new = [i for i in wanted if i not in have]
    gone = [i for i in have if i not in wanted]

    # what an existing table may not do
    for ident in wanted:
        if ident not in have:
            continue
        e, t = wanted[ident], have[ident]
        fv, lv = e.get("values") or {}, t["values"]
        fpart = (e.get("parts") or [{}])[0].get("values") if e.get("parts") else None
        if fv.get("format") != lv.get("format"):
            refuse.append(f"{ident}: its format is {lv.get('format')} in Meta Catalog, {fv.get('format')} in the file: "
                          "a table's format does not change (give it another name: sync then drops the old table)")
        if (fpart is None) != (t.get("part") is None):
            refuse.append(f"{ident}: the file {'adds a schema to' if fpart is not None else 'takes the schema off'} a table "
                          "Meta Catalog has: its table in the metastore would have to be made or dropped "
                          "(give it another name: sync then drops the old table)")
        elif fpart is not None:
            have_cols, want_cols = columns(t["part"]["values"]), columns(fpart)
            for name, typ in want_cols.items():
                if name in have_cols and have_cols[name] != typ:
                    refuse.append(f"{ident}: column {name} is {have_cols[name]}, the file says {typ}: a column's type "
                                  "does not change (provisioning refuses it; change it by hand in Trino, then here)")
            dropped = [n for n in have_cols if n not in want_cols]
            if dropped:
                notes.append(f"{ident}: column(s) {', '.join(dropped)} no longer in the file: they stay in the table "
                             "(provisioning never drops a column)")
        expected = 2 if t.get("part") is not None else 1
        if len(t.get("resources") or []) < expected:
            refuse.append(f"{ident}: Meta Catalog derived {len(t.get('resources') or [])} of its {expected} resources "
                          "(a derivation that failed): take it out of the file, sync, put it back, sync")
    if refuse:
        return [], refuse

    def step(op, ident, say, **kw):
        steps.append({"op": op, "table": ident, "say": f"{ident}: {say}", **kw})

    if new:
        docs = []
        for ident in new:
            e = dict(wanted[ident])
            # dependsOn reaches only documents loaded together: links to tables
            # Meta Catalog has already are made afterwards
            deps = [r for r in (e.get("dependsOn") or []) if refs[r] in new]
            if deps:
                e["dependsOn"] = deps
            else:
                e.pop("dependsOn", None)
            docs.append(e)
        steps.append({"op": "create", "tables": new, "documents": docs,
                      "say": f"new: {', '.join(new)} (made in Meta Catalog)"})

    provision, authorize, reject = [], [], []
    for ident in wanted:
        e = wanted[ident]
        fv = e.get("values") or {}
        grants = bool(fv.get("grants"))
        if ident in new:
            provision.append(ident)
            if grants:
                authorize.append(ident)
            continue
        t = have[ident]
        changed = False
        if own(t["values"]) != fv:
            diff = sorted(k for k in set(own(t["values"])) | set(fv) if own(t["values"]).get(k) != fv.get(k))
            values = dict(fv)
            values.update({k: t["values"][k] for k in STATUS if k in t["values"]})
            step("update", ident, f"{', '.join(diff)} as the file says", entity=t["id"], entityType="TableType",
                 values=values, wait="folder")
            changed = True
        fpart = (e.get("parts") or [{}])[0].get("values") if e.get("parts") else None
        if fpart is not None and t["part"]["values"] != fpart:
            added = [n for n in columns(fpart) if n not in columns(t["part"]["values"])]
            say = f"schema as the file says{' (columns ' + ', '.join(added) + ' added)' if added else ''}"
            step("update", ident, say, entity=t["part"]["id"], entityType="TableSchemaType", values=fpart, wait="table")
            changed = True
        resources = t.get("resources") or []
        if changed or not all(r.get("provisioningStatus") == "PROVISIONED" for r in resources):
            provision.append(ident)
        if grants and (changed or not all(r.get("authorizationStatus") == "AUTHORIZED" for r in resources)):
            authorize.append(ident)
        if not grants and (changed or any(r.get("authorizationStatus") == "AUTHORIZED" for r in resources)):
            reject.append(ident)

    for ident in wanted:
        e = wanted[ident]
        want_deps = {refs[r] for r in (e.get("dependsOn") or [])}
        if ident in new:
            for d in sorted(want_deps):
                if d not in new:
                    step("link", ident, f"lineage: depends on {d}", target=d)
            continue
        have_deps = {d for d in have[ident].get("depends") or [] if d in have or d in wanted}
        for d in sorted(want_deps - have_deps):
            step("link", ident, f"lineage: depends on {d}", target=d)
        for d in sorted(have_deps - want_deps):
            if d in gone:
                continue          # the removal takes the link away
            step("unlink", ident, f"lineage: no longer depends on {d}", target=d)

    for ident in provision:
        step("provision", ident, "provision (its folder, then its table in the metastore)")
    for ident in authorize:
        step("authorize", ident, "authorize its grants (Ceph at once, Trino within a minute)")
    for ident in reject:
        step("reject", ident, "reject (the file gives it no grants)")
    for ident in gone:
        step("remove", ident, "REMOVE: not in the file; reject, unprovision (DROPS the table, DELETES its files), "
                              "take it out of Meta Catalog")
    return steps + [{"note": n} for n in notes], []


def main() -> int:
    if len(sys.argv) != 4:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 2
    try:
        import yaml
    except ImportError:
        print("PyYAML is needed to read the file: pip3 install pyyaml (requirements.txt)", file=sys.stderr)
        return 1
    try:
        with open(sys.argv[1]) as f:
            entries = [e for e in yaml.safe_load_all(f) if e is not None]
    except yaml.YAMLError as e:
        print(f"{sys.argv[1]}: not YAML: {e}", file=sys.stderr)
        return 1
    with open(sys.argv[2]) as f:
        live = json.load(f)
    with open(sys.argv[3]) as f:
        scope = json.load(f)
    steps, refusals = plan(entries, live, scope)
    if refusals:
        for r in refusals:
            print(f"{sys.argv[1]}: {r}", file=sys.stderr)
        return 1
    for s in steps:
        print(json.dumps(s))
    return 0


if __name__ == "__main__":
    sys.exit(main())
