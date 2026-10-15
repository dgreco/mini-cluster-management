#!/usr/bin/env python3
"""What `make dataplatform-tables ARGS=model` sends to Meta Catalog: the
changes of a model file (metacatalog/table-model.yaml, Meta Catalog's bulk
format) to the model Meta Catalog has, as the calls that make them.

  model_plan.py <model.yaml> <live.json>

live.json is Meta Catalog's model as its API answers: {"traits": GET /trait,
"types": GET /entity-type, "mappings": GET /mapping, "links": GET
/trait/relationships}. Prints one JSON object per line, in the order the
calls must be made:

  {"say": ..., "method": "POST", "path": ..., "body": {...}}   a call
  {"note": ...}                                               what Meta Catalog has and the
                                                              file has not: left as it is
and nothing for a model Meta Catalog has already. Fails (exit 1, a message)
on a file Meta Catalog would refuse in a way that can be seen here.

  1. traits: a new one is made; one whose schema or father changed gets a
     version (fathers first). Meta Catalog merges a trait's schema with its
     father's only when the trait is made or versioned, so a trait whose
     father changed gets a version too.
  2. trait links (Relationships): a missing one is made.
  3. entity types: a new one is made; one whose traits or father changed, or
     whose merged schema would come out otherwise (its own schema, or that
     of a trait it carries) gets a version (fathers first). Entities stay
     pinned to the version they were made with.
  4. mappings: a new one is made; one whose values or entity path
     references changed, or whose target type was made or versioned here, is
     sent again (Meta Catalog replaces it in place, same id, stamped with the
     current versions: the entities it derives are validated against the
     target version stamped on their mapping).

Meta Catalog answers with merged schemas only, so the merge is done here as
Meta Catalog does it (JsonUtils.mergeSchemas over TypeLinearization, read in
its code, commit 29815bb): the type, then its traits from the last declared
to the first, then its father, each followed by its own linearization, an
element already there dropped from the left; the base schemas merged in the
reverse of that order, the last definition of a property deciding its value
and whether it is required. A trait the file does not define (Meta Catalog's
own: Provisionable, Aggregate, ...) counts with the schema Meta Catalog
answers for it. Schemas are compared by their properties (as written) and
required (as a set).

Standard library, plus PyYAML to read the model file (requirements.txt).
"""
import json
import sys

EMPTY = {"type": "object", "properties": {}}
# Meta Catalog makes the inverse of every trait link itself
INVERSE = {"DEPENDS_ON": "IS_REQUIRED_BY", "IS_REQUIRED_BY": "DEPENDS_ON", "HAS_PART": "IS_PART_OF",
           "IS_PART_OF": "HAS_PART", "MAPPED_TO": "IS_MAPPED_BY", "IS_MAPPED_BY": "MAPPED_TO"}


class PlanError(Exception):
    pass


def merge(schemas: list) -> dict:
    """JsonUtils.mergeSchemas: properties in order, the last one of a name
    deciding its value and whether it is required"""
    props, req = {}, {}
    for s in schemas:
        if not isinstance(s, dict) or not isinstance(s.get("properties"), dict):
            raise PlanError(f"a schema without properties: {json.dumps(s)[:200]} (Meta Catalog refuses it)")
        required = set(s["required"]) if isinstance(s.get("required"), list) else set()
        for name, value in s["properties"].items():
            props[name] = value
            req[name] = name in required
    return {"type": "object", "properties": props, "required": [n for n in props if req[n]],
            "additionalProperties": False}


def same(a: dict, b: dict) -> bool:
    return (a.get("properties") == b.get("properties")
            and set(a.get("required") or []) == set(b.get("required") or []))


def changes(live: dict, want: dict) -> str:
    """The properties a schema change touches, in words"""
    lp, wp = live.get("properties") or {}, want.get("properties") or {}
    lr, wr = set(live.get("required") or []), set(want.get("required") or [])
    out = []
    for what, names in (("added", [n for n in wp if n not in lp]), ("removed", [n for n in lp if n not in wp]),
                        ("changed", [n for n in wp if n in lp and wp[n] != lp[n]]),
                        ("now required", sorted(wr - lr)), ("no longer required", sorted(lr - wr))):
        if names:
            out.append(f"{', '.join(names)} {what}")
    return "; ".join(out)


def schema_of(obj: dict) -> dict:
    s = obj.get("schema")
    if isinstance(s, str):
        s = json.loads(s) if s.strip() else None
    return s or EMPTY


def loads(value, default):
    if isinstance(value, str):
        return json.loads(value) if value.strip() else default
    return default if value is None else value


def refs(value) -> list:
    return [{"alias": r.get("alias"), "referencePath": r.get("referencePath")} for r in loads(value, [])]


def fathers_first(defs: dict) -> list:
    """The names of defs, each after the father it has among them"""
    order, seen = [], set()

    def visit(name, path):
        if name in seen:
            return
        if name in path:
            raise PlanError(f"{name}: inherits from itself ({' -> '.join(path + [name])})")
        father = defs[name].get("inheritsFrom")
        if father in defs:
            visit(father, path + [name])
        seen.add(name)
        order.append(name)

    for n in defs:
        visit(n, [])
    return order


def read_model(sections: list) -> dict:
    """The parts of a model file (its YAML documents, parsed)"""
    model = {"traits": {}, "links": [], "types": {}, "mappings": []}
    for part in sections:
        for t in (part or {}).get("Traits") or []:
            model["traits"][t["name"]] = {"schema": t.get("schema") or EMPTY, "inheritsFrom": t.get("inheritsFrom")}
        for r in (part or {}).get("Relationships") or []:
            model["links"].append((r["sourceTrait"], r["relationshipType"], r["targetTrait"]))
        for t in (part or {}).get("EntityTypes") or []:
            model["types"][t["name"]] = {"schema": t.get("schema") or EMPTY, "traits": list(t.get("traits") or []),
                                         "inheritsFrom": t.get("inheritsFrom")}
        for m in (part or {}).get("Mappings") or []:
            model["mappings"].append({"sourceEntityType": m["sourceEntityType"],
                                      "targetEntityType": m["targetEntityType"],
                                      "mappingValues": m.get("mappingValues") or {},
                                      "entityPathReferences": refs(m.get("entityPathReferences") or [])})
    return model


def read_live(live: dict) -> dict:
    return {
        "traits": {t["name"]: {"schema": schema_of(t), "inheritsFrom": t.get("inheritsFrom"),
                               "version": t.get("version")} for t in live.get("traits") or []},
        "types": {t["name"]: {"schema": schema_of(t), "traits": list(t.get("traits") or []),
                              "inheritsFrom": t.get("inheritsFrom"), "version": t.get("version")}
                  for t in live.get("types") or []},
        "mappings": {(m["sourceEntityType"], m["targetEntityType"]):
                     {"mappingValues": loads(m.get("mappingValues"), {}),
                      "entityPathReferences": refs(m.get("entityPathReferences"))} for m in live.get("mappings") or []},
        "links": {(r.get("sourceTrait"), r.get("relationType") or r.get("relationshipType"), r.get("targetTrait"))
                  for r in live.get("links") or []},
    }


class Merger:
    """The schemas Meta Catalog would merge for the file's traits and types"""

    def __init__(self, model: dict, live: dict):
        self.model, self.live = model, live
        self.derived = {}

    def trait(self, name: str) -> dict:
        """A trait's merged schema: its father's, then its own"""
        if name not in self.derived:
            if name in self.model["traits"]:
                t = self.model["traits"][name]
                father = t.get("inheritsFrom")
                self.derived[name] = merge(([self.trait(father)] if father else []) + [t["schema"]])
            elif name in self.live["traits"]:
                self.derived[name] = self.live["traits"][name]["schema"]
            else:
                raise PlanError(f"trait {name}: neither in the model file nor in Meta Catalog")
        return self.derived[name]

    def element(self, kind: str, name: str):
        """(base schema, traits, father) of one element of a linearization. A
        trait or type the file does not define counts as one element with the
        schema Meta Catalog answers for it (its fathers merged in already)."""
        if kind == "trait":
            if name in self.model["traits"]:
                t = self.model["traits"][name]
                return t["schema"], [], t.get("inheritsFrom")
            return self.trait(name), [], None
        if name in self.model["types"]:
            t = self.model["types"][name]
            return t["schema"], t["traits"], t.get("inheritsFrom")
        if name in self.live["types"]:
            return self.live["types"][name]["schema"], [], None
        raise PlanError(f"entity type {name}: neither in the model file nor in Meta Catalog")

    def linearize(self, kind: str, name: str) -> list:
        _, traits, father = self.element(kind, name)
        combined = []
        for t in reversed(traits):
            combined = concat(combined, self.linearize("trait", t))
        if father:
            combined = concat(combined, self.linearize(kind, father))
        return [(kind, name)] + combined

    def type(self, name: str) -> dict:
        return merge([self.element(k, n)[0] for k, n in reversed(self.linearize("type", name))])


def concat(left: list, right: list) -> list:
    return [e for e in left if e not in right] + right


def plan(model: dict, live: dict) -> list:
    out = []
    m = Merger(model, live)
    touched = set()          # entity types made or versioned here

    def call(say, method, path, body):
        out.append({"say": say, "method": method, "path": path, "body": body})

    def body_of(name, d, traits=False):
        b = {"name": name, "schema": json.dumps(d["schema"])}
        if traits:
            b["traits"] = d["traits"]
        if d.get("inheritsFrom"):
            b["inheritsFrom"] = d["inheritsFrom"]
        return b

    for name in fathers_first(model["traits"]):
        t, lt = model["traits"][name], live["traits"].get(name)
        want = m.trait(name)
        if lt is None:
            call(f"trait {name}: new", "POST", "/trait", body_of(name, t))
            continue
        why = []
        if (t.get("inheritsFrom") or None) != (lt.get("inheritsFrom") or None):
            why.append(f"father {lt.get('inheritsFrom') or 'none'} -> {t.get('inheritsFrom') or 'none'}")
        if not same(lt["schema"], want):
            why.append(changes(lt["schema"], want) or "its schema")
        if why:
            call(f"trait {name}: version {(lt.get('version') or 0) + 1} ({'; '.join(why)})",
                 "POST", f"/trait/{name}/versions", body_of(name, t))

    for link in model["links"]:
        if link not in live["links"]:
            src, rel, tgt = link
            call(f"trait link {src} {rel} {tgt}: new", "POST", "/trait/link",
                 {"sourceTrait": src, "relationshipTypeName": rel, "targetTrait": tgt})

    types = []               # (name, call), fathers first
    for name in fathers_first(model["types"]):
        t, lt = model["types"][name], live["types"].get(name)
        want = m.type(name)
        if lt is None:
            types.append((name, {"say": f"entity type {name}: new", "method": "POST", "path": "/entity-type",
                                 "body": body_of(name, t, traits=True)}))
            touched.add(name)
            continue
        why = []
        if set(t["traits"]) != set(lt["traits"]):
            added = [x for x in t["traits"] if x not in lt["traits"]]
            removed = [x for x in lt["traits"] if x not in t["traits"]]
            why.append("traits " + ", ".join([f"+{x}" for x in added] + [f"-{x}" for x in removed]))
        if (t.get("inheritsFrom") or None) != (lt.get("inheritsFrom") or None):
            why.append(f"father {lt.get('inheritsFrom') or 'none'} -> {t.get('inheritsFrom') or 'none'}")
        if not same(lt["schema"], want):
            why.append(changes(lt["schema"], want) or "its schema")
        if why:
            types.append((name, {"say": f"entity type {name}: version {(lt.get('version') or 0) + 1} "
                                        f"({'; '.join(why)}); its entities keep the version they were made with",
                                 "method": "POST", "path": f"/entity-type/{name}/versions",
                                 "body": body_of(name, t, traits=True)}))
            touched.add(name)

    # A mapping is checked against its target's merged schema (Meta Catalog:
    # its values must be properties of it, its required properties mapped)
    # when it is sent, and again when its target gets a version. A mapping
    # whose target changes is therefore sent after the target's version when
    # what it maps now still fits the new version (a property added), and
    # before it, then once more to be stamped with the new version, when the
    # new values fit the old one (a property removed). A change that fits
    # neither way (a mapped property renamed) is two changes.
    def fits(values: dict, schema: dict) -> bool:
        props = set((schema.get("properties") or {}))
        return set(values) <= props and set(schema.get("required") or []) <= set(values)

    def misfit(values: dict, schema: dict) -> str:
        props, req = set(schema.get("properties") or {}), set(schema.get("required") or [])
        out = []
        if set(values) - props:
            out.append(f"{', '.join(sorted(set(values) - props))} not properties of it")
        if req - set(values):
            out.append(f"its required {', '.join(sorted(req - set(values)))} not mapped")
        return "; ".join(out)

    before, after = {}, []
    for mp in model["mappings"]:
        key = (mp["sourceEntityType"], mp["targetEntityType"])
        target = key[1]
        lm = live["mappings"].get(key)
        body = {"sourceEntityType": key[0], "targetEntityType": target,
                "mappingValues": json.dumps(mp["mappingValues"]),
                "entityPathReferences": json.dumps(mp["entityPathReferences"])}
        label = f"mapping {key[0]} -> {target}"
        new_target = m.type(target) if target in model["types"] else live["types"].get(target, {}).get("schema")
        if new_target is None:
            raise PlanError(f"{label}: entity type {target} is neither in the model file nor in Meta Catalog")
        if not fits(mp["mappingValues"], new_target):
            raise PlanError(f"{label}: its values do not fit {target} ({misfit(mp['mappingValues'], new_target)})")
        if lm is None:
            after.append({"say": f"{label}: new", "method": "POST", "path": "/mapping", "body": body})
            continue
        why = []
        lv, wv = lm["mappingValues"], mp["mappingValues"]
        names = [n for n in sorted(set(lv) | set(wv)) if lv.get(n) != wv.get(n)]
        if names:
            why.append(f"values {', '.join(names)} changed")
        if lm["entityPathReferences"] != mp["entityPathReferences"]:
            why.append("entity path references changed")
        derived = "what it derived before follows at its source's next edit"
        if target in touched and target in live["types"]:
            old_target = live["types"][target]["schema"]
            if fits(lv, new_target):
                why.append(f"{target} has a new version")
            elif fits(wv, old_target):
                before.setdefault(target, []).append(
                    {"say": f"{label}: sent first, the new version of {target} needs it ({'; '.join(why)})",
                     "method": "POST", "path": "/mapping", "body": body})
                why = [f"{target} has a new version"]
            else:
                raise PlanError(f"{label} and {target} change so that neither can go first (the mapping now "
                                f"in Meta Catalog does not fit the new {target}: {misfit(lv, new_target)}; the new "
                                f"mapping does not fit the {target} there: {misfit(wv, old_target)}): make it two "
                                "changes, the new property first, the old one taken out afterwards")
        elif names and not fits(wv, live["types"].get(target, {}).get("schema") or new_target):
            raise PlanError(f"{label}: its values do not fit {target} in Meta Catalog "
                            f"({misfit(wv, live['types'][target]['schema'])})")
        if why:
            after.append({"say": f"{label}: sent again ({'; '.join(why)}); {derived}",
                          "method": "POST", "path": "/mapping", "body": body})

    for name, c in types:
        out.extend(before.get(name, []))
        out.append(c)
    out.extend(after)

    for name in live["types"]:
        if name not in model["types"]:
            out.append({"note": f"entity type {name} is in Meta Catalog, not in the model file: left as it is"})
    # Meta Catalog's own traits (Provisionable, Aggregate, ...) are carried by the file's types
    used = {x for t in model["types"].values() for x in t["traits"]} | \
        {t.get("inheritsFrom") for t in model["traits"].values()}
    for name in live["traits"]:
        if name not in model["traits"] and name not in used:
            out.append({"note": f"trait {name} is in Meta Catalog, not in the model file: left as it is"})
    file_maps = {(x["sourceEntityType"], x["targetEntityType"]) for x in model["mappings"]}
    file_links = set(model["links"]) | {(t, INVERSE.get(r), s) for s, r, t in model["links"]}
    for key in live["mappings"]:
        if key not in file_maps:
            out.append({"note": f"mapping {key[0]} -> {key[1]} is in Meta Catalog, not in the model file: left as it is"})
    for link in sorted(live["links"], key=str):
        inverse_of_live = link[1] in ("IS_REQUIRED_BY", "IS_PART_OF", "IS_MAPPED_BY") and \
            (link[2], INVERSE[link[1]], link[0]) in live["links"]
        if link[0] in model["traits"] and link[2] in model["traits"] and link not in file_links and not inverse_of_live:
            out.append({"note": f"trait link {' '.join(map(str, link))} is in Meta Catalog, not in the model file: left as it is"})
    return out


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 2
    try:
        import yaml
    except ImportError:
        print("PyYAML is needed to read the model: pip3 install pyyaml (requirements.txt)", file=sys.stderr)
        return 1
    with open(sys.argv[1]) as f:
        model = read_model(list(yaml.safe_load_all(f)))
    with open(sys.argv[2]) as f:
        live = read_live(json.load(f))
    try:
        for line in plan(model, live):
            print(json.dumps(line))
    except PlanError as e:
        print(f"{sys.argv[1]}: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
