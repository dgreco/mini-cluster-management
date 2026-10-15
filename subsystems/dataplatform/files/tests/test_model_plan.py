"""Unit tests of model_plan.py (make test): how a model file's changes become
Meta Catalog calls. The merge as Meta Catalog does it, the order of the
calls, and what is refused before anything is sent. No network."""
import copy
import importlib.util
import json
import pathlib
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "model_plan.py"
spec = importlib.util.spec_from_file_location("model_plan", SCRIPT)
mp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mp)


def obj(props, required=()):
    return {"type": "object", "properties": props, "required": list(required)}


def merged(props, required=()):
    """A schema as Meta Catalog answers it, merged"""
    return {"type": "object", "properties": props, "required": list(required), "additionalProperties": False}


S = {"type": "string"}
FORMAT = {"type": "string", "enum": ["parquet", "iceberg"]}

# a model: a trait, a type carrying it and a built-in trait, a derived type, a mapping
MODEL = {
    "traits": {"Formatted": {"schema": obj({"format": FORMAT}, ["format"]), "inheritsFrom": None}},
    "links": [("Formatted", "DEPENDS_ON", "Formatted")],
    "types": {
        "Table": {"schema": obj({"name": S}, ["name"]), "traits": ["Provisionable", "Formatted"], "inheritsFrom": None},
        "Folder": {"schema": obj({"path": S, "kind": S}), "traits": [], "inheritsFrom": None},
    },
    "mappings": [{"sourceEntityType": "Table", "targetEntityType": "Folder",
                  "mappingValues": {"path": "#source.getValue('$.name').asText()", "kind": "'folder'"},
                  "entityPathReferences": []}],
}
PROVISIONABLE = merged({"provisioningStatus": S})
# what Meta Catalog answers for that model, loaded
LIVE = {
    "traits": [{"name": "Provisionable", "schema": json.dumps(PROVISIONABLE), "version": 1},
               {"name": "Formatted", "schema": json.dumps(merged({"format": FORMAT}, ["format"])), "version": 1}],
    "types": [{"name": "Table", "version": 1, "traits": ["Formatted", "Provisionable"],
               "schema": json.dumps(merged({"provisioningStatus": S, "format": FORMAT, "name": S}, ["format", "name"]))},
              {"name": "Folder", "version": 1, "traits": [], "schema": json.dumps(merged({"path": S, "kind": S}))}],
    "mappings": [{"id": "m1", "sourceEntityType": "Table", "targetEntityType": "Folder",
                  "mappingValues": json.dumps({"kind": "'folder'", "path": "#source.getValue('$.name').asText()"}),
                  "entityPathReferences": "[]"}],
    "links": [{"sourceTrait": "Formatted", "relationType": "DEPENDS_ON", "targetTrait": "Formatted"},
              {"sourceTrait": "Formatted", "relationType": "IS_REQUIRED_BY", "targetTrait": "Formatted"}],
}


def plan(model=None, live=None):
    return mp.plan(model or MODEL, mp.read_live(live or LIVE))


def calls(lines):
    return [(c["path"], c["say"].split(" (")[0]) for c in lines if "method" in c]


def notes(lines):
    return [n["note"] for n in lines if "note" in n]


class Merge(unittest.TestCase):
    def test_last_definition_decides_value_and_required(self):
        out = mp.merge([obj({"a": S, "b": S}, ["a"]), obj({"a": {"type": "integer"}})])
        self.assertEqual(out["properties"], {"a": {"type": "integer"}, "b": S})
        self.assertEqual(out["required"], [])

    def test_a_schema_without_properties_is_refused(self):
        with self.assertRaises(mp.PlanError):
            mp.merge([{"type": "object"}])

    def test_linearization_is_scala_s(self):
        model = {"traits": {"A": {"schema": obj({}), "inheritsFrom": "X"}, "B": {"schema": obj({}), "inheritsFrom": None},
                            "X": {"schema": obj({}), "inheritsFrom": None}},
                 "links": [], "mappings": [],
                 "types": {"T": {"schema": obj({}), "traits": ["A", "B"], "inheritsFrom": "F"},
                           "F": {"schema": obj({}), "traits": ["X"], "inheritsFrom": None}}}
        m = mp.Merger(model, mp.read_live({}))
        # T, then its traits from the last declared, then its father; X only where it comes last
        self.assertEqual([n for _, n in m.linearize("type", "T")], ["T", "B", "A", "F", "X"])


class Plan(unittest.TestCase):
    def test_nothing_for_the_model_meta_catalog_has(self):
        self.assertEqual(plan(), [])

    def test_a_trait_change_versions_the_trait_and_the_types_that_carry_it(self):
        model = copy.deepcopy(MODEL)
        model["traits"]["Formatted"]["schema"]["properties"]["format"] = {"type": "string", "enum": ["parquet", "iceberg", "orc"]}
        self.assertEqual(calls(plan(model)), [("/trait/Formatted/versions", "trait Formatted: version 2"),
                                              ("/entity-type/Table/versions", "entity type Table: version 2")])

    def test_a_new_trait_on_a_type(self):
        model = copy.deepcopy(MODEL)
        model["traits"]["Owned"] = {"schema": obj({"owner": S}), "inheritsFrom": None}
        model["types"]["Table"]["traits"].append("Owned")
        self.assertEqual(calls(plan(model)), [("/trait", "trait Owned: new"),
                                              ("/entity-type/Table/versions", "entity type Table: version 2")])

    def test_a_property_added_to_a_mapping_target_goes_before_its_mapping(self):
        model = copy.deepcopy(MODEL)
        model["types"]["Folder"]["schema"]["properties"]["region"] = S
        model["mappings"][0]["mappingValues"]["region"] = "'default'"
        self.assertEqual(calls(plan(model)), [("/entity-type/Folder/versions", "entity type Folder: version 2"),
                                              ("/mapping", "mapping Table -> Folder: sent again")])

    def test_a_property_taken_out_of_a_mapping_target_goes_after_its_mapping(self):
        model = copy.deepcopy(MODEL)
        del model["types"]["Folder"]["schema"]["properties"]["kind"]
        del model["mappings"][0]["mappingValues"]["kind"]
        got = calls(plan(model))
        self.assertEqual([p for p, _ in got], ["/mapping", "/entity-type/Folder/versions", "/mapping"])
        self.assertTrue(got[0][1].startswith("mapping Table -> Folder: sent first"))

    def test_a_target_version_alone_sends_its_mapping_again(self):
        model = copy.deepcopy(MODEL)
        model["types"]["Folder"]["schema"]["properties"]["note"] = S
        self.assertEqual(calls(plan(model)), [("/entity-type/Folder/versions", "entity type Folder: version 2"),
                                              ("/mapping", "mapping Table -> Folder: sent again")])

    def test_a_renamed_mapped_property_is_two_changes(self):
        model = copy.deepcopy(MODEL)
        props = model["types"]["Folder"]["schema"]["properties"]
        props["location"] = props.pop("path")
        values = model["mappings"][0]["mappingValues"]
        values["location"] = values.pop("path")
        with self.assertRaisesRegex(mp.PlanError, "neither can go first"):
            plan(model)

    def test_a_mapped_value_that_is_no_property_is_refused(self):
        model = copy.deepcopy(MODEL)
        model["mappings"][0]["mappingValues"]["nosuch"] = "'x'"
        with self.assertRaisesRegex(mp.PlanError, "nosuch not properties of it"):
            plan(model)

    def test_a_required_property_left_unmapped_is_refused(self):
        model = copy.deepcopy(MODEL)
        model["types"]["Folder"]["schema"]["required"] = ["path", "kind"]
        del model["mappings"][0]["mappingValues"]["kind"]
        with self.assertRaisesRegex(mp.PlanError, "required kind not mapped"):
            plan(model)

    def test_new_link_type_and_mapping(self):
        model = copy.deepcopy(MODEL)
        model["links"].append(("Formatted", "HAS_PART", "Formatted"))
        model["types"]["Table2"] = {"schema": obj({"x": S}), "traits": [], "inheritsFrom": None}
        model["mappings"].append({"sourceEntityType": "Table2", "targetEntityType": "Folder",
                                  "mappingValues": {"path": "'p'"}, "entityPathReferences": []})
        self.assertEqual(calls(plan(model)), [("/trait/link", "trait link Formatted HAS_PART Formatted: new"),
                                              ("/entity-type", "entity type Table2: new"),
                                              ("/mapping", "mapping Table2 -> Folder: new")])


class Notes(unittest.TestCase):
    def test_what_only_meta_catalog_has_is_shown_once_and_left(self):
        live = copy.deepcopy(LIVE)
        live["types"].append({"name": "Stray", "version": 1, "traits": [], "schema": json.dumps(merged({}))})
        live["traits"].append({"name": "Unused", "version": 1, "schema": json.dumps(merged({}))})
        model = copy.deepcopy(MODEL)
        model["links"] = []
        out = plan(model, live)
        self.assertEqual(calls(out), [])
        self.assertEqual(sorted(notes(out)), [
            "entity type Stray is in Meta Catalog, not in the model file: left as it is",
            "trait Unused is in Meta Catalog, not in the model file: left as it is",
            "trait link Formatted DEPENDS_ON Formatted is in Meta Catalog, not in the model file: left as it is"])

    def test_meta_catalog_s_own_traits_are_no_note(self):
        self.assertEqual(notes(plan()), [])


if __name__ == "__main__":
    unittest.main()
