"""The platform's tables, read from subsystems/dataplatform/metacatalog/
tables/tables.yaml (the file make dataplatform-tables brings Meta Catalog to,
and Airflow reads through git-sync like the DAGs): the DAGs and the SQL
name a table by <domain>.<layer>.<name>, without a version, and get the
highest version the file defines, so that a new version of a table is picked
up by the pipeline once it is in the file (and applied).

The names follow the platform's model (metacatalog/table-model.yaml):

  Trino   lakehouse.<domain>_<layer>.<name>_v<version>
  folder  s3://<bucket>/<prefix>/<domain>/<layer>/<name>_v<version>
"""
import os
import re
from dataclasses import dataclass, field

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_FILE = os.path.normpath(os.path.join(HERE, "..", "..", "..", "..", "metacatalog", "tables", "tables.yaml"))
TABLE_TYPE = "TableType"
PLACEHOLDER = re.compile(r"\{([a-z][a-z0-9]*)\.(bronze|silver|gold)\.([a-z][a-z0-9_]*)\}")


@dataclass(frozen=True)
class Table:
    domain: str
    layer: str          # bronze, silver, gold
    name: str
    version: int
    format: str
    columns: tuple = field(default=())     # ((name, type), ...)
    depends: tuple = field(default=())     # keys of the tables it is built from (its lineage)

    @property
    def key(self) -> str:
        """<domain>.<layer>.<name>: how the DAGs and the SQL name it"""
        return f"{self.domain}.{self.layer}.{self.name}"

    @property
    def versioned(self) -> str:
        return f"{self.name}_v{self.version}"

    def trino(self, catalog: str = "lakehouse") -> str:
        return f"{catalog}.{self.domain}_{self.layer}.{self.versioned}"

    def folder(self, prefix: str = "lakehouse") -> str:
        return f"{prefix}/{self.domain}/{self.layer}/{self.versioned}"

    def location(self, bucket: str, prefix: str = "lakehouse", scheme: str = "s3") -> str:
        return f"{scheme}://{bucket}/{self.folder(prefix)}"


def read_tables(path: str = "") -> dict:
    """{key: Table} at the highest version of each table in the file."""
    with open(path or os.environ.get("LAKEHOUSE_FILE") or DEFAULT_FILE) as f:
        docs = [d for d in yaml.safe_load_all(f) if d]
    key_of = {d["ref"]: "{domain}.{}.{name}".format(str(d["values"]["layer"]).lower(), **d["values"])
              for d in docs if d.get("entityType") == TABLE_TYPE}
    out = {}
    for d in docs:
        if d.get("entityType") != TABLE_TYPE:
            continue
        v = d["values"]
        parts = d.get("parts") or [{}]
        cols = tuple((c["name"], c["type"]) for c in (parts[0].get("values") or {}).get("columns") or [])
        deps = tuple(sorted({key_of[r] for r in d.get("dependsOn") or []}))
        t = Table(v["domain"], str(v["layer"]).lower(), v["name"], int(v["version"]), v["format"], cols, deps)
        if t.key not in out or out[t.key].version < t.version:
            out[t.key] = t
    return out


def built(tables: dict) -> list:
    """The silver and gold tables, each after those it is built from: the
    pipeline's steps, in Meta Catalog's lineage."""
    left = {k: t for k, t in tables.items() if t.layer != "bronze"}
    order = []
    while left:
        ready = sorted(k for k, t in left.items() if not any(d in left for d in t.depends))
        if not ready:
            raise ValueError("the lineage of " + ", ".join(sorted(left)) + " goes round")
        order += ready
        for k in ready:
            del left[k]
    return order


def render(sql: str, tables: dict, catalog: str = "lakehouse") -> str:
    """The SQL with every {<domain>.<layer>.<name>} replaced by the table's
    Trino name; an unknown table fails."""
    def name(m):
        key = ".".join(m.groups())
        if key not in tables:
            raise KeyError(f"{{{key}}}: no such table in tables.yaml")
        return tables[key].trino(catalog)
    return PLACEHOLDER.sub(name, sql)


def statements(sql: str) -> list:
    """The statements of a file: split at ';' that ends a line, comments
    (-- to the end of a line) dropped."""
    lines = [re.sub(r"--.*$", "", line) for line in sql.splitlines()]
    out, current = [], []
    for line in lines:
        current.append(line)
        if line.rstrip().endswith(";"):
            out.append("\n".join(current).strip().rstrip(";").strip())
            current = []
    rest = "\n".join(current).strip()
    if rest:
        out.append(rest)
    return [s for s in out if s]
