# Trino's access control, decided here by the Open Policy Agent: Trino asks
# before every statement (spec.clusterConfig.authorization.opa in trino.yaml,
# package trino). Deny by default; what is allowed:
#
#   Meta Catalog's provisioner   everything in the platform's catalog: it is
#                                the only one that creates, alters or drops a
#                                schema or a table (admins included)
#   a table's grants             a user or group granted READ on a table in
#                                Meta Catalog sees it, its columns and its
#                                schema, and may SELECT; granted WRITE, also
#                                INSERT, UPDATE, DELETE, MERGE, TRUNCATE and
#                                the table procedures (optimize, ...)
#   the admin groups             read and write every table (as their STS role
#                                does on the files); no DDL
#   everybody                    run queries, see the catalog and its
#                                information_schema (filtered to what they
#                                may read), read the system catalog, use the
#                                built-in functions, set session properties,
#                                see and kill their own queries
#
# The grants are data, not rules: iam-sync writes them (package dataplatform,
# ConfigMap trino-grants) from the record of grants that Meta Catalog's
# authorize and reject keep, with the provisioner and the admin groups of
# gitops/iam/access.yaml. Until it has, nothing below matches and Trino
# refuses everything but the general statements.
#
# This decides statements only. The files stay Ceph's to refuse: Trino reads
# and writes them with the user's own STS role (trino-catalog-lakehouse.yaml).
#
# The catalog gate (manifests/governance/catalog-gate) asks the same rules
# for a Spark job's user, with requests shaped as Trino's: SelectFromColumns
# to read a table's definition, InsertIntoTable to commit to it, ShowTables,
# FilterSchemas and FilterTables for the lists. A change of the rules on the
# tables changes what the jobs may do as well.
#
# Tests: trino_test.rego (make dataplatform-policy-test).
package trino

import data.dataplatform

default allow := false

user := input.context.identity.user

groups := object.get(input.context.identity, "groups", [])

operation := input.action.operation

resource := input.action.resource

# ---------------------------------------------------------------- who ----
is_provisioner if user == dataplatform.provisioner

is_admin if {
	some group in groups
	group in dataplatform.admin_groups
}

# a grant ({"users": [...], "groups": [...]}) names the user or one of their groups
names_user(grant) if user in grant.users

names_user(grant) if {
	some group in groups
	group in grant.groups
}

# "orders$snapshots" (an Iceberg metadata table) is the table orders
base_table(name) := split(name, "$")[0]

may_write(schema, table) if is_admin

may_write(schema, table) if names_user(dataplatform.tables[schema][base_table(table)].rw)

may_read(schema, table) if may_write(schema, table)

may_read(schema, table) if names_user(dataplatform.tables[schema][base_table(table)].ro)

sees_schema(schema) if is_admin

sees_schema(schema) if {
	some table, _ in dataplatform.tables[schema]
	may_read(schema, table)
}

# ---------------------------------------------------------------- what ----
# the catalogs every user finds: the platform's and Trino's own
catalogs := {dataplatform.catalog, "system"}

read_operations := {"SelectFromColumns", "ShowColumns", "FilterColumns", "FilterTables", "ShowCreateTable"}

write_operations := {"InsertIntoTable", "DeleteFromTable", "UpdateTableColumns", "TruncateTable", "ExecuteTableProcedure"}

schema_operations := {"FilterSchemas", "ShowTables", "ShowFunctions", "ShowCreateSchema"}

# the catalog an object of the request is in, whatever kind it is
catalog_of(r) := r.table.catalogName

catalog_of(r) := r.schema.catalogName

catalog_of(r) := r.function.catalogName

catalog_of(r) := r.catalog.name

catalog_of(r) := r.catalogSessionProperty.catalogName

# ---------------------------------------------------------------- everybody ----
allow if operation == "ExecuteQuery"

allow if operation == "SetSystemSessionProperty"

allow if {
	operation in {"AccessCatalog", "FilterCatalogs", "ShowSchemas", "SetCatalogSessionProperty"}
	catalog_of(resource) in catalogs
}

# Trino's own catalog: the JDBC metadata, the runtime tables (a user's own
# queries: the rules on queries below), nothing to change
allow if {
	operation in read_operations
	resource.table.catalogName == "system"
}

allow if {
	operation in schema_operations
	resource.schema.catalogName == "system"
}

allow if {
	operation in {"ExecuteFunction", "FilterFunctions"}
	resource.function.catalogName == "system"
	resource.function.schemaName == "builtin"
}

# information_schema: its rows are filtered by the rules on schemas, tables
# and columns
allow if {
	operation in schema_operations
	resource.schema.catalogName == dataplatform.catalog
	resource.schema.schemaName == "information_schema"
}

allow if {
	operation in read_operations
	resource.table.catalogName == dataplatform.catalog
	resource.table.schemaName == "information_schema"
}

# their own queries; the admin groups everybody's
allow if {
	operation in {"ViewQueryOwnedBy", "FilterViewQueryOwnedBy", "KillQueryOwnedBy"}
	resource.user.user == user
}

allow if {
	operation in {"ViewQueryOwnedBy", "FilterViewQueryOwnedBy", "KillQueryOwnedBy", "ReadSystemInformation"}
	is_admin
}

allow if {
	operation == "ExecuteProcedure"
	resource.function == {"catalogName": "system", "schemaName": "runtime", "functionName": "kill_query"}
}

# ---------------------------------------------------------------- the tables ----
allow if {
	operation in schema_operations
	resource.schema.catalogName == dataplatform.catalog
	sees_schema(resource.schema.schemaName)
}

allow if {
	operation in read_operations
	resource.table.catalogName == dataplatform.catalog
	may_read(resource.table.schemaName, resource.table.tableName)
}

allow if {
	operation in write_operations
	resource.table.catalogName == dataplatform.catalog
	may_write(resource.table.schemaName, resource.table.tableName)
}

# ---------------------------------------------------------------- the provisioner ----
allow if {
	is_provisioner
	catalog_of(resource) == dataplatform.catalog
}

# ---------------------------------------------------------------- Stackable ----
# A worker that is told to stop finishes its queries first: the operator's
# hook asks it to, as this user.
# https://docs.stackable.tech/home/stable/trino/usage-guide/operations/graceful-shutdown/
allow if {
	user == "graceful-shutdown-user"
	operation == "WriteSystemInformation"
}

# ---------------------------------------------------------------- lists ----
# Filtering a list (catalogs, schemas, tables, functions, queries): the
# positions of the items the user may see, each decided as above.
batch contains i if {
	operation != "FilterColumns"
	some i, item in input.action.filterResources
	allow with input.action.resource as item
}

# Columns come as one table with all its columns: the table decides for all.
batch contains i if {
	operation == "FilterColumns"
	count(input.action.filterResources) == 1
	table := input.action.filterResources[0].table
	allow with input.action.resource as {"table": table}
	some i, _ in table.columns
}

# No row is hidden inside a table a user may read.
rowFilters := []
