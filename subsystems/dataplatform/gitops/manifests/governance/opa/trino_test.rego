# Unit tests of trino.rego (make dataplatform-policy-test). The requests are
# shaped as Trino 481 sends them; the grants as iam-sync writes them. Not
# part of the ConfigMap the Agent loads (kustomization.yaml names trino.rego
# alone).
package trino_test

import data.trino

grants := {
	"catalog": "lakehouse",
	"provisioner": "service-account-metacatalog-provisioner",
	"admin_groups": ["admins"],
	"tables": {
		"retail_orders_bronze": {"orders_raw": {"rw": {"users": ["carol"], "groups": ["engineers"]}}},
		"retail_analytics_gold": {"daily_revenue": {
			"rw": {"users": [], "groups": ["engineers"]},
			"ro": {"users": [], "groups": ["analysts"]},
		}},
	},
}

who := {
	"provisioner": {"user": "service-account-metacatalog-provisioner", "groups": []},
	"engineer": {"user": "eve", "groups": ["engineers"]},
	"analyst": {"user": "ann", "groups": ["analysts"]},
	"admin": {"user": "adam", "groups": ["admins"]},
	"carol": {"user": "carol", "groups": []},
	"nobody": {"user": "nick", "groups": []},
}

request(identity, operation, resource) := {
	"context": {"identity": who[identity], "softwareStack": {"trinoVersion": "481"}},
	"action": {"operation": operation, "resource": resource},
}

table(schema, name) := {"table": {"catalogName": "lakehouse", "schemaName": schema, "tableName": name, "columns": ["x"]}}

schema(name) := {"schema": {"catalogName": "lakehouse", "schemaName": name}}

allowed(identity, operation, resource) if {
	trino.allow with input as request(identity, operation, resource) with data.dataplatform as grants
}

# ---------------------------------------------------------------- everybody ----
test_everybody_runs_queries_and_sees_the_catalogs if {
	allowed("nobody", "ExecuteQuery", {})
	allowed("nobody", "AccessCatalog", {"catalog": {"name": "lakehouse"}})
	allowed("nobody", "AccessCatalog", {"catalog": {"name": "system"}})
	allowed("nobody", "ShowSchemas", {"catalog": {"name": "lakehouse"}})
	allowed("nobody", "SetSystemSessionProperty", {"systemSessionProperty": {"name": "query_max_run_time"}})
	allowed("nobody", "ExecuteFunction", {"function": {"catalogName": "system", "schemaName": "builtin", "functionName": "sequence"}})
	allowed("nobody", "SelectFromColumns", {"table": {"catalogName": "system", "schemaName": "jdbc", "tableName": "tables"}})
	allowed("nobody", "SelectFromColumns", table("information_schema", "tables"))
	not allowed("nobody", "AccessCatalog", {"catalog": {"name": "other"}})
}

test_a_user_without_a_grant_reaches_no_table if {
	not allowed("nobody", "SelectFromColumns", table("retail_orders_bronze", "orders_raw"))
	not allowed("nobody", "ShowTables", schema("retail_orders_bronze"))
	not allowed("nobody", "FilterTables", table("retail_orders_bronze", "orders_raw"))
	not allowed("nobody", "FilterSchemas", schema("retail_orders_bronze"))
	allowed("nobody", "FilterSchemas", schema("information_schema"))
}

# ---------------------------------------------------------------- the grants ----
test_a_reader_reads_and_does_not_write if {
	allowed("analyst", "SelectFromColumns", table("retail_analytics_gold", "daily_revenue"))
	allowed("analyst", "ShowColumns", table("retail_analytics_gold", "daily_revenue"))
	allowed("analyst", "ShowTables", schema("retail_analytics_gold"))
	allowed("analyst", "FilterSchemas", schema("retail_analytics_gold"))
	every operation in {"InsertIntoTable", "DeleteFromTable", "UpdateTableColumns", "TruncateTable"} {
		not allowed("analyst", operation, table("retail_analytics_gold", "daily_revenue"))
	}
	not allowed("analyst", "SelectFromColumns", table("retail_orders_bronze", "orders_raw"))
	not allowed("analyst", "FilterSchemas", schema("retail_orders_bronze"))
}

test_a_writer_writes_and_reads if {
	every operation in {"SelectFromColumns", "InsertIntoTable", "DeleteFromTable", "UpdateTableColumns", "TruncateTable", "ExecuteTableProcedure"} {
		allowed("engineer", operation, table("retail_orders_bronze", "orders_raw"))
	}
}

test_a_user_named_in_a_grant_has_it_without_a_group if {
	allowed("carol", "InsertIntoTable", table("retail_orders_bronze", "orders_raw"))
	not allowed("carol", "SelectFromColumns", table("retail_analytics_gold", "daily_revenue"))
}

test_an_iceberg_metadata_table_follows_its_table if {
	allowed("analyst", "SelectFromColumns", table("retail_analytics_gold", "daily_revenue$snapshots"))
	not allowed("nobody", "SelectFromColumns", table("retail_analytics_gold", "daily_revenue$snapshots"))
}

test_the_admin_groups_read_and_write_every_table if {
	allowed("admin", "SelectFromColumns", table("retail_orders_bronze", "orders_raw"))
	allowed("admin", "InsertIntoTable", table("retail_analytics_gold", "daily_revenue"))
	allowed("admin", "FilterSchemas", schema("any_schema_at_all"))
	allowed("admin", "ViewQueryOwnedBy", {"user": {"user": "eve"}})
}

# ---------------------------------------------------------------- DDL ----
ddl := {
	"CreateTable", "DropTable", "RenameTable", "AddColumn", "DropColumn", "RenameColumn", "AlterColumn",
	"SetTableComment", "SetColumnComment", "SetTableProperties", "CreateView", "DropView",
	"CreateMaterializedView", "SetTableAuthorization",
}

test_only_the_provisioner_creates_alters_or_drops if {
	every operation in ddl {
		allowed("provisioner", operation, table("retail_orders_bronze", "orders_raw"))
		not allowed("engineer", operation, table("retail_orders_bronze", "orders_raw"))
		not allowed("admin", operation, table("retail_orders_bronze", "orders_raw"))
	}
	every operation in {"CreateSchema", "DropSchema", "RenameSchema", "SetSchemaAuthorization"} {
		allowed("provisioner", operation, schema("retail_orders_bronze"))
		not allowed("engineer", operation, schema("retail_orders_bronze"))
		not allowed("admin", operation, schema("retail_orders_bronze"))
	}
}

test_the_provisioner_stays_in_the_platforms_catalog if {
	not allowed("provisioner", "CreateSchema", {"schema": {"catalogName": "system", "schemaName": "x"}})
	not allowed("provisioner", "CreateTable", {"table": {"catalogName": "other", "schemaName": "x", "tableName": "y"}})
}

test_procedures_are_the_provisioners_but_killing_a_query if {
	allowed("provisioner", "ExecuteProcedure", {"function": {"catalogName": "lakehouse", "schemaName": "system", "functionName": "register_table"}})
	not allowed("engineer", "ExecuteProcedure", {"function": {"catalogName": "lakehouse", "schemaName": "system", "functionName": "register_table"}})
	allowed("engineer", "ExecuteProcedure", {"function": {"catalogName": "system", "schemaName": "runtime", "functionName": "kill_query"}})
}

# ---------------------------------------------------------------- queries, the system ----
test_a_user_sees_and_kills_their_own_queries_only if {
	allowed("engineer", "ViewQueryOwnedBy", {"user": {"user": "eve"}})
	allowed("engineer", "KillQueryOwnedBy", {"user": {"user": "eve"}})
	not allowed("engineer", "ViewQueryOwnedBy", {"user": {"user": "ann"}})
	not allowed("engineer", "KillQueryOwnedBy", {"user": {"user": "ann"}})
}

test_nobody_impersonates_or_changes_the_system if {
	every identity in {"provisioner", "engineer", "admin"} {
		not allowed(identity, "ImpersonateUser", {"user": {"user": "ann"}})
		not allowed(identity, "WriteSystemInformation", {})
	}
	not allowed("engineer", "ReadSystemInformation", {})
	allowed("admin", "ReadSystemInformation", {})
}

test_a_worker_may_be_told_to_finish_its_queries if {
	trino.allow with input as {
		"context": {"identity": {"user": "graceful-shutdown-user", "groups": []}, "softwareStack": {"trinoVersion": "481"}},
		"action": {"operation": "WriteSystemInformation"},
	}
		with data.dataplatform as grants
}

# ---------------------------------------------------------------- before the grants are there ----
test_without_the_grants_nobody_reaches_a_table_or_makes_one if {
	not trino.allow with input as request("engineer", "SelectFromColumns", table("retail_orders_bronze", "orders_raw"))
	not trino.allow with input as request("provisioner", "CreateTable", table("retail_orders_bronze", "orders_raw"))
	not trino.allow with input as request("admin", "SelectFromColumns", table("retail_orders_bronze", "orders_raw"))
	trino.allow with input as request("engineer", "ExecuteQuery", {})
}

test_an_empty_provisioner_is_nobody if {
	no_provisioner := object.union(grants, {"provisioner": ""})
	not trino.allow with input as request("provisioner", "CreateTable", table("retail_orders_bronze", "orders_raw"))
		with data.dataplatform as no_provisioner
}

# ---------------------------------------------------------------- lists ----
test_a_list_keeps_what_the_user_may_see if {
	seen := trino.batch with input as {
		"context": {"identity": who.analyst, "softwareStack": {"trinoVersion": "481"}},
		"action": {"operation": "FilterTables", "filterResources": [
			table("retail_orders_bronze", "orders_raw"),
			table("retail_analytics_gold", "daily_revenue"),
			table("information_schema", "tables"),
		]},
	}
		with data.dataplatform as grants
	seen == {1, 2}
}

test_columns_follow_their_table if {
	columns := {"table": {"catalogName": "lakehouse", "schemaName": "retail_analytics_gold", "tableName": "daily_revenue", "columns": ["day", "revenue", "orders"]}}
	seen := trino.batch with input as {
		"context": {"identity": who.analyst, "softwareStack": {"trinoVersion": "481"}},
		"action": {"operation": "FilterColumns", "filterResources": [columns]},
	}
		with data.dataplatform as grants
	seen == {0, 1, 2}
	hidden := trino.batch with input as {
		"context": {"identity": who.nobody, "softwareStack": {"trinoVersion": "481"}},
		"action": {"operation": "FilterColumns", "filterResources": [columns]},
	}
		with data.dataplatform as grants
	hidden == set()
}

test_no_row_is_filtered if {
	trino.rowFilters == [] with input as request("analyst", "GetRowFilters", table("retail_analytics_gold", "daily_revenue"))
		with data.dataplatform as grants
}
