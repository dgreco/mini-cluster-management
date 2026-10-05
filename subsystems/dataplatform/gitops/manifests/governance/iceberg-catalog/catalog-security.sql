-- Roles of the Iceberg JDBC catalog. Runs as a superuser on the application
-- database: once at bootstrap (postInitApplicationSQLRefs) and again at every
-- `make dataplatform-iam`, so everything here is idempotent.
--
-- Trino connects as the owner (iceberg); its own access control lets only
-- Meta Catalog's provisioner create schemas and tables. The Spark jobs connect
-- as spark_writer, which cannot. The files are Ceph's to refuse, with the
-- token's role (gitops/iam).

-- iam-sync's reader of earlier installs (it derived Trino's schema owners
-- from the locations; only Meta Catalog's provisioner has DDL now): gone
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'iam_sync') THEN
    REVOKE ALL ON iceberg_tables, iceberg_namespace_properties FROM iam_sync;
    REVOKE ALL ON SCHEMA public FROM iam_sync;
    DROP ROLE iam_sync;
  END IF;
END $$;

-- The Spark jobs' login (Secret compute/spark-catalog-logins). Meta Catalog
-- alone creates schemas and tables (Trino gives DDL to its provisioner only),
-- so Spark may read the catalog and commit to the tables that exist (the JDBC
-- catalog's commit updates only the row's metadata locations) but not add,
-- drop or rename a table or a schema. make dataplatform-iam sets its password.
DO $$
BEGIN
  CREATE ROLE spark_writer NOLOGIN;
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;
REVOKE ALL ON iceberg_tables, iceberg_namespace_properties FROM spark_writer;
GRANT USAGE ON SCHEMA public TO spark_writer;
GRANT SELECT ON iceberg_tables, iceberg_namespace_properties TO spark_writer;
GRANT UPDATE (metadata_location, previous_metadata_location) ON iceberg_tables TO spark_writer;

-- earlier installs: one role per Keycloak group and row-level security by
-- bucket (the group buckets are gone); every leftover goes
ALTER TABLE iceberg_tables DISABLE ROW LEVEL SECURITY;
ALTER TABLE iceberg_namespace_properties DISABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS group_bucket ON iceberg_tables;
DROP POLICY IF EXISTS group_bucket ON iceberg_namespace_properties;
DROP POLICY IF EXISTS reader ON iceberg_tables;
DROP POLICY IF EXISTS reader ON iceberg_namespace_properties;
DROP FUNCTION IF EXISTS lakehouse_namespace_bucket(text, text);
DROP FUNCTION IF EXISTS lakehouse_my_bucket();
DROP FUNCTION IF EXISTS lakehouse_bucket(text);
DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT rolname FROM pg_roles WHERE rolname LIKE 'grp-%' OR rolname IN ('lakehouse_groups', 'iceberg_jobs') LOOP
    EXECUTE format('REVOKE ALL ON iceberg_tables, iceberg_namespace_properties FROM %I', r.rolname);
    EXECUTE format('REVOKE ALL ON SCHEMA public FROM %I', r.rolname);
    EXECUTE format('DROP ROLE %I', r.rolname);
  END LOOP;
END $$;
