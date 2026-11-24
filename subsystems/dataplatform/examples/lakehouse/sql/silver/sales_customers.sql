-- sales.silver.customers from sales.bronze.customers, through Trino.
-- A dimension: MERGE keeps one row per customer, rewrites only the rows
-- whose values changed and adds the new customers. Run as the pipeline
-- (WRITE on silver, READ on bronze); {domain.layer.name} is the table at the
-- version subsystems/dataplatform/metacatalog/tables/tables.yaml defines.
MERGE INTO {sales.silver.customers} t
USING (
  SELECT customer_id, full_name, email, email_domain, country, segment, signup_date
  FROM (
    SELECT customer_id,
           trim(full_name) AS full_name,
           lower(trim(email)) AS email,
           split_part(lower(trim(email)), '@', 2) AS email_domain,
           upper(trim(country)) AS country,
           lower(segment) AS segment,
           signup_date,
           row_number() OVER (PARTITION BY customer_id ORDER BY ingested_at DESC) AS latest
    FROM {sales.bronze.customers}
    WHERE customer_id IS NOT NULL
  )
  WHERE latest = 1
) s
ON t.customer_id = s.customer_id
WHEN MATCHED AND (t.full_name IS DISTINCT FROM s.full_name OR t.email IS DISTINCT FROM s.email
                  OR t.country IS DISTINCT FROM s.country OR t.segment IS DISTINCT FROM s.segment
                  OR t.signup_date IS DISTINCT FROM s.signup_date) THEN
  UPDATE SET full_name = s.full_name, email = s.email, email_domain = s.email_domain, country = s.country,
             segment = s.segment, signup_date = s.signup_date
WHEN NOT MATCHED THEN
  INSERT (customer_id, full_name, email, email_domain, country, segment, signup_date)
  VALUES (s.customer_id, s.full_name, s.email, s.email_domain, s.country, s.segment, s.signup_date);
