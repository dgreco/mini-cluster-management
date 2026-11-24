-- sales.silver.orders from sales.bronze.orders, through Trino.
-- A fact table, rebuilt whole: one row per order in its latest status (the
-- bronze events repeat an order when its status changes), broken events
-- (no customer, a total of zero or less, an unknown currency) left out,
-- totals in EUR at the example's fixed rates. DELETE without a condition is
-- a metadata operation in Iceberg: the old rows go in one snapshot, the new
-- ones come in the next (a reader in between sees an empty table).
DELETE FROM {sales.silver.orders};

INSERT INTO {sales.silver.orders}
  (order_id, customer_id, order_date, order_ts, status, currency, amount, amount_eur, items, channel)
SELECT order_id,
       customer_id,
       CAST(order_ts AS date),
       CAST(order_ts AS timestamp(6)),
       status,
       currency,
       CAST(round(amount, 2) AS decimal(12, 2)),
       CAST(round(amount * CASE currency WHEN 'EUR' THEN 1.0 WHEN 'USD' THEN 0.92
                                         WHEN 'GBP' THEN 1.17 WHEN 'CHF' THEN 1.04 END, 2) AS decimal(12, 2)),
       items,
       channel
FROM (
  SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) AS latest
  FROM {sales.bronze.orders}
  WHERE customer_id IS NOT NULL AND amount > 0 AND currency IN ('EUR', 'USD', 'GBP', 'CHF')
)
WHERE latest = 1;
