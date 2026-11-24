-- sales.gold.customer_value from sales.silver.orders and
-- sales.silver.customers, through Trino: what each customer brought in over
-- their life (paid orders only), with a value band. Rebuilt whole.
DELETE FROM {sales.gold.customer_value};

INSERT INTO {sales.gold.customer_value}
  (customer_id, country, segment, first_order_date, last_order_date, orders, revenue_eur, value_band)
WITH v AS (
  SELECT customer_id,
         min(order_date) AS first_order_date,
         max(order_date) AS last_order_date,
         count(*) AS orders,
         sum(amount_eur) AS revenue_eur
  FROM {sales.silver.orders}
  WHERE status IN ('paid', 'shipped', 'delivered')
  GROUP BY customer_id
)
SELECT v.customer_id,
       c.country,
       c.segment,
       v.first_order_date,
       v.last_order_date,
       v.orders,
       CAST(v.revenue_eur AS decimal(18, 2)),
       CASE WHEN v.revenue_eur >= 2500 THEN 'top'
            WHEN v.revenue_eur >= 1500 THEN 'high'
            WHEN v.revenue_eur >= 700 THEN 'mid'
            ELSE 'low' END
FROM v
LEFT JOIN {sales.silver.customers} c ON v.customer_id = c.customer_id;
