-- sales.gold.daily_sales from sales.silver.orders and sales.silver.customers,
-- through Trino: revenue per day, country and channel of the orders that were
-- paid (placed, cancelled and returned ones left out). Rebuilt whole.
DELETE FROM {sales.gold.daily_sales};

INSERT INTO {sales.gold.daily_sales}
  (order_date, country, channel, orders, customers, items, revenue_eur, avg_order_eur)
SELECT o.order_date,
       coalesce(c.country, 'unknown'),
       o.channel,
       count(*),
       count(DISTINCT o.customer_id),
       sum(o.items),
       CAST(sum(o.amount_eur) AS decimal(18, 2)),
       CAST(avg(o.amount_eur) AS decimal(12, 2))
FROM {sales.silver.orders} o
LEFT JOIN {sales.silver.customers} c ON o.customer_id = c.customer_id
WHERE o.status IN ('paid', 'shipped', 'delivered')
GROUP BY 1, 2, 3;
