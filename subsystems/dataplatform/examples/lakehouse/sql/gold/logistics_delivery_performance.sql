-- logistics.gold.delivery_performance from logistics.silver.shipments and,
-- across domains, sales.silver.orders and sales.silver.customers (where the
-- parcels go), through Trino. Rebuilt whole. The join of every parcel to its
-- order is the heaviest statement of the example: at hundreds of millions of
-- rows it needs Trino's memory, or Spark (the DAG lakehouse_spark).
DELETE FROM {logistics.gold.delivery_performance};

INSERT INTO {logistics.gold.delivery_performance}
  (shipped_date, carrier_name, service_level, destination_country, shipments, delivered, on_time,
   on_time_rate, avg_transit_hours, p95_transit_hours)
SELECT s.shipped_date,
       coalesce(s.carrier_name, 'unknown'),
       coalesce(s.service_level, 'unknown'),
       coalesce(c.country, 'unknown'),
       count(*),
       count(s.delivered_ts),
       count_if(s.on_time),
       CAST(count_if(s.on_time) AS double) / nullif(count(s.delivered_ts), 0),
       avg(s.transit_hours),
       approx_percentile(s.transit_hours, 0.95)
FROM {logistics.silver.shipments} s
LEFT JOIN {sales.silver.orders} o ON s.order_id = o.order_id
LEFT JOIN {sales.silver.customers} c ON o.customer_id = c.customer_id
GROUP BY 1, 2, 3, 4;
