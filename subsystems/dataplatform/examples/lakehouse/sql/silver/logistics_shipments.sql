-- logistics.silver.shipments from logistics.bronze.shipments and
-- logistics.bronze.carriers, through Trino. Rebuilt whole: one row per
-- parcel in its latest event, with its carrier's name, service level and
-- promised hours, its transit time and whether it came within them.
DELETE FROM {logistics.silver.shipments};

INSERT INTO {logistics.silver.shipments}
  (shipment_id, order_id, carrier_code, carrier_name, service_level, warehouse, shipped_date, shipped_ts,
   delivered_ts, transit_hours, promised_hours, on_time, status, weight_kg)
SELECT s.shipment_id,
       s.order_id,
       s.carrier_code,
       c.carrier_name,
       c.service_level,
       s.warehouse,
       CAST(s.shipped_ts AS date),
       CAST(s.shipped_ts AS timestamp(6)),
       CAST(s.delivered_ts AS timestamp(6)),
       CASE WHEN s.delivered_ts IS NOT NULL THEN date_diff('second', s.shipped_ts, s.delivered_ts) / 3600.0 END,
       c.promised_hours,
       coalesce(s.delivered_ts IS NOT NULL
                AND date_diff('second', s.shipped_ts, s.delivered_ts) <= c.promised_hours * 3600, false),
       s.status,
       s.weight_kg
FROM (
  SELECT *, row_number() OVER (PARTITION BY shipment_id ORDER BY ingested_at DESC) AS latest
  FROM {logistics.bronze.shipments}
) s
LEFT JOIN (
  SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY carrier_code ORDER BY ingested_at DESC) AS latest
    FROM {logistics.bronze.carriers}
  ) WHERE latest = 1
) c ON s.carrier_code = c.carrier_code
WHERE s.latest = 1;
