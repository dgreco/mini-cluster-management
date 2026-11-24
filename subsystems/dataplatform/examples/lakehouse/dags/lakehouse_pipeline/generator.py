"""The synthetic data of the lakehouse example's bronze tables: a web shop's
customers and orders (domain sales), and the carriers and parcels that
deliver them (domain logistics).

The data is cut into parts of at most rows_per_file rows, one parquet file
each (<folder>/part-<n>.parquet), so that the parts are written side by side
by Airflow's task pods and a run of hundreds of millions of rows takes as
many pods as the cluster gives it. Everything is computed with numpy and
pyarrow, column at a time; a part of two million orders takes a few seconds
and some hundred MB.

A part is a function of (seed, table, part) alone: the same seed gives the
same rows, and a part of shipments is derived from the part of orders with
the same number (the same random draws), so that every shipment carries an
order that exists, placed before it left. Sizes, for N orders:

  orders     N rows, plus about 1% repeated (the earlier 'placed' event of an
             order, loaded a day before the real one), about 0.2% without a
             customer and 0.3% with a total of zero or less: what silver
             leaves out or folds together
  customers  max(1000, N / 20)
  shipments  one per order that was shipped, delivered or returned (about
             74% of N)
  carriers   12

so N = 200 million writes about 360 million rows.
"""
import io

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

TABLE_CODES = {"customers": 1, "orders": 2, "shipments": 3, "carriers": 4}
BRONZE = {"customers": "sales.bronze.customers", "orders": "sales.bronze.orders",
          "shipments": "logistics.bronze.shipments", "carriers": "logistics.bronze.carriers"}

FIRST = ["Anna", "Ben", "Chiara", "Dario", "Elena", "Felix", "Greta", "Hugo", "Ines", "Jonas", "Klara", "Luca",
         "Marta", "Nils", "Olga", "Paul", "Rosa", "Sven", "Tina", "Umberto", "Vera", "Wim", "Xenia", "Yann",
         "Zoe", "Aldo", "Bea", "Carlo", "Dora", "Emil", "Flora", "Gino", "Hanna", "Ivo", "Jana", "Karl", "Lea",
         "Mario", "Nora", "Otto"]
LAST = ["Rossi", "Muller", "Martin", "Garcia", "Smith", "Jansen", "Novak", "Schmidt", "Bianchi", "Dubois",
        "Lopez", "Brown", "Visser", "Kowalski", "Fischer", "Ricci", "Bernard", "Fernandez", "Taylor", "Bakker",
        "Nowak", "Weber", "Romano", "Thomas", "Sanchez", "Wilson", "Smit", "Wojcik", "Meyer", "Colombo",
        "Petit", "Perez", "Evans", "Meijer", "Lewandowski", "Wagner", "Conti", "Robert", "Gomez", "Walker"]
# reserved for examples (RFC 2606): no real address is ever made up
EMAIL_DOMAINS = ["example.com", "example.net", "post.example", "mail.example", "shop.example"]
COUNTRIES = ["DE", "FR", "IT", "ES", "NL", "GB", "US", "CH", "AT", "BE", "SE", "PL"]
COUNTRY_P = [0.2, 0.14, 0.13, 0.1, 0.07, 0.1, 0.08, 0.04, 0.04, 0.03, 0.03, 0.04]
SEGMENTS = ["consumer", "smb", "enterprise"]
SEGMENT_P = [0.8, 0.17, 0.03]
STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled", "returned"]
STATUS_P = [0.05, 0.15, 0.2, 0.5, 0.06, 0.04]
CURRENCIES = ["EUR", "USD", "GBP", "CHF"]
CURRENCY_P = [0.6, 0.2, 0.12, 0.08]
CHANNELS = ["web", "app", "store", "marketplace"]
CHANNEL_P = [0.5, 0.3, 0.12, 0.08]
WAREHOUSES = ["DE-LEJ", "NL-AMS", "IT-MXP", "ES-MAD", "PL-POZ"]
# code, name, service level, home market, promised hours
CARRIERS = [("PKX", "Parcelix", "standard", "DE", 48), ("PKXE", "Parcelix Express", "express", "DE", 24),
            ("ROAD", "Roadrunner", "economy", "FR", 96), ("ROADS", "Roadrunner Plus", "standard", "FR", 48),
            ("SWIFT", "Swiftbox", "express", "NL", 24), ("SWIFTE", "Swiftbox Eco", "economy", "NL", 120),
            ("ALPS", "Alpine Post", "standard", "CH", 48), ("BOOT", "Bootleg Freight", "economy", "IT", 96),
            ("BOOTX", "Bootleg Prime", "express", "IT", 24), ("IBEX", "Iberia Express", "express", "ES", 24),
            ("IBER", "Iberia Cargo", "standard", "ES", 72), ("VIST", "Vistula Lines", "economy", "PL", 120)]
CARRIER_P = [0.18, 0.08, 0.1, 0.08, 0.1, 0.05, 0.06, 0.08, 0.05, 0.08, 0.08, 0.06]
TRANSIT_HOURS = {"express": 20.0, "standard": 40.0, "economy": 80.0}     # mean hours on the road

ORDERS_START_MS = int(np.datetime64("2024-01-01T00:00:00", "ms").astype(np.int64))
ORDERS_SPAN_MS = 2 * 365 * 86_400_000
SIGNUP_START_DAY = int(np.datetime64("2018-01-01", "D").astype(np.int64))
HOUR_MS = 3_600_000


def sizes(orders: int) -> dict:
    """Rows of each table for N orders (the shipments' are about 74% of the
    orders', decided while they are made)."""
    return {"orders": orders, "customers": max(1000, orders // 20), "carriers": len(CARRIERS)}


def plan(orders: int, rows_per_file: int) -> list:
    """Every part to write: {table, part, start, count}. Shipments go with
    the orders' parts."""
    if orders < 1 or rows_per_file < 1:
        raise ValueError("orders and rows_per_file must be positive")
    n = sizes(orders)
    out = [{"table": "carriers", "part": 0, "start": 0, "count": n["carriers"]}]
    for table in ("customers", "orders", "shipments"):
        total = n["orders" if table == "shipments" else table]
        for part, start in enumerate(range(0, total, rows_per_file)):
            out.append({"table": table, "part": part, "start": start, "count": min(rows_per_file, total - start)})
    return out


def rng(seed: int, table: str, part: int) -> np.random.Generator:
    return np.random.default_rng([int(seed), TABLE_CODES[table], int(part)])


def pick(r: np.random.Generator, values: list, p: list, n: int) -> pa.Array:
    return pc.take(pa.array(values), pa.array(r.choice(len(values), size=n, p=p).astype(np.int32)))


def timestamps(ms: np.ndarray, mask=None) -> pa.Array:
    return pa.array(ms.astype(np.int64), type=pa.timestamp("ms"), mask=mask)


def customers(seed: int, part: int, start: int, count: int, ingested_ms: int) -> pa.Table:
    r = rng(seed, "customers", part)
    ids = np.arange(start + 1, start + count + 1, dtype=np.int64)
    first = pc.take(pa.array(FIRST), pa.array(r.integers(0, len(FIRST), count)))
    last = pc.take(pa.array(LAST), pa.array(r.integers(0, len(LAST), count)))
    local = pc.binary_join_element_wise(pc.utf8_lower(first), ".", pc.utf8_lower(last), pc.cast(pa.array(ids), pa.string()), "")
    email = pc.binary_join_element_wise(local, pick(r, EMAIL_DOMAINS, [0.2] * 5, count), "@")
    email = pc.if_else(pa.array(r.random(count) < 0.3), pc.utf8_capitalize(email), email)
    country = pick(r, COUNTRIES, COUNTRY_P, count)
    country = pc.if_else(pa.array(r.random(count) < 0.2), pc.utf8_lower(country), country)
    return pa.table({
        "customer_id": pa.array(ids),
        "full_name": pc.binary_join_element_wise(first, last, " "),
        "email": email,
        "country": country,
        "segment": pick(r, SEGMENTS, SEGMENT_P, count),
        "signup_date": pa.array((SIGNUP_START_DAY + r.integers(0, 2900, count)).astype(np.int32), type=pa.date32()),
        "ingested_at": timestamps(np.full(count, ingested_ms)),
    })


def _orders_core(seed: int, part: int, start: int, count: int, customers_total: int) -> dict:
    """The orders of a part as numpy columns, always drawn in this order
    (shipments draw them again)."""
    r = rng(seed, "orders", part)
    core = {
        "order_id": np.arange(start + 1, start + count + 1, dtype=np.int64),
        "customer_id": r.integers(1, customers_total + 1, count, dtype=np.int64),
        "no_customer": r.random(count) < 0.002,
        "order_ts": ORDERS_START_MS + r.integers(0, ORDERS_SPAN_MS, count, dtype=np.int64),
        "status": r.choice(len(STATUSES), size=count, p=STATUS_P).astype(np.int32),
        "currency": r.choice(len(CURRENCIES), size=count, p=CURRENCY_P).astype(np.int32),
        "amount": np.round(r.lognormal(3.8, 0.8, count), 2),
        "broken": r.random(count) < 0.003,
        "items": np.clip(1 + r.poisson(1.5, count), 1, 20).astype(np.int32),
        "channel": r.choice(len(CHANNELS), size=count, p=CHANNEL_P).astype(np.int32),
        "repeated": r.random(count) < 0.01,
    }
    core["amount"] = np.where(core["broken"], -np.round(core["amount"] * r.random(count), 2), core["amount"])
    return core


def orders(seed: int, part: int, start: int, count: int, customers_total: int, ingested_ms: int) -> pa.Table:
    c = _orders_core(seed, part, start, count, customers_total)

    def table(rows, status, ingested):
        return pa.table({
            "order_id": pa.array(c["order_id"][rows]),
            "customer_id": pa.array(c["customer_id"][rows], mask=c["no_customer"][rows]),
            "order_ts": timestamps(c["order_ts"][rows]),
            "status": pc.take(pa.array(STATUSES), pa.array(status)),
            "currency": pc.take(pa.array(CURRENCIES), pa.array(c["currency"][rows])),
            "amount": pa.array(c["amount"][rows]),
            "items": pa.array(c["items"][rows]),
            "channel": pc.take(pa.array(CHANNELS), pa.array(c["channel"][rows])),
            "ingested_at": timestamps(np.full(int(rows.sum()) if rows.dtype == bool else len(rows), ingested)),
        })
    every = np.ones(count, dtype=bool)
    events = [table(every, c["status"], ingested_ms)]
    rep = c["repeated"]
    if rep.any():
        # the order's first event, loaded a day earlier: silver keeps the later one
        events.append(table(rep, np.zeros(int(rep.sum()), dtype=np.int32), ingested_ms - 24 * HOUR_MS))
    return pa.concat_tables(events)


def shipments(seed: int, part: int, start: int, count: int, customers_total: int, ingested_ms: int) -> pa.Table:
    c = _orders_core(seed, part, start, count, customers_total)
    shipped = np.isin(c["status"], [STATUSES.index("shipped"), STATUSES.index("delivered"), STATUSES.index("returned")])
    m = int(shipped.sum())
    r = rng(seed, "shipments", part)
    carrier = r.choice(len(CARRIERS), size=m, p=CARRIER_P).astype(np.int32)
    mean_hours = np.array([TRANSIT_HOURS[k[2]] for k in CARRIERS])[carrier]
    shipped_ts = c["order_ts"][shipped] + r.integers(2 * HOUR_MS, 72 * HOUR_MS, m, dtype=np.int64)
    transit_ms = (mean_hours * r.gamma(6.0, 1 / 6.0, m) * HOUR_MS).astype(np.int64)
    order_status = c["status"][shipped]
    lost = r.random(m) < 0.003
    # in_transit, delivered, returned, lost
    status = np.where(order_status == STATUSES.index("shipped"), 0,
                      np.where(order_status == STATUSES.index("delivered"), 1, 2)).astype(np.int32)
    status = np.where(lost & (status != 2), 3, status).astype(np.int32)
    not_delivered = (status == 0) | (status == 3)
    order_id = c["order_id"][shipped]
    return pa.table({
        "shipment_id": pa.array(order_id * 10 + 1),
        "order_id": pa.array(order_id),
        "carrier_code": pc.take(pa.array([k[0] for k in CARRIERS]), pa.array(carrier)),
        "warehouse": pick(r, WAREHOUSES, [0.3, 0.25, 0.2, 0.15, 0.1], m),
        "shipped_ts": timestamps(shipped_ts),
        "delivered_ts": timestamps(shipped_ts + transit_ms, mask=not_delivered),
        "status": pc.take(pa.array(["in_transit", "delivered", "returned", "lost"]), pa.array(status)),
        "weight_kg": pa.array(np.round(r.gamma(2.0, 1.5, m) + 0.1, 2)),
        "ingested_at": timestamps(np.full(m, ingested_ms)),
    })


def carriers(ingested_ms: int) -> pa.Table:
    return pa.table({
        "carrier_code": pa.array([k[0] for k in CARRIERS]),
        "carrier_name": pa.array([k[1] for k in CARRIERS]),
        "service_level": pa.array([k[2] for k in CARRIERS]),
        "home_country": pa.array([k[3] for k in CARRIERS]),
        "promised_hours": pa.array([k[4] for k in CARRIERS], type=pa.int32()),
        "ingested_at": timestamps(np.full(len(CARRIERS), ingested_ms)),
    })


def make(spec: dict, seed: int, orders_total: int, ingested_ms: int) -> pa.Table:
    """The rows of one part of plan()."""
    customers_total = sizes(orders_total)["customers"]
    t, part, start, count = spec["table"], spec["part"], spec["start"], spec["count"]
    if t == "customers":
        return customers(seed, part, start, count, ingested_ms)
    if t == "orders":
        return orders(seed, part, start, count, customers_total, ingested_ms)
    if t == "shipments":
        return shipments(seed, part, start, count, customers_total, ingested_ms)
    if t == "carriers":
        return carriers(ingested_ms)
    raise ValueError(f"no bronze table {t!r}")


def file_name(part: int) -> str:
    return f"part-{part:05d}.parquet"


def to_parquet(table: pa.Table) -> bytes:
    """The table as one parquet file, in memory (the task pods' file system
    may be read-only). Timestamps in milliseconds without a time zone, as
    the bronze tables' timestamp(3) columns are."""
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy", coerce_timestamps="ms", row_group_size=512 * 1024)
    return buf.getvalue().to_pybytes()


def check_columns(table: pa.Table, columns: tuple) -> None:
    """The generated columns must be the table's in tables.yaml (names,
    in that order): a parquet table is read by column name."""
    want = [n for n, _ in columns]
    if table.column_names != want:
        raise ValueError(f"generated columns {table.column_names}, the table has {want}")


def write(s3, bucket: str, key: str, table: pa.Table) -> int:
    from boto3.s3.transfer import TransferConfig
    data = to_parquet(table)
    s3.upload_fileobj(io.BytesIO(data), bucket, key,
                      Config=TransferConfig(multipart_threshold=64 * 1024 * 1024, multipart_chunksize=32 * 1024 * 1024))
    return len(data)
