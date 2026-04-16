#!/usr/bin/env python3
"""
Simulates a live OLTP workload by continuously making random changes
(INSERT, UPDATE, DELETE) to the PostgreSQL source tables.

Debezium captures these changes via the WAL and streams them to Kafka.

Usage:
    docker exec jupyter python /home/jovyan/project/simulate.py
    docker exec jupyter python /home/jovyan/project/simulate.py --rate 2.0
    docker exec jupyter python /home/jovyan/project/simulate.py --rate 0.5 --tables customers

Press Ctrl-C to stop.
"""

import argparse
import os
import random
import sys
import time

def _ensure(pkg, import_name=None):
    import importlib.util, subprocess
    if importlib.util.find_spec(import_name or pkg) is None:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

_ensure("psycopg2-binary", "psycopg2")

import psycopg2


# ── Data pools for generating realistic mutations ─────────────────────────

FIRST_NAMES = [
    "Alex", "Maria", "Chen", "Fatima", "Lars", "Yuki", "Amir", "Sofia",
    "Raj", "Emma", "Diego", "Anna", "Kai", "Olivia", "Mateo", "Lena",
    "Noah", "Mia", "Lucas", "Ella", "Oscar", "Zara", "Ivan", "Luna",
]

LAST_NAMES = [
    "Smith", "Johansson", "Park", "Garcia", "Virtanen", "Nakamura",
    "Petrov", "Andersen", "Kumar", "Muller", "Silva", "Novak",
    "Tanaka", "Kowalski", "Ozols", "Mets", "Kim", "Larsen",
]

COUNTRIES = [
    "Estonia", "Finland", "Latvia", "Lithuania", "Sweden", "Norway",
    "Germany", "Spain", "Japan", "South Korea", "India", "Brazil",
    "Poland", "Italy", "France", "Netherlands", "Canada", "Australia",
]

CITIES = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]

EMAIL_DOMAINS = ["example.com", "mail.com", "inbox.org", "test.net"]


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "postgres"),
        port=int(os.environ.get("PG_PORT", 5432)),
        dbname=os.environ.get("PG_DB", "sourcedb"),
        user=os.environ.get("PG_USER", "cdc_user"),
        password=os.environ.get("PG_PASSWORD", "cdc_pass"),
    )


def execute(sql, params=None, fetch=False):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql, params)
    result = cur.fetchall() if fetch else None
    cur.close()
    conn.close()
    return result


def get_random_ids(table):
    """Get all IDs from a table for random selection."""
    rows = execute(f"SELECT id FROM {table};", fetch=True)
    return [r[0] for r in rows] if rows else []


# ── Mutation functions ────────────────────────────────────────────────────

def insert_customer():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    name = f"{first} {last}"
    email = f"{first.lower()}.{last.lower()}@{random.choice(EMAIL_DOMAINS)}"
    country = random.choice(COUNTRIES)
    execute("INSERT INTO customers (name, email, country) VALUES (%s, %s, %s);", (name, email, country))
    return f"INSERT customer: {name} ({country})"


def update_customer():
    ids = get_random_ids("customers")
    if not ids:
        return "SKIP update customer: no rows"
    cid = random.choice(ids)
    field = random.choice(["email", "country"])
    if field == "email":
        new_val = f"updated_{cid}_{random.randint(100,999)}@{random.choice(EMAIL_DOMAINS)}"
        execute("UPDATE customers SET email = %s WHERE id = %s;", (new_val, cid))
    else:
        new_val = random.choice(COUNTRIES)
        execute("UPDATE customers SET country = %s WHERE id = %s;", (new_val, cid))
    return f"UPDATE customer id={cid}: {field} → {new_val}"


def delete_customer():
    ids = get_random_ids("customers")
    if len(ids) <= 3:  # keep at least 3 rows
        return "SKIP delete customer: too few rows"
    cid = random.choice(ids)
    execute("DELETE FROM customers WHERE id = %s;", (cid,))
    return f"DELETE customer id={cid}"


def insert_driver():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    name = f"{first} {last}"
    license_num = f"TLC-{random.randint(20000, 99999)}"
    rating = round(random.uniform(3.5, 5.0), 2)
    city = random.choice(CITIES)
    execute(
        "INSERT INTO drivers (name, license_number, rating, city) VALUES (%s, %s, %s, %s);",
        (name, license_num, rating, city),
    )
    return f"INSERT driver: {name} ({city}, rating={rating})"


def update_driver():
    ids = get_random_ids("drivers")
    if not ids:
        return "SKIP update driver: no rows"
    did = random.choice(ids)
    action = random.choice(["rating", "city", "active"])
    if action == "rating":
        new_val = round(random.uniform(3.5, 5.0), 2)
        execute("UPDATE drivers SET rating = %s WHERE id = %s;", (new_val, did))
        return f"UPDATE driver id={did}: rating → {new_val}"
    elif action == "city":
        new_val = random.choice(CITIES)
        execute("UPDATE drivers SET city = %s WHERE id = %s;", (new_val, did))
        return f"UPDATE driver id={did}: city → {new_val}"
    else:
        execute("UPDATE drivers SET active = NOT active WHERE id = %s;", (did,))
        return f"UPDATE driver id={did}: toggled active"


def delete_driver():
    ids = get_random_ids("drivers")
    if len(ids) <= 3:
        return "SKIP delete driver: too few rows"
    did = random.choice(ids)
    execute("DELETE FROM drivers WHERE id = %s;", (did,))
    return f"DELETE driver id={did}"


# ── Weighted random operation selector ────────────────────────────────────

CUSTOMER_OPS = [
    (insert_customer, 40),   # 40% inserts
    (update_customer, 40),   # 40% updates
    (delete_customer, 20),   # 20% deletes
]

DRIVER_OPS = [
    (insert_driver, 30),
    (update_driver, 50),
    (delete_driver, 20),
]


def weighted_choice(ops):
    total = sum(w for _, w in ops)
    r = random.randint(1, total)
    cumulative = 0
    for fn, weight in ops:
        cumulative += weight
        if r <= cumulative:
            return fn
    return ops[0][0]


# ── Main loop ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Simulate OLTP changes for CDC testing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--rate", type=float, default=1.0,
                        help="Operations per second.")
    parser.add_argument("--tables", default="both", choices=["customers", "drivers", "both"],
                        help="Which tables to mutate.")
    args = parser.parse_args()

    interval = 1.0 / args.rate

    print("=" * 60)
    print("CDC Change Simulator")
    print("=" * 60)
    print(f"Rate:   {args.rate:.1f} ops/sec")
    print(f"Tables: {args.tables}")
    print("Press Ctrl-C to stop.\n")

    ops_count = 0
    t0 = time.monotonic()

    try:
        while True:
            # Pick a table
            if args.tables == "customers":
                op_fn = weighted_choice(CUSTOMER_OPS)
            elif args.tables == "drivers":
                op_fn = weighted_choice(DRIVER_OPS)
            else:
                if random.random() < 0.6:  # 60% customer, 40% driver
                    op_fn = weighted_choice(CUSTOMER_OPS)
                else:
                    op_fn = weighted_choice(DRIVER_OPS)

            try:
                result = op_fn()
                ops_count += 1
                elapsed = time.monotonic() - t0
                print(f"[{ops_count:>5} | {elapsed:>6.1f}s] {result}")
            except Exception as e:
                print(f"[ERROR] {e}")

            time.sleep(interval)

    except KeyboardInterrupt:
        pass
    finally:
        elapsed = time.monotonic() - t0
        print(f"\nStopped — {ops_count} operations in {elapsed:.0f}s "
              f"({ops_count / max(elapsed, 1):.1f} ops/s)")

        # Print final state
        print("\nFinal state:")
        c_count = execute("SELECT COUNT(*) FROM customers;", fetch=True)[0][0]
        d_count = execute("SELECT COUNT(*) FROM drivers;", fetch=True)[0][0]
        print(f"  customers: {c_count} rows")
        print(f"  drivers:   {d_count} rows")


if __name__ == "__main__":
    main()