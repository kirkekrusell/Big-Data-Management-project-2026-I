#!/usr/bin/env python3
"""
Seeds the PostgreSQL source database with initial tables and data.

Creates two tables:
  - customers: people who take taxi rides
  - drivers:   taxi drivers with license and rating info

Run once after `docker compose up` to set up the CDC source.

Usage:
    docker exec jupyter python /home/jovyan/project/seed.py
"""

import os
import sys

def _ensure(pkg, import_name=None):
    import importlib.util, subprocess
    if importlib.util.find_spec(import_name or pkg) is None:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

_ensure("psycopg2-binary", "psycopg2")

import psycopg2


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "postgres"),
        port=int(os.environ.get("PG_PORT", 5432)),
        dbname=os.environ.get("PG_DB", "sourcedb"),
        user=os.environ.get("PG_USER", "cdc_user"),
        password=os.environ.get("PG_PASSWORD", "cdc_pass"),
    )


def execute(sql):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()


def fetch(sql):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def main():
    print("=" * 60)
    print("Seeding PostgreSQL source database")
    print("=" * 60)

    # ── Verify WAL level ───────────────────────────────────────
    wal = fetch("SHOW wal_level;")
    print(f"\nwal_level = {wal[0][0]}")
    if wal[0][0] != "logical":
        sys.exit("ERROR: wal_level must be 'logical' for CDC!")

    # ── Create tables ──────────────────────────────────────────
    print("\nCreating tables...")

    execute("""
        DROP TABLE IF EXISTS drivers CASCADE;
        DROP TABLE IF EXISTS customers CASCADE;

        CREATE TABLE customers (
            id         SERIAL PRIMARY KEY,
            name       VARCHAR(100) NOT NULL,
            email      VARCHAR(200) NOT NULL,
            country    VARCHAR(50)  NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE drivers (
            id            SERIAL PRIMARY KEY,
            name          VARCHAR(100) NOT NULL,
            license_number VARCHAR(20) NOT NULL,
            rating        DECIMAL(3,2) DEFAULT 4.50,
            city          VARCHAR(50)  NOT NULL,
            active        BOOLEAN DEFAULT TRUE,
            created_at    TIMESTAMP DEFAULT NOW()
        );
    """)
    print("  ✓ customers")
    print("  ✓ drivers")

    # ── Seed customers ─────────────────────────────────────────
    print("\nInserting customers...")
    execute("""
        INSERT INTO customers (name, email, country) VALUES
            ('Alice Mets',      'alice@example.com',     'Estonia'),
            ('Bob Virtanen',    'bob@example.com',       'Finland'),
            ('Carol Ozols',     'carol@example.com',     'Latvia'),
            ('David Jonaitis',  'david@example.com',     'Lithuania'),
            ('Eva Svensson',    'eva@example.com',       'Sweden'),
            ('Frank Muller',    'frank@example.com',     'Germany'),
            ('Grace Kim',       'grace@example.com',     'South Korea'),
            ('Hiro Tanaka',     'hiro@example.com',      'Japan'),
            ('Ingrid Larsen',   'ingrid@example.com',    'Norway'),
            ('Javier Garcia',   'javier@example.com',    'Spain');
    """)
    count = fetch("SELECT COUNT(*) FROM customers;")[0][0]
    print(f"  ✓ {count} customers inserted")

    # ── Seed drivers ───────────────────────────────────────────
    print("\nInserting drivers...")
    execute("""
        INSERT INTO drivers (name, license_number, rating, city) VALUES
            ('Tom Driver',      'TLC-10001', 4.85, 'Manhattan'),
            ('Sarah Wheels',    'TLC-10002', 4.72, 'Brooklyn'),
            ('Mike Road',       'TLC-10003', 4.91, 'Queens'),
            ('Lisa Lane',       'TLC-10004', 4.68, 'Bronx'),
            ('Jake Taxi',       'TLC-10005', 4.55, 'Manhattan'),
            ('Nina Cab',        'TLC-10006', 4.93, 'Brooklyn'),
            ('Omar Ride',       'TLC-10007', 4.80, 'Queens'),
            ('Priya Fast',      'TLC-10008', 4.77, 'Manhattan');
    """)
    count = fetch("SELECT COUNT(*) FROM drivers;")[0][0]
    print(f"  ✓ {count} drivers inserted")

    # ── Summary ────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Seed complete!")
    print("=" * 60)

    print("\nCustomers:")
    for row in fetch("SELECT id, name, email, country FROM customers ORDER BY id;"):
        print(f"  {row}")

    print("\nDrivers:")
    for row in fetch("SELECT id, name, license_number, rating, city FROM drivers ORDER BY id;"):
        print(f"  {row}")

    print(f"\nReady for Debezium CDC. Tables to monitor:")
    print(f"  public.customers ({fetch('SELECT COUNT(*) FROM customers;')[0][0]} rows)")
    print(f"  public.drivers   ({fetch('SELECT COUNT(*) FROM drivers;')[0][0]} rows)")


if __name__ == "__main__":
    main()