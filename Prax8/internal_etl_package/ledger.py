"""Postgres-backed snapshot ledger.

The merger writes one row per (table, execution_date) every time it commits a
new Iceberg snapshot. On the next run for the same date, it asks the ledger
for the most recent snapshot *before* that date and rolls the table back to
it before re-applying — that's how backfills stop trampling newer data.

Schema is deliberately append-only: no primary key on (table_name,
execution_date). Multiple inserts for the same date are expected (each
backfill creates a fresh snapshot) and get_snapshot_before picks the latest
by recorded_at when they collide.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date
from typing import Optional

import psycopg2

log = logging.getLogger(__name__)


LEDGER_TABLE = "snapshot_ledger"

_DDL = f"""
CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
    table_name      TEXT      NOT NULL,
    execution_date  DATE      NOT NULL,
    snapshot_id     BIGINT    NOT NULL,
    recorded_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class Ledger:
    """Records every snapshot id we commit and lets callers look up the
    most recent one before a given date."""

    def __init__(self, connection_string: str):
        self._dsn = connection_string
        self._initialized = False

    @contextmanager
    def _connect(self):
        conn = psycopg2.connect(self._dsn)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        if self._initialized:
            return
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(_DDL)
        self._initialized = True

    def record_snapshot(
        self, table: str, execution_date: date, snapshot_id: int
    ) -> None:
        """Append a row recording that `table` was committed at
        `snapshot_id` for `execution_date`."""
        self._ensure_schema()
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {LEDGER_TABLE} "
                "(table_name, execution_date, snapshot_id) VALUES (%s, %s, %s)",
                (table, execution_date, int(snapshot_id)),
            )

    def get_snapshot_before(
        self, table: str, execution_date: date
    ) -> Optional[int]:
        """Return the snapshot id of the most recent record whose
        execution_date is strictly less than the given date, or None if no
        such record exists.

        When several rows share an execution_date, the one inserted last
        (largest recorded_at) wins.
        """
        self._ensure_schema()
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT snapshot_id
                FROM {LEDGER_TABLE}
                WHERE table_name = %s AND execution_date < %s
                ORDER BY execution_date DESC, recorded_at DESC
                LIMIT 1
                """,
                (table, execution_date),
            )
            row = cur.fetchone()
        return int(row[0]) if row else None
