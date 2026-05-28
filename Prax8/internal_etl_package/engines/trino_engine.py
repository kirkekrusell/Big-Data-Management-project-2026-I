"""Trino + Iceberg adapter — stub.

This file is the Open/Closed exhibit: a future engineer migrating to Trino
fills in the method bodies and nothing else in the repo changes. The factory
already knows how to wire it up, the merger and quality code already talk to
TableEngine, the tests already mock the interface.

The class exists, the import works, the contract is declared. See
`marias_notes/take_home_trino.md` for the migration brief.
"""
from internal_etl_package.engines.base import TableEngine


_NOT_IMPLEMENTED_MSG = "Trino migration — see marias_notes/take_home_trino.md"


class TrinoIcebergEngine(TableEngine):
    """Stub — Trino migration queued for Q2. See marias_notes/take_home_trino.md.

    Demonstrates Open/Closed: a new engine arrives as a new file, with zero
    edits to merger.py, quality.py, ledger.py, or dag_factory.py.
    """

    def __init__(self, trino_conn=None):
        self._trino = trino_conn

    def current_snapshot_id(self, table: str) -> int:
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    def rollback_to_snapshot(self, table: str, snapshot_id: int) -> None:
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    def merge(self, table: str, source_path: str, primary_key: str) -> int:
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    def count_duplicates(self, table: str, primary_key: str) -> int:
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    def row_count(self, table: str) -> int:
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    def null_rate(self, table: str, column: str) -> float:
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)
