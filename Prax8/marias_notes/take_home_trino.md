# Take-home — wire up the Trino engine 🚀

If you enjoyed today's lab and want to feel the Open/Closed principle
pay you back, here's a ticket I've had on my backlog forever:

**Goal:** make `engine: trino` work in `config/tables.yaml`.

The seam is already in place. Right now
`internal_etl_package/engines/trino_engine.py` is a stub — every
method raises `NotImplementedError`. Your job:

1. Add the `trino` package import at the top (already permitted by
   the architectural test — `engines/` is the only directory allowed
   to import compute libraries). The `trino` Python DBAPI client is
   already installed in the airflow image.
2. Implement the six methods on the `TableEngine` ABC the same way
   `spark_engine.py` does, but speak Trino SQL through the
   `iceberg_datalake` catalog. The dialect is mostly the same — the
   rollback call is
   `CALL iceberg_datalake.system.rollback_to_snapshot('schema',
   'table', snapshot_id)` (Trino registers this procedure because we
   moved the catalog to Hive Metastore — under the old Nessie catalog
   it wasn't available, and the take-home was harder).
3. Read row counts and the `$snapshots` metadata table via
   `iceberg_datalake.<schema>."<table>$snapshots"` (the `"` quoting is
   required because of the `$`).

Notice what you do **NOT** have to edit: `merger.py`, `quality.py`,
`ledger.py`, `dag_factory.py`. That's the whole point.

The architecture tests in `tests/test_architecture.py` will keep
you honest. — Maria 💚
