"""Maria's stateful merger.

Reverts the target table to a prior healthy snapshot before applying the new
source files, so a backfill for an older date can't trample newer writes.
Mechanics: ledger lookup → engine rollback → engine merge → ledger record.

Notice: this module does not import any specific compute engine. It speaks
through the TableEngine abstraction only, which is why swapping Spark for
Trino later is a one-file change in engines/.
"""
from __future__ import annotations

import logging
from datetime import date

from internal_etl_package.config_loader import TableConfig
from internal_etl_package.engines.base import TableEngine
from internal_etl_package.ledger import Ledger


log = logging.getLogger(__name__)


def merge_table(
    table_config: TableConfig,
    execution_date: date,
    engine: TableEngine,
    ledger: Ledger,
) -> None:
    """Maria's stateful merger — reverts to a healthy past version before merging,
    so backfills don't trample newer data.

    Notice: this function does not import Spark, Trino, or any specific engine.
    It speaks to the world through the TableEngine interface only. This is what
    lets us swap execution engines without touching reliability logic.
    """
    table = table_config.name

    # ─── Phase 2, Step 1 of 2 ─────────────────────────────────────────────
    # Look up the snapshot that was active just *before* this execution_date.
    # The Ledger helper is already wired up for you (see ledger.py).
    # 👉 TODO: replace `None` below with the correct ledger call.
    snapshot_id = None  # TODO

    if snapshot_id is None:
        log.info(
            f"[{table}] No prior snapshot — first run for this date. "
            "Skipping rollback."
        )
    else:
        log.info(f"[{table}] Rolling back to snapshot {snapshot_id} before merge.")

        # ─── Phase 2, Step 2 of 2 ─────────────────────────────────────────
        # Restore the table to that snapshot. Notice you're talking to `engine`,
        # not to Spark directly — so this line works the same way whether the
        # underlying engine is Spark or any future engine (Trino, etc.).
        # 👉 TODO: call the engine method that rolls a table back to a snapshot.
        pass  # TODO

    # ─── Provided: the merge itself ──────────────────────────────────────
    new_snapshot_id = engine.merge(
        table=table,
        source_path=table_config.source_path,
        primary_key=table_config.primary_key,
    )

    # ─── Provided: bookkeeping (do NOT touch this) ───────────────────────
    ledger.record_snapshot(table, execution_date, new_snapshot_id)
    log.info(f"[{table}] Merged successfully. New snapshot: {new_snapshot_id}")
