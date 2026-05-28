"""Data-quality checks and the post-merge audit.

Four read-only checks — freshness, volume, completeness, consistency — return
a CheckResult and are the Phase 1.5 walkthrough material.

post_audit is the active step: count duplicates on the primary key, page
#data-incidents if any are found, then raise DataQualityError so Airflow
marks the task failed and downstream consumers never see the poisoned table.

Like the merger, this module depends on the TableEngine abstraction, not on
any specific compute engine.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from internal_etl_package import DataQualityError
from internal_etl_package.alerting import send_slack_alert
from internal_etl_package.config_loader import TableConfig
from internal_etl_package.engines.base import TableEngine


log = logging.getLogger(__name__)


CheckStatus = Literal["OK", "WARNING", "CRITICAL"]

MAX_NULL_RATE = 0.01  # 1% null in a critical column triggers a warning
MIN_ROW_COUNT = 1     # any rows is the floor; tighten per table later


@dataclass(frozen=True)
class CheckResult:
    status: CheckStatus
    message: str


def freshness(table_config: TableConfig, engine: TableEngine) -> CheckResult:
    """Proxy freshness via row count.

    A proper freshness check would inspect the latest snapshot's commit
    timestamp, but that information isn't on the six-method TableEngine seam
    by design. An empty table is the only signal we can derive through the
    interface, so that's what this check reports. The freshness_sla_hours
    field on TableConfig is consumed by an Airflow sensor in a later task.
    """
    table = table_config.name
    n = engine.row_count(table)
    if n == 0:
        return CheckResult("CRITICAL", f"{table}: empty — no rows ingested")
    return CheckResult("OK", f"{table}: {n} rows present")


def volume(table_config: TableConfig, engine: TableEngine) -> CheckResult:
    table = table_config.name
    n = engine.row_count(table)
    if n < MIN_ROW_COUNT:
        return CheckResult(
            "CRITICAL", f"{table}: only {n} rows (below floor {MIN_ROW_COUNT})"
        )
    return CheckResult("OK", f"{table}: {n} rows")


def completeness(table_config: TableConfig, engine: TableEngine) -> CheckResult:
    table = table_config.name
    breaches: list[tuple[str, float]] = []
    for column in table_config.critical_columns:
        rate = engine.null_rate(table, column)
        if rate > MAX_NULL_RATE:
            breaches.append((column, rate))
    if breaches:
        worst_col, worst_rate = max(breaches, key=lambda b: b[1])
        return CheckResult(
            "WARNING",
            f"{table}: null rate {worst_rate:.2%} in `{worst_col}` "
            f"exceeds threshold {MAX_NULL_RATE:.2%}",
        )
    return CheckResult(
        "OK", f"{table}: every critical column within null-rate threshold"
    )


def consistency(table_config: TableConfig, engine: TableEngine) -> CheckResult:
    table = table_config.name
    dups = engine.count_duplicates(table, table_config.primary_key)
    if dups > 0:
        return CheckResult(
            "CRITICAL",
            f"{table}: {dups} duplicate `{table_config.primary_key}` values",
        )
    return CheckResult("OK", f"{table}: primary key unique")


def post_audit(table_config: TableConfig, engine: TableEngine) -> None:
    """The ACA pattern's third stage — verify the table is healthy AFTER merge.

    If duplicates exist on the primary key, the table is considered poisoned
    and we MUST stop the pipeline before downstream consumers see it.
    """
    pk = table_config.primary_key
    table = table_config.name

    # ─── Phase 3, Step 1 of 3 ─────────────────────────────────────────────
    # Count primary-key values that appear more than once.
    # The engine already exposes a method for this — find it in engines/base.py.
    # 👉 TODO: replace 0 with a call to the engine.
    duplicate_count = 0  # TODO

    log.info(f"[{table}] Post-audit: {duplicate_count} duplicate keys found.")

    if duplicate_count > 0:
        # ─── Phase 3, Step 2 of 3 ─────────────────────────────────────────
        # Page the team. Use `send_slack_alert` (already imported above) with
        # severity="CRITICAL" so it routes to #data-incidents AND the pager.
        # 👉 TODO: call send_slack_alert(...) before raising below.
        pass  # TODO

        # ─── Phase 3, Step 3 of 3 ─────────────────────────────────────────
        # Stop the pipeline so downstream consumers never see the bad data.
        # 👉 TODO: raise DataQualityError with a helpful message.
        pass  # TODO
