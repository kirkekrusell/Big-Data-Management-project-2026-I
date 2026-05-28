#!/usr/bin/env python3
"""Phase 3 demo — sneak duplicate primary keys into prd.users.

Two effects:

  1. Source side: drop a file with rows whose user_id collides with existing
     records (same id, different email). post_audit's count_duplicates check
     should fire on the next DAG run.
  2. Warehouse side: INSERT INTO iceberg.prd.users with the same rows so the
     duplicates are observable immediately (MERGE INTO would dedupe them via
     ON pk=pk, defeating the purpose).

Idempotent: deletes any prior chaos file before writing, and the INSERT is
re-run-safe (it adds the same fixed set of duplicates each time).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from chaos._voice import data_root, error, info, maria_says


# Fixed set of dups so the test gate sees a deterministic count_duplicates.
DUPLICATE_USER_IDS = list(range(1, 11))  # user_ids 1..10, ten duplicate keys
DUP_FILE_NAME = "dups.csv"


def write_duplicate_source(root: Path) -> Path:
    """Drop dups.csv next to the normal source file."""
    out_dir = root / "source" / "users"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / DUP_FILE_NAME

    with open(out_file, "w", encoding="utf-8", newline="") as f:
        f.write("user_id,email,created_at\n")
        for uid in DUPLICATE_USER_IDS:
            # Same user_id as the real row but a different email — a clear
            # "two competing values for one primary key" smell.
            f.write(f"{uid},chaos{uid:04d}@example.com,2024-10-09 00:00:00\n")

    info(f"wrote {len(DUPLICATE_USER_IDS)} duplicate-laden rows to {out_file}")
    return out_file


def insert_duplicates_into_warehouse() -> None:
    """Append duplicate rows directly so count_duplicates() sees them.

    Imported lazily so unit tests don't pull pyspark.
    """
    from internal_etl_package.engines.spark_engine import _get_or_create_spark

    spark = _get_or_create_spark()
    # Spark SQL VALUES(...) inserting the same shape as users.csv.
    rows = ",".join(
        f"({uid}, 'chaos{uid:04d}@example.com', TIMESTAMP '2024-10-09 00:00:00')"
        for uid in DUPLICATE_USER_IDS
    )
    spark.sql(
        f"INSERT INTO iceberg.prd.users (user_id, email, created_at) VALUES {rows}"
    )
    info(f"inserted {len(DUPLICATE_USER_IDS)} duplicate rows into iceberg.prd.users")


def main(*, skip_spark: bool = False) -> int:
    maria_says(
        "Heads up — a colleague accidentally re-ran the user export against "
        "the staging table. There may be dupes incoming. 😬"
    )

    try:
        write_duplicate_source(data_root())
        if not skip_spark and os.environ.get("CHAOS_SKIP_SPARK") != "1":
            insert_duplicates_into_warehouse()
        else:
            info("skipped warehouse INSERT (env)")
    except Exception as exc:
        error(f"could not inject duplicates: {exc}")
        return 1

    print()
    info("post_audit should fire on the next DAG run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
