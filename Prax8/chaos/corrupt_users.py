#!/usr/bin/env python3
"""Phase 2 demo — simulate an incomplete Oct 7 backfill.

Two effects, applied in order:

  1. Source side: rewrite data/source/users/users.csv to a truncated copy
     where every row with created_at >= 2024-10-08 has been dropped. This
     is "the analyst's bad backfill file" the team thinks is good.
  2. Warehouse side: DELETE FROM iceberg.prd.users WHERE created_at >=
     '2024-10-08'. This represents the "bad backfill already ran" state.
     MERGE INTO alone can't produce this because MERGE doesn't delete
     rows that are missing from the source.

Idempotent: re-running just re-applies the same corruption.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from chaos._voice import data_root, error, info, maria_says


CUTOFF_DATE = "2024-10-08"


def write_undercount_source(root: Path) -> int:
    """Rewrite source/users/users.csv keeping only rows with created_at < cutoff.

    Returns the number of rows kept.
    """
    seed = root / "seed" / "users.csv"
    if not seed.exists():
        raise FileNotFoundError(f"seed file missing: {seed}")

    out_dir = root / "source" / "users"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "users.csv"

    kept = 0
    dropped = 0
    with open(seed, "r", encoding="utf-8") as f_in, open(
        out_file, "w", encoding="utf-8", newline=""
    ) as f_out:
        header = f_in.readline()
        f_out.write(header)
        for line in f_in:
            # created_at is the third column, format YYYY-MM-DD HH:MM:SS
            parts = line.rstrip("\n").split(",", 2)
            created_at = parts[2] if len(parts) >= 3 else ""
            if created_at[:10] < CUTOFF_DATE:
                f_out.write(line)
                kept += 1
            else:
                dropped += 1

    info(f"wrote undercount source: kept {kept} rows, dropped {dropped} (>= {CUTOFF_DATE})")
    return kept


def delete_late_rows_from_warehouse() -> None:
    """DELETE FROM iceberg.prd.users WHERE created_at >= cutoff.

    Imported lazily so unit tests don't pull pyspark.
    """
    from internal_etl_package.engines.spark_engine import _get_or_create_spark

    spark = _get_or_create_spark()
    spark.sql(
        f"DELETE FROM iceberg.prd.users WHERE created_at >= TIMESTAMP '{CUTOFF_DATE} 00:00:00'"
    )
    info(f"deleted rows from iceberg.prd.users where created_at >= {CUTOFF_DATE}")


def main(*, skip_spark: bool = False) -> int:
    maria_says(
        "Hey team, urgent — analyst flagged that yesterday's user signups "
        "were incomplete. I'm pushing the backfill now. 🤞"
    )

    try:
        kept = write_undercount_source(data_root())
        if not skip_spark and os.environ.get("CHAOS_SKIP_SPARK") != "1":
            delete_late_rows_from_warehouse()
        else:
            info("skipped warehouse DELETE (env)")
    except Exception as exc:
        error(f"could not apply corruption: {exc}")
        return 1

    print()
    info(f"prd.users source now has {kept} rows. Trigger the DAG to apply.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
