#!/usr/bin/env python3
"""Copy *.starter -> *.py so the live files contain TODO placeholders.

The clean-state reset for dev iteration. Idempotent.
"""
import shutil
import sys
from pathlib import Path


PAIRS = [
    ("internal_etl_package/merger.py.starter", "internal_etl_package/merger.py"),
    ("internal_etl_package/quality.py.starter", "internal_etl_package/quality.py"),
]


def _root() -> Path:
    """Project root containing internal_etl_package/. Mirrors apply_solutions."""
    here = Path(__file__).resolve().parent
    for cand in (Path("/opt/airflow"), here.parent, here.parent.parent):
        if (cand / "internal_etl_package" / "merger.py").exists():
            return cand
    raise SystemExit("Could not locate internal_etl_package/")


def main() -> int:
    root = _root()
    for src, dst in PAIRS:
        shutil.copy(root / src, root / dst)
        print(f"reverted {src}  →  {dst}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
