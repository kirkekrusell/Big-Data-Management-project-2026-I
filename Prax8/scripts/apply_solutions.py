#!/usr/bin/env python3
"""Copy *.solution -> *.py so the live files contain working code.

Used during dev iteration (not exposed to participants). Idempotent.
"""
import shutil
import sys
from pathlib import Path


PAIRS = [
    ("internal_etl_package/merger.py.solution", "internal_etl_package/merger.py"),
    ("internal_etl_package/quality.py.solution", "internal_etl_package/quality.py"),
]


def _root() -> Path:
    """Project root containing internal_etl_package/.

    The container mounts internal_etl_package/ under /opt/airflow/ while
    scripts/ lives at /opt/scripts/ — script_dir/.. doesn't work. Pick the
    first candidate that has the expected layout.
    """
    here = Path(__file__).resolve().parent
    for cand in (Path("/opt/airflow"), here.parent, here.parent.parent):
        if (cand / "internal_etl_package" / "merger.py").exists():
            return cand
    raise SystemExit("Could not locate internal_etl_package/")


def main() -> int:
    root = _root()
    for src, dst in PAIRS:
        shutil.copy(root / src, root / dst)
        print(f"applied  {src}  →  {dst}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
