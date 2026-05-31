#!/usr/bin/env python3
"""Stress-test bonus — run every chaos scenario in sequence, then reset.

Useful for facilitator pre-flight: prove every demo's mechanics fire end-to-end
before the room watches.
"""
from __future__ import annotations

import sys

from chaos import corrupt_users, inject_duplicates, reset
from chaos._voice import info, maria_says


def main() -> int:
    maria_says("Running every chaos demo in sequence. Hold onto your dashboards.")

    for step in (reset.main, corrupt_users.main, reset.main, inject_duplicates.main, reset.main):
        info(f"---- {step.__module__} ----")
        rc = step()
        if rc != 0:
            return rc

    return 0


if __name__ == "__main__":
    sys.exit(main())
