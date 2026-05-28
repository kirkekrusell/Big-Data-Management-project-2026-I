"""Shared helpers for the chaos scripts: Maria's voice + path resolution.

Each chaos script imports from here so the workshop's tone and "where things
live" are configured in one place.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


_BLUE = "\033[34m"
_GREEN = "\033[32m"
_RED = "\033[31m"
_RESET = "\033[0m"
_BOLD = "\033[1m"


def maria_says(message: str) -> None:
    """Print a Slack-style 'Maria via Slack:' line, in blue."""
    print(f"{_BLUE}💬 Maria (via Slack): {_BOLD}{message}{_RESET}")


def info(message: str) -> None:
    print(f"{_GREEN}→ {message}{_RESET}")


def error(message: str) -> None:
    print(f"{_RED}✗ {message}{_RESET}", file=sys.stderr)


def data_root() -> Path:
    return Path(os.environ.get("DATA_ROOT", "/opt/data"))


def alerts_dir() -> Path:
    return Path(os.environ.get("ALERT_DIR", "/opt/airflow/alerts"))
