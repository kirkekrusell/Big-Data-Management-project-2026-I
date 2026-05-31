"""Slack-mock alerting.

Two callable shapes (per spec §6.6):

  send_slack_alert(severity, table, message)
    Direct invocation. post_audit calls this with severity="CRITICAL" when a
    DQ failure is detected. WARNING is reserved for caller-driven warnings.

  slack_callback_on_failure(context)
    Airflow on_failure_callback adapter. Wired by dag_factory as the infra
    safety net — unexpected Spark/network crashes route here. Routes by
    exception type: DataQualityError → CRITICAL, everything else → CRITICAL.

Output lives under /opt/airflow/alerts/. Two files:

  slack.log    every alert, with ANSI color so `tail -f` is red/yellow live
  pager.log    CRITICAL only, prefixed `*BUZZ BUZZ*` — the "wake the oncall"

Channels (the chat-style chrome the format mimics):
  CRITICAL → #data-incidents
  WARNING  → #data-quality-drift
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path


# ANSI color codes; the terminal renders them when the facilitator tails the log.
_RESET = "\033[0m"
_RED = "\033[31m"
_YELLOW = "\033[33m"

_COLORS = {"CRITICAL": _RED, "WARNING": _YELLOW}
_CHANNELS = {"CRITICAL": "#data-incidents", "WARNING": "#data-quality-drift"}
_ICONS = {"CRITICAL": "🚨", "WARNING": "⚠️"}


def _alert_dir() -> Path:
    return Path(os.environ.get("ALERT_DIR", "/opt/airflow/alerts"))


def _slack_log() -> Path:
    return Path(os.environ.get("SLACK_MOCK_PATH", str(_alert_dir() / "slack.log")))


def _pager_log() -> Path:
    return Path(os.environ.get("PAGER_LOG_PATH", str(_alert_dir() / "pager.log")))


def _format(severity: str, body: str) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    icon = _ICONS.get(severity, "🚨")
    channel = _CHANNELS.get(severity, "#data-incidents")
    return f"[{ts}] {icon} {severity} {channel} | MariaBot | {body}"


def _emit(severity: str, body: str) -> None:
    line = _format(severity, body)
    color = _COLORS.get(severity, _RED)

    slack = _slack_log()
    pager = _pager_log()
    slack.parent.mkdir(parents=True, exist_ok=True)
    with open(slack, "a", encoding="utf-8") as f:
        f.write(f"{color}{line}{_RESET}\n")

    if severity == "CRITICAL":
        with open(pager, "a", encoding="utf-8") as f:
            f.write(f"*BUZZ BUZZ* {line}\n")


def send_slack_alert(severity: str, table: str, message: str) -> None:
    """Direct invocation used by post_audit.

    `severity` must be "CRITICAL" or "WARNING". CRITICAL writes to both
    slack.log and pager.log; WARNING writes only to slack.log.
    """
    _emit(severity=severity, body=f"table={table} | {message}")


def slack_callback_on_failure(context: dict) -> None:
    """Airflow on_failure_callback adapter.

    Routes the failure by exception type. Explicit DQ failures inside
    post_audit already called send_slack_alert directly with CRITICAL — this
    callback is the safety net for unexpected infra crashes (Spark hang,
    network timeout, etc.) which we also treat as CRITICAL by default.
    """
    from internal_etl_package import DataQualityError

    dag = context.get("dag")
    exc = context.get("exception")
    dag_id = getattr(dag, "dag_id", "unknown_dag")

    severity = "CRITICAL" if isinstance(exc, (DataQualityError, BaseException)) else "WARNING"
    _emit(
        severity=severity,
        body=f"DAG={dag_id} FAILED — {exc}",
    )
