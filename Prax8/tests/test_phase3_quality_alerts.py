"""Phase 3 — duplicate detection, alert routing, and the circuit breaker.

The four tests below verify that the participant's post_audit fix:

  1. Pages CRITICAL to slack.log AND pager.log when duplicates exist.
  2. Raises DataQualityError so Airflow marks the task failed (the
     "downstream consumers never see poisoned data" guarantee).
  3. Does not page or raise on clean data.
  4. The slack format includes the table name and a description of the
     failure.

Skipped until post_audit's three TODOs are filled in.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from internal_etl_package import DataQualityError, alerting, quality
from internal_etl_package.engines.base import TableEngine


_QUALITY_PATH = Path("/opt/airflow/internal_etl_package/quality.py")
pytestmark = pytest.mark.skipif(
    "# TODO" in _QUALITY_PATH.read_text(),
    reason="quality.py still has Phase 3 TODOs — fill them in to enable",
)


@pytest.fixture
def alert_paths(tmp_path, monkeypatch):
    monkeypatch.setenv("ALERT_DIR", str(tmp_path))
    monkeypatch.setenv("SLACK_MOCK_PATH", str(tmp_path / "slack.log"))
    monkeypatch.setenv("PAGER_LOG_PATH", str(tmp_path / "pager.log"))
    return tmp_path / "slack.log", tmp_path / "pager.log"


def _engine_with_dups(n: int):
    e = MagicMock(spec=TableEngine)
    e.count_duplicates.return_value = n
    return e


def test_duplicate_keys_trigger_data_quality_error(users_config, alert_paths):
    engine = _engine_with_dups(7)
    with pytest.raises(DataQualityError, match="7 duplicate keys"):
        quality.post_audit(users_config, engine)


def test_critical_alert_writes_to_slack_and_pager(users_config, alert_paths):
    slack, pager = alert_paths
    engine = _engine_with_dups(5)
    with pytest.raises(DataQualityError):
        quality.post_audit(users_config, engine)
    assert "CRITICAL" in slack.read_text()
    assert "*BUZZ BUZZ*" in pager.read_text()


def test_clean_data_does_not_page_or_raise(users_config, alert_paths):
    slack, pager = alert_paths
    engine = _engine_with_dups(0)
    quality.post_audit(users_config, engine)
    assert not slack.exists() or slack.read_text() == ""
    assert not pager.exists() or pager.read_text() == ""


def test_slack_alert_format_includes_table_name_and_error(users_config, alert_paths):
    slack, _ = alert_paths
    engine = _engine_with_dups(3)
    with pytest.raises(DataQualityError):
        quality.post_audit(users_config, engine)
    text = slack.read_text()
    assert "prd.users" in text
    assert "3 duplicate keys" in text


def test_warning_alert_does_not_write_to_pager_log(alert_paths):
    """Cross-check: a non-Phase-3 WARNING path (e.g. from a future completeness
    auto-alert) must skip the pager. Pinned here so post_audit's fix doesn't
    accidentally route warnings to pager."""
    _, pager = alert_paths
    alerting.send_slack_alert("WARNING", "prd.orders", "null rate elevated")
    assert not pager.exists() or pager.read_text() == ""
