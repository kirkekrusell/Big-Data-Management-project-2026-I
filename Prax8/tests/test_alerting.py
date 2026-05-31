"""Tests for the Slack-mock alerter.

Routes alerts to a tmp_path via env vars so the real /opt/airflow/alerts/
files are never touched.
"""
import pytest

from internal_etl_package import DataQualityError
from internal_etl_package import alerting


@pytest.fixture
def alert_paths(tmp_path, monkeypatch):
    monkeypatch.setenv("ALERT_DIR", str(tmp_path))
    monkeypatch.setenv("SLACK_MOCK_PATH", str(tmp_path / "slack.log"))
    monkeypatch.setenv("PAGER_LOG_PATH", str(tmp_path / "pager.log"))
    return tmp_path / "slack.log", tmp_path / "pager.log"


# ─── send_slack_alert (direct invocation) ───────────────────────────────────


def test_critical_writes_to_slack_log(alert_paths):
    slack, _ = alert_paths
    alerting.send_slack_alert("CRITICAL", "prd.users", "DQ Failure: 5 duplicate keys")
    line = slack.read_text()
    assert "CRITICAL" in line
    assert "prd.users" in line
    assert "5 duplicate keys" in line


def test_critical_also_writes_to_pager_log(alert_paths):
    slack, pager = alert_paths
    alerting.send_slack_alert("CRITICAL", "prd.users", "DQ Failure")
    assert "*BUZZ BUZZ*" in pager.read_text()


def test_warning_writes_to_slack_log(alert_paths):
    slack, _ = alert_paths
    alerting.send_slack_alert("WARNING", "prd.users", "Null rate elevated")
    assert "WARNING" in slack.read_text()


def test_warning_does_not_write_to_pager_log(alert_paths):
    _, pager = alert_paths
    alerting.send_slack_alert("WARNING", "prd.users", "Null rate elevated")
    assert not pager.exists() or pager.read_text() == ""


def test_critical_routes_to_data_incidents_channel(alert_paths):
    slack, _ = alert_paths
    alerting.send_slack_alert("CRITICAL", "t", "boom")
    assert "#data-incidents" in slack.read_text()


def test_warning_routes_to_data_quality_drift_channel(alert_paths):
    slack, _ = alert_paths
    alerting.send_slack_alert("WARNING", "t", "drift")
    assert "#data-quality-drift" in slack.read_text()


def test_slack_log_format_includes_timestamp(alert_paths):
    slack, _ = alert_paths
    alerting.send_slack_alert("CRITICAL", "prd.users", "boom")
    import re
    assert re.search(r"\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]", slack.read_text())


def test_slack_log_format_includes_table_and_message(alert_paths):
    slack, _ = alert_paths
    alerting.send_slack_alert("CRITICAL", "prd.users", "47 duplicate keys")
    text = slack.read_text()
    assert "table=prd.users" in text
    assert "47 duplicate keys" in text


def test_slack_log_uses_ansi_color_codes(alert_paths):
    """The facilitator tails this file live; the terminal renders the color."""
    slack, _ = alert_paths
    alerting.send_slack_alert("CRITICAL", "t", "boom")
    text = slack.read_text()
    assert "\033[31m" in text  # red
    assert "\033[0m" in text   # reset


def test_multiple_alerts_append(alert_paths):
    slack, _ = alert_paths
    alerting.send_slack_alert("CRITICAL", "a", "1")
    alerting.send_slack_alert("WARNING", "b", "2")
    lines = slack.read_text().strip().splitlines()
    assert len(lines) == 2


# ─── slack_callback_on_failure (Airflow callback adapter) ───────────────────


def _ctx(dag_id="replicate__prd__users", exc=None):
    return {
        "dag": type("D", (), {"dag_id": dag_id})(),
        "exception": exc,
    }


def test_callback_writes_dag_id_and_exception(alert_paths):
    slack, _ = alert_paths
    alerting.slack_callback_on_failure(_ctx(exc=RuntimeError("spark crashed")))
    text = slack.read_text()
    assert "replicate__prd__users" in text
    assert "spark crashed" in text


def test_callback_writes_to_pager_log_for_data_quality_error(alert_paths):
    _, pager = alert_paths
    alerting.slack_callback_on_failure(_ctx(exc=DataQualityError("dup keys")))
    assert "*BUZZ BUZZ*" in pager.read_text()


def test_callback_handles_missing_dag_gracefully(alert_paths):
    slack, _ = alert_paths
    alerting.slack_callback_on_failure({"exception": ValueError("oops")})
    assert "unknown_dag" in slack.read_text()
