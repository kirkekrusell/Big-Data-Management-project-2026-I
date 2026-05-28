"""Tests for the DQ checks and post_audit.

Mock-only — TableEngine is a MagicMock and the alerting module is patched in
the post_audit tests so they don't touch the real slack.log file.

The post_audit tests assert the Phase 3 *fix* (count duplicates → alert →
raise). They get skipped when the live quality.py still has its starter
TODOs. The freshness/volume/completeness/consistency tests run regardless
because those four checks ship complete in the starter.
"""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from internal_etl_package import DataQualityError, quality
from internal_etl_package.engines.base import TableEngine


_QUALITY_PATH = Path(__file__).resolve().parent.parent / "internal_etl_package" / "quality.py"
_quality_has_todos = "# TODO" in _QUALITY_PATH.read_text()
_post_audit_skip = pytest.mark.skipif(
    _quality_has_todos,
    reason="quality.py post_audit is in starter state — apply solutions first",
)


def _engine(**returns):
    e = MagicMock(spec=TableEngine)
    for name, value in returns.items():
        getattr(e, name).return_value = value
    return e


# ─── post_audit ─────────────────────────────────────────────────────────────


@_post_audit_skip
def test_post_audit_passes_on_clean_data(users_config):
    engine = _engine(count_duplicates=0)
    quality.post_audit(users_config, engine)
    engine.count_duplicates.assert_called_once_with("prd.users", "user_id")


@_post_audit_skip
def test_post_audit_raises_on_duplicates(users_config):
    engine = _engine(count_duplicates=5)
    with patch.object(quality, "send_slack_alert") as alert:
        with pytest.raises(DataQualityError, match="5 duplicate keys"):
            quality.post_audit(users_config, engine)
        alert.assert_called_once()
        kwargs = alert.call_args.kwargs
        assert kwargs["severity"] == "CRITICAL"
        assert kwargs["table"] == "prd.users"
        assert "5" in kwargs["message"]


@_post_audit_skip
def test_post_audit_does_not_alert_on_clean_data(users_config):
    engine = _engine(count_duplicates=0)
    with patch.object(quality, "send_slack_alert") as alert:
        quality.post_audit(users_config, engine)
        alert.assert_not_called()


# ─── freshness / volume / completeness / consistency ────────────────────────


def test_freshness_ok_when_rows_present(users_config):
    r = quality.freshness(users_config, _engine(row_count=1000))
    assert r.status == "OK"


def test_freshness_critical_when_table_empty(users_config):
    r = quality.freshness(users_config, _engine(row_count=0))
    assert r.status == "CRITICAL"
    assert "empty" in r.message.lower()


def test_volume_ok_above_floor(users_config):
    r = quality.volume(users_config, _engine(row_count=1000))
    assert r.status == "OK"


def test_volume_critical_below_floor(users_config):
    r = quality.volume(users_config, _engine(row_count=0))
    assert r.status == "CRITICAL"


def test_completeness_ok_when_nulls_below_threshold(users_config):
    r = quality.completeness(users_config, _engine(null_rate=0.001))
    assert r.status == "OK"


def test_completeness_warning_when_any_critical_column_too_null(users_config):
    engine = MagicMock(spec=TableEngine)
    # First two critical columns are fine, third (created_at) is dirty.
    engine.null_rate.side_effect = [0.0, 0.0, 0.5]
    r = quality.completeness(users_config, engine)
    assert r.status == "WARNING"
    assert "created_at" in r.message


def test_completeness_checks_every_critical_column(users_config):
    engine = MagicMock(spec=TableEngine)
    engine.null_rate.return_value = 0.0
    quality.completeness(users_config, engine)
    columns_checked = [c.args[1] for c in engine.null_rate.call_args_list]
    assert columns_checked == users_config.critical_columns


def test_consistency_ok_when_no_duplicates(users_config):
    r = quality.consistency(users_config, _engine(count_duplicates=0))
    assert r.status == "OK"


def test_consistency_critical_when_duplicates(users_config):
    r = quality.consistency(users_config, _engine(count_duplicates=3))
    assert r.status == "CRITICAL"
    assert "3" in r.message
