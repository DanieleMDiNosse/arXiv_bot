"""Tests for persistent user metrics tracking."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import arXiv_bot as bot


def _utc_datetime(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def test_user_metrics_summary_tracks_total_dau_wau_and_mau(tmp_path, monkeypatch) -> None:
    """Repeated same-day activity should not inflate DAU/WAU/MAU counts."""
    metrics_path = tmp_path / "bot_metrics.sqlite3"
    monkeypatch.setattr(bot, "METRICS_DB_FILE", metrics_path)

    bot._initialize_metrics_db()
    bot._record_user_interaction(1, now=_utc_datetime(2026, 3, 28, 9))
    bot._record_user_interaction(1, now=_utc_datetime(2026, 3, 28, 18))
    bot._record_user_interaction(2, now=_utc_datetime(2026, 3, 23, 12))
    bot._record_user_interaction(3, now=_utc_datetime(2026, 2, 28, 12))

    summary = bot._get_user_metrics_summary(now=_utc_datetime(2026, 3, 28, 23))

    assert summary["total_users"] == 3
    assert summary["daily_active_users"] == 1
    assert summary["weekly_active_users"] == 2
    assert summary["monthly_active_users"] == 3
    assert summary["recap_enabled_users"] == 0


def test_user_metrics_syncs_existing_settings_and_recap_flags(tmp_path, monkeypatch) -> None:
    """Startup backfill should register existing settings users and recap flags."""
    metrics_path = tmp_path / "bot_metrics.sqlite3"
    settings_path = tmp_path / "bot_settings.json"
    monkeypatch.setattr(bot, "METRICS_DB_FILE", metrics_path)
    monkeypatch.setattr(bot, "SETTINGS_FILE", settings_path)

    settings_path.write_text(
        json.dumps(
            {
                "users": {
                    "10": {
                        bot.WELCOME_SHOWN_AT_KEY: "2026-03-01T09:00:00+00:00",
                        "daily_recap_enabled": True,
                        "daily_recap_chat_id": 1001,
                    },
                    "20": {
                        bot.WELCOME_SHOWN_AT_KEY: "2026-03-05T09:00:00+00:00",
                        "daily_recap_enabled": False,
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    bot._initialize_metrics_db()
    bot._sync_metrics_users_from_settings()

    summary = bot._get_user_metrics_summary(now=_utc_datetime(2026, 3, 28, 12))

    assert summary["total_users"] == 2
    assert summary["daily_active_users"] == 0
    assert summary["weekly_active_users"] == 0
    assert summary["monthly_active_users"] == 0
    assert summary["recap_enabled_users"] == 1
