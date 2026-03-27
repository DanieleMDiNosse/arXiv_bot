"""Tests for the per-user feedback daily limit."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import arXiv_bot as bot


def _utc_datetime(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def test_feedback_limit_is_capped_and_persisted(tmp_path, monkeypatch) -> None:
    """Successful feedback submissions should stop at the daily cap."""
    settings_path = tmp_path / "bot_settings.json"
    monkeypatch.setattr(bot, "SETTINGS_FILE", settings_path)

    user_id = 123
    user_data: dict[str, object] = {}
    now = _utc_datetime(2026, 3, 27, 12)

    for _ in range(bot.FEEDBACK_DAILY_LIMIT + 2):
        bot._record_feedback_submission(user_id, user_data, now=now)

    assert bot._remaining_feedback_slots(user_id, user_data, now=now) == 0

    stored = json.loads(settings_path.read_text(encoding="utf-8"))
    assert stored["users"][str(user_id)]["feedback_daily_usage"] == {
        "date": "2026-03-27",
        "count": bot.FEEDBACK_DAILY_LIMIT,
    }


def test_feedback_limit_resets_on_next_utc_day(tmp_path, monkeypatch) -> None:
    """A stored count from a previous UTC day should not reduce today's quota."""
    settings_path = tmp_path / "bot_settings.json"
    monkeypatch.setattr(bot, "SETTINGS_FILE", settings_path)

    user_id = 456
    yesterday = _utc_datetime(2026, 3, 27, 23)
    today = _utc_datetime(2026, 3, 28, 0)

    for _ in range(3):
        bot._record_feedback_submission(user_id, now=yesterday)

    fresh_user_data: dict[str, object] = {}
    assert bot._remaining_feedback_slots(user_id, fresh_user_data, now=today) == bot.FEEDBACK_DAILY_LIMIT
    assert fresh_user_data["feedback_daily_usage"] == {"date": "2026-03-28", "count": 0}
