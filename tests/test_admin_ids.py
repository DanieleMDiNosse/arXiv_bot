"""Tests for admin user ID parsing."""

from __future__ import annotations

import arXiv_bot as bot


def test_report_admin_user_ids_accept_multiple_values(monkeypatch) -> None:
    """Comma-separated admin IDs should all be accepted."""
    monkeypatch.setattr(bot, "REPORT_ADMIN_USER_ID", "111, 222;333")

    assert bot.get_report_admin_user_ids() == [111, 222, 333]
    assert bot.get_report_admin_user_id() == 111
    assert bot._is_admin_user(222)
    assert not bot._is_admin_user(444)


def test_report_admin_user_ids_invalid_values_are_ignored(monkeypatch) -> None:
    """Invalid tokens should be skipped without breaking valid IDs."""
    monkeypatch.setattr(bot, "REPORT_ADMIN_USER_ID", "abc 555")

    assert bot.get_report_admin_user_ids() == [555]
    assert bot._is_admin_user(555)
