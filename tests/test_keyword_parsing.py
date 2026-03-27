"""Tests for keyword input parsing."""

from __future__ import annotations

import arXiv_bot as bot


def test_parse_keywords_input_accepts_bullet_lists() -> None:
    """Bullet-style multiline input should become one keyword per line."""
    raw = "- keyword1\n- keyword2\n- keyword3 + keyword4"

    parsed = bot.parse_keywords_input(raw)

    assert parsed == ["keyword1", "keyword2", "keyword3 + keyword4"]


def test_parse_keywords_input_keeps_existing_comma_behavior() -> None:
    """Comma-separated input should still deduplicate and normalize '+' spacing."""
    raw = "keyword1, keyword2+keyword3, keyword1"

    parsed = bot.parse_keywords_input(raw)

    assert parsed == ["keyword1", "keyword2 + keyword3"]
