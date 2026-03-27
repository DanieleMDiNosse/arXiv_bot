"""Tests for reply and inline menu layout."""

from __future__ import annotations

import arXiv_bot as bot


def test_main_menu_uses_more_button_for_secondary_actions() -> None:
    """The reply keyboard should expose one More button instead of two actions."""
    markup = bot.build_main_menu_markup()
    labels = [button.text for row in markup.keyboard for button in row]

    assert bot.MENU_BTN_MORE in labels
    assert bot.MENU_BTN_REPORT not in labels
    assert bot.MENU_BTN_COFFEE not in labels


def test_more_menu_contains_feedback_and_coffee_actions() -> None:
    """The More submenu should expose feedback and coffee options."""
    markup = bot.build_more_menu_markup()
    buttons = [button for row in markup.inline_keyboard for button in row]

    assert [button.text for button in buttons] == [bot.MENU_BTN_REPORT, bot.MENU_BTN_COFFEE]
    assert [button.callback_data for button in buttons] == ["moremenu:report", "moremenu:coffee"]
