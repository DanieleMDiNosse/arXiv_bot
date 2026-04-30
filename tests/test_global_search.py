"""Tests for Global Search menu helpers and cache scope handling."""

from __future__ import annotations

from types import SimpleNamespace

import arXiv_bot as bot


def test_global_search_source_picker_starts_unselected() -> None:
    """The source picker should expose every source and control action."""
    markup = bot.build_global_search_sources_markup([])
    rows = markup.inline_keyboard

    assert [button.text for button in rows[0]] == ["arXiv", "bioRxiv"]
    assert [button.text for button in rows[1]] == ["medRxiv", "ChemRxiv"]
    assert [button.text for button in rows[2]] == ["SSRN", "IEEE"]
    assert [button.text for button in rows[3]] == ["PubMed"]
    assert [button.text for button in rows[4]] == ["Select All", "Clear All"]
    assert [button.text for button in rows[5]] == ["Search", "Cancel"]


def test_global_search_source_picker_marks_selected_sources() -> None:
    """Selected sources should be visibly marked in the picker."""
    markup = bot.build_global_search_sources_markup([bot.SOURCE_ARXIV, bot.SOURCE_PUBMED])
    buttons = [button for row in markup.inline_keyboard[:4] for button in row]
    labels = [button.text for button in buttons]

    assert "✅ arXiv" in labels
    assert "✅ PubMed" in labels
    assert "bioRxiv" in labels


def test_keyword_source_picker_marks_selected_sources_and_controls() -> None:
    """Keyword source selection should support multi-select before continuing."""
    markup = bot.build_keyword_scope_markup("add", [bot.SOURCE_ARXIV, bot.SOURCE_SSRN])
    rows = markup.inline_keyboard

    assert [button.text for button in rows[0]] == ["✅ arXiv", "bioRxiv"]
    assert [button.text for button in rows[2]] == ["✅ SSRN", "IEEE"]
    assert [button.text for button in rows[4]] == ["Select All", "Clear All"]
    assert [button.text for button in rows[5]] == ["Continue", "Cancel"]
    assert rows[5][0].callback_data == "kwmenu:start:add"


def test_global_scope_description_and_pagination_keep_zero_hours() -> None:
    """Global searches should use an all-time label and preserve 0h in callbacks."""
    markup = bot.build_more_results_markup(
        scope=bot.SEARCH_SCOPE_GLOBAL,
        hours_back=0,
        offset=10,
        results_token=7,
    )
    callback_data = markup.inline_keyboard[0][0].callback_data

    assert bot._normalize_search_scope("global") == bot.SEARCH_SCOPE_GLOBAL
    assert bot._describe_search_window(bot.SEARCH_SCOPE_GLOBAL, 0) == "all time"
    assert callback_data == "more_results:7:global:0:10"


def test_get_cached_papers_matches_global_scope_with_zero_hours() -> None:
    """Global cached results should be retrievable with the dedicated scope."""
    context = SimpleNamespace(
        user_data={
            "papers": ["paper-a"],
            "cache_scope": bot.SEARCH_SCOPE_GLOBAL,
            "cache_hours_back": 0,
        }
    )

    assert bot.get_cached_papers(context, hours_back=0, scope=bot.SEARCH_SCOPE_GLOBAL) == ["paper-a"]
    assert bot.get_cached_papers(context, hours_back=24, scope=bot.SEARCH_SCOPE_TODAY) == []
