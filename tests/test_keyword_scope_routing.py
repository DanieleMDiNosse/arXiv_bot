from __future__ import annotations

import asyncio
from types import SimpleNamespace

import arXiv_bot as bot


class _FakeMessage:
    def __init__(self, text: str) -> None:
        self.text = text
        self.replies: list[str] = []

    async def reply_text(self, text: str, *args, **kwargs) -> None:
        self.replies.append(text)


class _FakeCallbackQuery:
    def __init__(self, data: str) -> None:
        self.data = data
        self.message = SimpleNamespace(reply_markup=None)
        self.answers: list[tuple[str | None, bool]] = []
        self.edited_text: list[str] = []

    async def answer(self, text: str | None = None, show_alert: bool = False) -> None:
        self.answers.append((text, show_alert))

    async def edit_message_text(self, text: str, *args, **kwargs) -> None:
        self.edited_text.append(text)

    async def edit_message_reply_markup(self, *args, **kwargs) -> None:
        return None


def test_menu_text_router_prioritizes_selected_keyword_source(monkeypatch) -> None:
    routed: list[tuple[str, str, str]] = []

    async def fake_apply_keywords_input(update, context, raw):
        routed.append(("all", raw, "all"))

    async def fake_apply_keywords_input_for_source(update, context, raw, source, mode):
        routed.append((mode, raw, source))

    monkeypatch.setattr(bot, "apply_keywords_input", fake_apply_keywords_input)
    monkeypatch.setattr(bot, "apply_keywords_input_for_source", fake_apply_keywords_input_for_source)

    update = SimpleNamespace(
        message=_FakeMessage("market microstructure"),
        effective_user=SimpleNamespace(id=373851206),
        effective_chat=SimpleNamespace(id=373851206),
    )
    context = SimpleNamespace(
        user_data={
            "awaiting_keywords_input": True,
            "awaiting_add_keyword_source": bot.SOURCE_SSRN,
        }
    )

    asyncio.run(bot.menu_text_router(update, context))

    assert routed == [("add", "market microstructure", bot.SOURCE_SSRN)]
    assert "awaiting_add_keyword_source" not in context.user_data
    assert "awaiting_keywords_input" not in context.user_data


def test_menu_text_router_prioritizes_selected_keyword_sources(monkeypatch) -> None:
    routed: list[tuple[str, str, tuple[str, ...]]] = []

    async def fake_apply_keywords_input(update, context, raw):
        raise AssertionError("single-scope keyword routing should not run")

    async def fake_apply_keywords_input_for_source(update, context, raw, source, mode):
        routed.append((mode, raw, tuple(source)))

    monkeypatch.setattr(bot, "apply_keywords_input", fake_apply_keywords_input)
    monkeypatch.setattr(bot, "apply_keywords_input_for_source", fake_apply_keywords_input_for_source)

    update = SimpleNamespace(
        message=_FakeMessage("market microstructure"),
        effective_user=SimpleNamespace(id=373851206),
        effective_chat=SimpleNamespace(id=373851206),
    )
    context = SimpleNamespace(
        user_data={
            "awaiting_add_keyword_sources": [bot.SOURCE_SSRN, bot.SOURCE_IEEE],
        }
    )

    asyncio.run(bot.menu_text_router(update, context))

    assert routed == [("add", "market microstructure", (bot.SOURCE_SSRN, bot.SOURCE_IEEE))]
    assert "awaiting_add_keyword_sources" not in context.user_data


def test_keyword_scope_callback_waits_for_continue_before_prompting(monkeypatch) -> None:
    prompted: list[tuple[str, ...]] = []

    async def fake_prompt_add_keyword_for_sources(update, context, sources):
        prompted.append(tuple(sources))

    monkeypatch.setattr(bot, "prompt_add_keyword_for_sources", fake_prompt_add_keyword_for_sources)

    context = SimpleNamespace(user_data={})

    toggle_arxiv = _FakeCallbackQuery("kwmenu:toggle:add:arxiv")
    update = SimpleNamespace(
        callback_query=toggle_arxiv,
        effective_chat=SimpleNamespace(id=373851206),
        message=None,
    )
    asyncio.run(bot.keyword_scope_callback(update, context))

    toggle_ssrn = _FakeCallbackQuery("kwmenu:toggle:add:ssrn")
    update.callback_query = toggle_ssrn
    asyncio.run(bot.keyword_scope_callback(update, context))

    start = _FakeCallbackQuery("kwmenu:start:add")
    update.callback_query = start
    asyncio.run(bot.keyword_scope_callback(update, context))

    assert prompted == [(bot.SOURCE_ARXIV, bot.SOURCE_SSRN)]
    assert context.user_data[bot._keyword_scope_state_key("add")] == [
        bot.SOURCE_ARXIV,
        bot.SOURCE_SSRN,
    ]


def test_apply_keywords_input_for_source_add_does_not_refresh(monkeypatch) -> None:
    stored_keywords: dict[str, list[str]] = {bot.SOURCE_ARXIV: []}

    def fake_get_keywords_for_source(source, user_data=None, user_id=None):
        return list(stored_keywords.get(source, []))

    def fake_set_keywords_for_source(*, user_id, source, keywords, user_data=None):
        stored_keywords[source] = list(keywords)
        return list(keywords)

    async def fail_refresh_cache(*args, **kwargs):
        raise AssertionError("add keyword should not trigger a refresh")

    monkeypatch.setattr(bot, "get_keywords_for_source", fake_get_keywords_for_source)
    monkeypatch.setattr(bot, "set_keywords_for_source", fake_set_keywords_for_source)
    monkeypatch.setattr(bot, "refresh_cache", fail_refresh_cache)

    message = _FakeMessage("graph neural networks")
    update = SimpleNamespace(
        message=message,
        effective_user=SimpleNamespace(id=373851206),
        effective_chat=SimpleNamespace(id=373851206),
    )
    context = SimpleNamespace(user_data={})

    success = asyncio.run(
        bot.apply_keywords_input_for_source(
            update,
            context,
            raw="graph neural networks",
            source=bot.SOURCE_ARXIV,
            mode="add",
        )
    )

    assert success is True
    assert stored_keywords[bot.SOURCE_ARXIV] == ["graph neural networks"]
    assert len(message.replies) == 1
    assert "Keywords Updated" in message.replies[0]
    assert "matching paper(s)" not in message.replies[0]
