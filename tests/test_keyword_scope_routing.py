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
