from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import requests

import arXiv_bot as bot
from arXiv_bot import Paper, SOURCE_ARXIV


def _paper(arxiv_id: str) -> Paper:
    return Paper(
        index=0,
        arxiv_id=arxiv_id,
        title=f"Paper {arxiv_id}",
        summary="",
        authors=["Author"],
        published=datetime(2025, 1, 1, tzinfo=timezone.utc),
        updated=datetime(2025, 1, 1, tzinfo=timezone.utc),
        published_raw="2025-01-01",
        updated_raw="2025-01-01",
        primary_category="cs.AI",
        link_abs=f"https://arxiv.org/abs/{arxiv_id}",
        link_pdf=f"https://arxiv.org/pdf/{arxiv_id}.pdf",
        source=SOURCE_ARXIV,
    )


def test_fetch_arxiv_entries_by_ids_splits_timed_out_batches(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_fetch_batch(arxiv_ids):
        batch = list(arxiv_ids)
        calls.append(batch)
        if len(batch) > 1:
            raise requests.Timeout("timed out")
        return [f"entry:{batch[0]}"], f"https://export.arxiv.org/api/query?id_list={batch[0]}"

    monkeypatch.setattr(bot, "_fetch_arxiv_entries_by_ids_batch", fake_fetch_batch)

    entries, request_url, failed_ids = bot.fetch_arxiv_entries_by_ids(["2501.00001", "2501.00002", "2501.00003"])

    assert calls == [
        ["2501.00001", "2501.00002", "2501.00003"],
        ["2501.00001"],
        ["2501.00002", "2501.00003"],
        ["2501.00002"],
        ["2501.00003"],
    ]
    assert entries == [
        "entry:2501.00001",
        "entry:2501.00002",
        "entry:2501.00003",
    ]
    assert request_url == (
        "https://export.arxiv.org/api/query?id_list=2501.00001"
        " | https://export.arxiv.org/api/query?id_list=2501.00002"
        " | https://export.arxiv.org/api/query?id_list=2501.00003"
    )
    assert failed_ids == []


def test_fetch_papers_by_arxiv_ids_returns_partial_results_for_failed_requests(monkeypatch) -> None:
    entry = object()

    def fake_fetch(arxiv_ids):
        assert list(arxiv_ids) == ["2501.00001", "2501.00002"]
        return [entry], "https://export.arxiv.org/api/query?id_list=2501.00001", ["2501.00002"]

    def fake_entry_to_paper(raw_entry, index):
        assert raw_entry is entry
        assert index == 0
        return _paper("2501.00001")

    monkeypatch.setattr(bot, "fetch_arxiv_entries_by_ids", fake_fetch)
    monkeypatch.setattr(bot, "entry_to_paper", fake_entry_to_paper)

    papers, missing = asyncio.run(bot.fetch_papers_by_arxiv_ids(["2501.00001", "2501.00002"]))

    assert [paper.arxiv_id for paper in papers] == ["2501.00001"]
    assert papers[0].index == 1
    assert missing == ["2501.00002"]


def test_set_bookmarks_persists_saved_paper_snapshot(tmp_path, monkeypatch) -> None:
    settings_path = tmp_path / "bot_settings.json"
    monkeypatch.setattr(bot, "SETTINGS_FILE", settings_path)

    user_id = 123
    paper = _paper("2501.00001")
    paper_ref = bot.paper_ref_for(paper)

    saved_refs = bot.set_bookmarks(
        user_id,
        [paper_ref],
        papers_by_ref={paper_ref: paper},
    )

    assert saved_refs == [paper_ref]

    stored = json.loads(settings_path.read_text(encoding="utf-8"))
    bookmark_entry = stored["users"][str(user_id)]["bookmarks"][0]
    assert bookmark_entry["ref"] == paper_ref
    assert bookmark_entry["paper"]["title"] == paper.title
    assert bookmark_entry["paper"]["link_pdf"] == paper.link_pdf

    restored = bot.get_bookmarked_papers(user_id=user_id)
    assert [saved_paper.arxiv_id for saved_paper in restored] == [paper.arxiv_id]
    assert restored[0].title == paper.title


def test_bookmarks_cmd_uses_saved_snapshot_without_refetch(monkeypatch) -> None:
    paper = _paper("2501.00003")
    paper_ref = bot.paper_ref_for(paper)
    replies: list[tuple[str, dict[str, object]]] = []

    async def fake_reply_text(text: str, **kwargs) -> None:
        replies.append((text, kwargs))

    async def fail_fetch(_paper_refs):
        raise AssertionError("bookmarks_cmd should not re-fetch saved bookmarks")

    monkeypatch.setattr(bot, "fetch_papers_by_refs", fail_fetch)

    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=321),
        message=SimpleNamespace(reply_text=fake_reply_text),
    )
    context = SimpleNamespace(
        user_data={
            "bookmarks": [
                {
                    "ref": paper_ref,
                    "paper": bot._serialize_bookmark_paper(paper),
                }
            ],
            "papers": [],
        }
    )

    asyncio.run(bot.bookmarks_cmd(update, context))

    assert len(replies) == 1
    assert paper.title in replies[0][0]


def test_bookmarks_cmd_skips_legacy_id_only_bookmarks_without_refetch(monkeypatch) -> None:
    replies: list[tuple[str, dict[str, object]]] = []

    async def fake_reply_text(text: str, **kwargs) -> None:
        replies.append((text, kwargs))

    async def fail_fetch(_paper_refs):
        raise AssertionError("bookmarks_cmd should not re-fetch legacy bookmarks")

    monkeypatch.setattr(bot, "fetch_papers_by_refs", fail_fetch)

    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=654),
        message=SimpleNamespace(reply_text=fake_reply_text),
    )
    context = SimpleNamespace(
        user_data={
            "bookmarks": ["arxiv:2511.16652v2"],
            "papers": [],
        }
    )

    asyncio.run(bot.bookmarks_cmd(update, context))

    assert len(replies) == 1
    assert replies[0][0].startswith("No bookmarked papers are available right now.")
    assert "stored as IDs only and were skipped" in replies[0][0]
