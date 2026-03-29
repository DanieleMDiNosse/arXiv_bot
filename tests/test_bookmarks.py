from __future__ import annotations

import asyncio
from datetime import datetime, timezone

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
