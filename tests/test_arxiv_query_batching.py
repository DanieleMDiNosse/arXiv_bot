from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import arXiv_bot as bot


def _entry(arxiv_id: str, *, published: datetime, updated: datetime | None = None) -> SimpleNamespace:
    updated_at = updated or published
    return SimpleNamespace(
        id=f"https://arxiv.org/abs/{arxiv_id}",
        title=f"Paper {arxiv_id}",
        summary="Abstract",
        authors=[SimpleNamespace(name="Author")],
        published=published.isoformat(),
        updated=updated_at.isoformat(),
        links=[SimpleNamespace(href=f"https://arxiv.org/pdf/{arxiv_id}.pdf", title="pdf")],
        tags=[{"term": "cs.AI"}],
    )


def test_fetch_arxiv_papers_batches_large_keyword_queries(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    calls: list[str] = []
    requested_max_results: list[int] = []
    sleep_calls: list[float] = []

    entry_a = _entry("2501.00001", published=now - timedelta(hours=1), updated=now - timedelta(minutes=30))
    entry_b = _entry("2501.00002", published=now - timedelta(hours=2), updated=now - timedelta(hours=1))
    entry_c = _entry("2501.00003", published=now - timedelta(hours=3), updated=now - timedelta(hours=2))

    def fake_fetch_arxiv_entries(
        query: str,
        max_results: int = bot.DEFAULT_MAX_RESULTS,
        sort_by: str = "submittedDate",
        request_timeout=None,
        rate_limit_retries=None,
    ):
        calls.append(query)
        requested_max_results.append(max_results)
        assert sort_by == "lastUpdatedDate"
        if 'ti:"alpha"' in query:
            return [entry_a, entry_b], "https://export.arxiv.org/api/query?batch=1"
        return [entry_a, entry_c], "https://export.arxiv.org/api/query?batch=2"

    monkeypatch.setattr(bot, "ARXIV_QUERY_BATCH_MAX_KEYWORDS", 2)
    monkeypatch.setattr(bot, "ARXIV_KEYWORD_FETCH_MAX_RESULTS", 120)
    monkeypatch.setattr(bot, "ARXIV_QUERY_BATCH_DELAY_SECONDS", 1.5)
    monkeypatch.setattr(bot, "fetch_arxiv_entries", fake_fetch_arxiv_entries)
    monkeypatch.setattr(bot.systime, "sleep", sleep_calls.append)

    papers, request_url, raw_count = bot.fetch_arxiv_papers(
        ["alpha", "beta", "gamma", "delta"],
        hours_back=24,
        max_results=bot.DEFAULT_MAX_RESULTS,
        sort_by="lastUpdatedDate",
        use_updated=True,
    )

    assert len(calls) == 2
    assert all(" OR " in query for query in calls)
    assert requested_max_results == [120, 120]
    assert sleep_calls == [1.5]
    assert [paper.arxiv_id for paper in papers] == ["2501.00001", "2501.00002", "2501.00003"]
    assert [paper.index for paper in papers] == [1, 2, 3]
    assert "https://export.arxiv.org/api/query?batch=1" in request_url
    assert "https://export.arxiv.org/api/query?batch=2" in request_url
    assert raw_count == 4
