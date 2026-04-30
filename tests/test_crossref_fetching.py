from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import requests

import arXiv_bot as bot


class FakeCrossrefResponse:
    def __init__(
        self,
        status_code: int,
        *,
        payload: dict[str, Any] | None = None,
        url: str = "https://api.crossref.org/works",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload or {"message": {"items": []}}
        self.url = url
        self.headers = headers or {}

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


def _crossref_item(doi: str, *, published: datetime, updated: datetime | None = None, title: str = "Paper") -> dict[str, Any]:
    updated_at = updated or published
    return {
        "DOI": doi,
        "title": [title],
        "abstract": "Quantum finance and market microstructure",
        "author": [{"given": "Ada", "family": "Lovelace"}],
        "created": {"date-time": published.isoformat()},
        "indexed": {"date-time": updated_at.isoformat()},
        "URL": f"https://doi.org/{doi}",
        "container-title": ["IEEE"],
    }


def test_fetch_crossref_preprint_papers_retries_after_rate_limit(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    sleep_calls: list[int] = []
    responses = [
        FakeCrossrefResponse(429, headers={"Retry-After": "9"}),
        FakeCrossrefResponse(
            200,
            payload={"message": {"items": []}},
            url="https://api.crossref.org/works?page=ok",
        ),
    ]

    def fake_get(url: str, *, params: dict[str, Any], timeout: int, headers: dict[str, str]):
        calls.append(
            {
                "url": url,
                "params": dict(params),
                "timeout": timeout,
                "headers": dict(headers),
            }
        )
        return responses.pop(0)

    monkeypatch.setattr(bot, "CROSSREF_RATE_LIMIT_RETRIES", 2)
    monkeypatch.setattr(bot, "CROSSREF_CURSOR_DELAY_SECONDS", 0.0)
    monkeypatch.setattr(bot.requests, "get", fake_get)
    monkeypatch.setattr(bot.systime, "sleep", sleep_calls.append)

    papers, request_url, raw_count = bot.fetch_crossref_preprint_papers(
        source=bot.SOURCE_IEEE,
        doi_prefix="10.1109",
        keywords=["quantum"],
        query_text="quantum",
        max_results=10,
    )

    assert papers == []
    assert request_url == "https://api.crossref.org/works?page=ok"
    assert raw_count == 0
    assert len(calls) == 2
    assert sleep_calls == [9]
    assert all(call["timeout"] == bot.CROSSREF_REQUEST_TIMEOUT for call in calls)


def test_fetch_crossref_preprint_papers_stops_when_page_is_older_than_cutoff(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    calls: list[dict[str, Any]] = []
    sleep_calls: list[float] = []
    responses = [
        FakeCrossrefResponse(
            200,
            payload={
                "message": {
                    "items": [
                        _crossref_item(
                            "10.1109/older-paper",
                            published=now - timedelta(hours=36),
                            updated=now - timedelta(hours=30),
                            title="Old IEEE paper",
                        )
                    ],
                    "next-cursor": "next-page",
                }
            },
            url="https://api.crossref.org/works?page=1",
        )
    ]

    def fake_get(url: str, *, params: dict[str, Any], timeout: int, headers: dict[str, str]):
        calls.append({"url": url, "params": dict(params)})
        return responses.pop(0)

    monkeypatch.setattr(bot, "CROSSREF_CURSOR_DELAY_SECONDS", 1.0)
    monkeypatch.setattr(bot.requests, "get", fake_get)
    monkeypatch.setattr(bot.systime, "sleep", sleep_calls.append)

    papers, request_url, raw_count = bot.fetch_crossref_preprint_papers(
        source=bot.SOURCE_IEEE,
        doi_prefix="10.1109",
        keywords=["quantum"],
        hours_back=24,
        query_text="quantum",
        max_results=10,
    )

    assert papers == []
    assert request_url == "https://api.crossref.org/works?page=1"
    assert raw_count == 1
    assert len(calls) == 1
    assert sleep_calls == []


def test_fetch_crossref_preprint_papers_returns_partial_results_after_later_rate_limit(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    calls: list[dict[str, Any]] = []
    responses = [
        FakeCrossrefResponse(
            200,
            payload={
                "message": {
                    "items": [
                        _crossref_item(
                            "10.1109/recent-paper",
                            published=now - timedelta(hours=6),
                            updated=now - timedelta(hours=2),
                            title="Quantum finance in IEEE",
                        )
                    ],
                    "next-cursor": "next-page",
                }
            },
            url="https://api.crossref.org/works?page=1",
        ),
        FakeCrossrefResponse(429, url="https://api.crossref.org/works?page=2"),
    ]

    def fake_get(url: str, *, params: dict[str, Any], timeout: int, headers: dict[str, str]):
        calls.append({"url": url, "params": dict(params)})
        return responses.pop(0)

    monkeypatch.setattr(bot, "CROSSREF_RATE_LIMIT_RETRIES", 1)
    monkeypatch.setattr(bot, "CROSSREF_CURSOR_DELAY_SECONDS", 0.0)
    monkeypatch.setattr(bot.requests, "get", fake_get)

    papers, request_url, raw_count = bot.fetch_crossref_preprint_papers(
        source=bot.SOURCE_IEEE,
        doi_prefix="10.1109",
        keywords=["quantum"],
        hours_back=24,
        query_text="quantum",
        max_results=10,
    )

    assert len(papers) == 1
    assert papers[0].arxiv_id == "10.1109/recent-paper"
    assert papers[0].index == 1
    assert request_url == "https://api.crossref.org/works?page=1"
    assert raw_count == 1
    assert len(calls) == 2
