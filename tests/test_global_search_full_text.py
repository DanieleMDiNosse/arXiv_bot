from datetime import datetime, timezone

import requests

import arXiv_bot as bot
from arXiv_bot import (
    Paper,
    SOURCE_ARXIV,
    _format_source_fetch_error,
    _openalex_item_to_preprint_paper,
    build_arxiv_full_text_query,
    build_pubmed_full_text_query,
    fetch_arxiv_papers_by_text,
    parse_full_text_search_input,
)


def test_parse_full_text_search_keeps_single_query_string() -> None:
    assert (
        parse_full_text_search_input("Attention Is All You Need, transformers + sequence modeling")
        == "Attention Is All You Need, transformers + sequence modeling"
    )


def test_parse_full_text_search_strips_outer_quotes() -> None:
    assert parse_full_text_search_input('"Attention Is All You Need"') == "Attention Is All You Need"


def test_build_arxiv_full_text_query_uses_whole_phrase() -> None:
    assert build_arxiv_full_text_query("Attention Is All You Need") == (
        '(ti:"Attention Is All You Need" OR abs:"Attention Is All You Need" OR '
        'all:"Attention Is All You Need")'
    )


def test_build_pubmed_full_text_query_uses_whole_phrase() -> None:
    assert build_pubmed_full_text_query("Attention Is All You Need") == (
        '("Attention Is All You Need"[Title/Abstract] OR '
        '"Attention Is All You Need"[Text Word])'
    )


def test_format_source_fetch_error_reports_rate_limit() -> None:
    response = requests.Response()
    response.status_code = 429

    exc = requests.HTTPError(response=response)

    assert _format_source_fetch_error(SOURCE_ARXIV, exc) == (
        "arXiv is rate-limiting requests right now. Try again in a minute."
    )


def test_format_source_fetch_error_reports_other_http_status() -> None:
    response = requests.Response()
    response.status_code = 503

    exc = requests.HTTPError(response=response)

    assert _format_source_fetch_error(SOURCE_ARXIV, exc) == "arXiv request failed with HTTP 503."


def test_openalex_item_to_preprint_paper_accepts_arxiv_without_doi() -> None:
    item = {
        "doi": None,
        "title": "Deviations from Tradition: Stylized Facts in the Era of DeFi",
        "publication_date": "2025-10-26",
        "updated_date": "2025-10-29",
        "abstract_inverted_index": {"Stylized": [0], "facts": [1]},
        "authorships": [{"author": {"display_name": "Daniele Maria Di Nosse"}}],
        "primary_topic": {"display_name": "Finance"},
        "primary_location": {
            "landing_page_url": "http://arxiv.org/abs/2510.22834",
            "pdf_url": "https://arxiv.org/pdf/2510.22834",
            "source": {"display_name": "ArXiv.org", "id": "https://openalex.org/S4393918464"},
        },
        "best_oa_location": {},
    }

    paper = _openalex_item_to_preprint_paper(item, source=SOURCE_ARXIV)

    assert paper is not None
    assert paper.arxiv_id == "2510.22834"
    assert paper.link_abs == "https://arxiv.org/abs/2510.22834"
    assert paper.link_pdf == "https://arxiv.org/pdf/2510.22834.pdf"
    assert paper.source == SOURCE_ARXIV


def test_fetch_arxiv_papers_by_text_uses_openalex_title_fallback_on_rate_limit(monkeypatch) -> None:
    response = requests.Response()
    response.status_code = 429
    rate_limit_error = requests.HTTPError(response=response)

    expected_paper = Paper(
        index=0,
        arxiv_id="2510.22834",
        title="Deviations from Tradition: Stylized Facts in the Era of DeFi",
        summary="",
        authors=["Daniele Maria Di Nosse"],
        published=datetime(2025, 10, 26, tzinfo=timezone.utc),
        updated=datetime(2025, 10, 26, tzinfo=timezone.utc),
        published_raw="2025-10-26",
        updated_raw="2025-10-26",
        primary_category="Finance",
        link_abs="https://arxiv.org/abs/2510.22834",
        link_pdf="https://arxiv.org/pdf/2510.22834.pdf",
        source=SOURCE_ARXIV,
    )

    def fake_fetch_arxiv_entries(*args, **kwargs):
        raise rate_limit_error

    def fake_openalex_fallback(query_text: str, max_results: int):
        assert query_text == "Deviations from Tradition: Stylized Facts in the Era of DeFi"
        assert max_results == bot.ARXIV_FULL_TEXT_MAX_RESULTS
        return [expected_paper], "https://api.openalex.org/autocomplete/works?q=Deviations", 1

    monkeypatch.setattr(bot, "fetch_arxiv_entries", fake_fetch_arxiv_entries)
    monkeypatch.setattr(bot, "fetch_openalex_arxiv_title_fallback_papers", fake_openalex_fallback)

    papers, request_url, raw_count = fetch_arxiv_papers_by_text(
        "Deviations from Tradition: Stylized Facts in the Era of DeFi"
    )

    assert papers == [expected_paper]
    assert request_url == "https://api.openalex.org/autocomplete/works?q=Deviations"
    assert raw_count == 1
