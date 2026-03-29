from arXiv_bot import (
    build_arxiv_full_text_query,
    build_pubmed_full_text_query,
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
