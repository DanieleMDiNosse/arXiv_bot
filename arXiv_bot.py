#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Optional, Sequence

import feedparser
import requests
from dateutil import parser as dateparser
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

ARXIV_API_URL = "http://export.arxiv.org/api/query"
DEFAULT_MAX_RESULTS = int(os.getenv("MAX_RESULTS", "300"))
DEFAULT_HOURS_BACK = int(os.getenv("ARXIV_HOURS_BACK", "72"))
SETTINGS_FILE = Path("bot_settings.json")


@dataclass
class Paper:
    index: int
    arxiv_id: str
    title: str
    summary: str
    authors: List[str]
    published: datetime
    updated: datetime
    published_raw: str
    updated_raw: str
    primary_category: str
    link_abs: str
    link_pdf: str


def load_settings() -> dict[str, Any]:
    if SETTINGS_FILE.exists():
        try:
            return json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("Could not read settings file")
    return {}


def save_settings(data: dict[str, Any]) -> None:
    SETTINGS_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _get_user_settings(settings: dict[str, Any], user_id: int) -> dict[str, Any]:
    users = settings.get("users", {})
    if not isinstance(users, dict):
        return {}

    user_settings = users.get(str(user_id), {})
    if not isinstance(user_settings, dict):
        return {}
    return user_settings


def _save_user_setting(user_id: int, key: str, value: Any) -> None:
    settings = load_settings()
    users = settings.get("users")
    if not isinstance(users, dict):
        users = {}
        settings["users"] = users

    user_settings = users.get(str(user_id))
    if not isinstance(user_settings, dict):
        user_settings = {}
        users[str(user_id)] = user_settings

    if value is None:
        user_settings.pop(key, None)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        user_settings[key] = list(value)
    else:
        user_settings[key] = value

    if not user_settings:
        users.pop(str(user_id), None)

    save_settings(settings)


def _get_user_id(update: Update) -> Optional[int]:
    user = update.effective_user
    if user is None:
        return None
    return int(user.id)


def get_keywords(
    user_data: Optional[dict[str, Any]] = None,
    user_id: Optional[int] = None,
) -> List[str]:
    if user_data is not None and "custom_keywords" in user_data:
        return list(user_data["custom_keywords"])

    settings = load_settings()
    if user_id is not None:
        user_settings = _get_user_settings(settings, user_id)
        if "custom_keywords" in user_settings:
            keywords = list(user_settings["custom_keywords"])
            if user_data is not None:
                user_data["custom_keywords"] = keywords
            return keywords

    if "custom_keywords" in settings:
        return list(settings["custom_keywords"])

    raw = os.getenv("ARXIV_KEYWORDS", "").strip()
    if not raw:
        return []
    return [k.strip() for k in raw.split(",") if k.strip()]


def get_hours_back(bot_data: Optional[dict[str, Any]] = None) -> int:
    if bot_data is not None and "hours_back" in bot_data:
        return int(bot_data["hours_back"])

    settings = load_settings()
    if "hours_back" in settings:
        return int(settings["hours_back"])

    return DEFAULT_HOURS_BACK


def parse_keywords_input(raw: str) -> List[str]:
    raw = raw.strip()
    if not raw:
        return []

    if "," in raw:
        items = [part.strip() for part in raw.split(",")]
    else:
        items = [part.strip() for part in raw.split()]

    cleaned: List[str] = []
    for item in items:
        item = item.strip().strip('"').strip("'").strip()
        if item:
            cleaned.append(item)

    seen: set[str] = set()
    unique: List[str] = []
    for item in cleaned:
        key = item.casefold()
        if key not in seen:
            seen.add(key)
            unique.append(item)

    return unique


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def build_arxiv_query(keywords: Sequence[str]) -> Optional[str]:
    keyword_groups: List[str] = []
    for kw in keywords:
        kw_clean = kw.replace('"', "").strip()
        if not kw_clean:
            continue
        keyword_groups.append(
            f'(ti:"{kw_clean}" OR abs:"{kw_clean}" OR all:"{kw_clean}")'
        )

    if not keyword_groups:
        return None

    return "(" + " OR ".join(keyword_groups) + ")"


def fetch_arxiv_entries(
    query: str,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Any], str]:
    params = {
        "search_query": query,
        "start": 0,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }

    response = requests.get(
        ARXIV_API_URL,
        params=params,
        timeout=30,
        headers={"User-Agent": "telegram-arxiv-codex-bot/1.0"},
    )
    response.raise_for_status()

    feed = feedparser.parse(response.text)
    return list(feed.entries), response.url


def get_primary_category(entry: Any) -> str:
    try:
        tags = getattr(entry, "tags", [])
        if tags:
            return tags[0]["term"]
    except Exception:
        pass
    return ""


def extract_pdf_link(entry: Any) -> str:
    for link in getattr(entry, "links", []):
        href = getattr(link, "href", "")
        title = getattr(link, "title", "")
        if title == "pdf" or href.endswith(".pdf"):
            return href

    entry_id = getattr(entry, "id", "")
    if entry_id:
        return entry_id.replace("/abs/", "/pdf/") + ".pdf"
    return ""


def entries_to_recent_papers(entries: Sequence[Any], hours_back: int) -> List[Paper]:
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=hours_back)

    papers: List[Paper] = []

    for entry in entries:
        try:
            published = ensure_utc(dateparser.parse(getattr(entry, "published")))
            updated = ensure_utc(dateparser.parse(getattr(entry, "updated")))
        except Exception:
            logger.exception("Could not parse entry timestamps")
            continue

        if published < cutoff:
            break

        authors: List[str] = []
        for author in getattr(entry, "authors", []):
            name = getattr(author, "name", "").strip()
            if name:
                authors.append(name)

        entry_id = getattr(entry, "id", "")
        arxiv_id = entry_id.rstrip("/").split("/")[-1] if entry_id else ""

        papers.append(
            Paper(
                index=len(papers) + 1,
                arxiv_id=arxiv_id,
                title=normalize_text(getattr(entry, "title", "")),
                summary=normalize_text(getattr(entry, "summary", "")),
                authors=authors,
                published=published,
                updated=updated,
                published_raw=normalize_text(getattr(entry, "published", "")),
                updated_raw=normalize_text(getattr(entry, "updated", "")),
                primary_category=get_primary_category(entry),
                link_abs=entry_id,
                link_pdf=extract_pdf_link(entry),
            )
        )

    return papers


def format_paper_line(paper: Paper) -> str:
    """Format one paper entry for the `/today` list message.

    Parameters
    ----------
    paper : Paper
        Paper metadata and abstract text to show in Telegram.

    Returns
    -------
    str
        HTML-formatted message fragment for a single paper entry.

    Notes
    -----
    The paper abstract is rendered as an expandable blockquote so the list
    remains compact by default while still exposing the full abstract on tap in
    Telegram clients that support expandable blockquotes.

    Examples
    --------
    >>> paper = Paper(
    ...     index=1,
    ...     arxiv_id="1234.56789",
    ...     title="Example paper",
    ...     summary="This is the abstract.",
    ...     authors=["A. Researcher"],
    ...     published=datetime(2024, 1, 1, tzinfo=timezone.utc),
    ...     updated=datetime(2024, 1, 1, tzinfo=timezone.utc),
    ...     published_raw="",
    ...     updated_raw="",
    ...     primary_category="cs.AI",
    ...     link_abs="https://arxiv.org/abs/1234.56789",
    ...     link_pdf="https://arxiv.org/pdf/1234.56789.pdf",
    ... )
    >>> "<blockquote expandable>" in format_paper_line(paper)
    True
    """
    authors = ", ".join(paper.authors[:3])
    if len(paper.authors) > 3:
        authors += ", et al."

    published_label = paper.published.strftime("%Y-%m-%d %H:%M UTC")
    abstract_html = render_expandable_abstract_html(paper.summary)

    message = (
        f"<b>{paper.index}.</b> {html.escape(paper.title)}\n"
        f"<i>{html.escape(authors)}</i>\n"
        f"<code>{html.escape(paper.arxiv_id)}</code> | "
        f"{html.escape(paper.primary_category)} | {html.escape(published_label)}"
    )
    if abstract_html:
        message += f"\n{abstract_html}"
    return message


def build_paper_reply_markup(paper: Paper) -> Optional[InlineKeyboardMarkup]:
    """Build inline buttons for a paper entry.

    Parameters
    ----------
    paper : Paper
        Paper metadata shown in the `/today` list.

    Returns
    -------
    InlineKeyboardMarkup | None
        Inline keyboard with an arXiv link button and, when available, a PDF
        download callback button. Returns `None` if no buttons can be built.

    Notes
    -----
    Telegram only allows bot-side actions from inline keyboard buttons, not
    from HTML links embedded in the message text. The PDF action is therefore
    implemented as a callback button rather than a clickable text icon.
    """
    buttons: List[InlineKeyboardButton] = []
    if paper.link_abs:
        buttons.append(InlineKeyboardButton(text=paper.link_abs, url=paper.link_abs))
    if paper.link_pdf and paper.arxiv_id:
        buttons.append(
            InlineKeyboardButton(text="📄", callback_data=f"pdf:{paper.arxiv_id}")
        )

    if not buttons:
        return None
    return InlineKeyboardMarkup([buttons])


def find_cached_paper_by_arxiv_id(
    context: ContextTypes.DEFAULT_TYPE,
    arxiv_id: str,
) -> Optional[Paper]:
    """Return a cached paper matching a given arXiv identifier.

    Parameters
    ----------
    context : ContextTypes.DEFAULT_TYPE
        Telegram callback context holding the per-user paper cache.
    arxiv_id : str
        arXiv identifier embedded in the callback payload.

    Returns
    -------
    Paper | None
        Matching cached paper if present, otherwise `None`.

    Notes
    -----
    Callback buttons store a stable arXiv identifier rather than a transient
    list index. This avoids downloading the wrong paper if the cached list is
    refreshed between `/today` and the button press.
    """
    for paper in get_cached_papers(context):
        if paper.arxiv_id == arxiv_id:
            return paper
    return None


def render_expandable_abstract_html(abstract: str) -> str:
    """Render an abstract as a Telegram expandable HTML blockquote.

    Parameters
    ----------
    abstract : str
        Raw abstract text from arXiv.

    Returns
    -------
    str
        Escaped HTML snippet that Telegram can render as a collapsed abstract
        preview with tap-to-expand behavior. Returns an empty string if the
        abstract is missing.

    Notes
    -----
    Telegram HTML parse mode supports `<blockquote expandable>...</blockquote>`.
    This keeps the paper list readable while still making the full abstract
    available in the same message.

    Examples
    --------
    >>> render_expandable_abstract_html("This is an abstract.")
    '<blockquote expandable>This is an abstract.</blockquote>'
    """
    cleaned = normalize_text(abstract)
    if not cleaned:
        return ""
    return f"<blockquote expandable>{html.escape(cleaned)}</blockquote>"


async def refresh_cache(update: Update, context: ContextTypes.DEFAULT_TYPE) -> List[Paper]:
    user_id = _get_user_id(update)
    if user_id is None:
        raise RuntimeError("Could not determine the Telegram user for this request.")

    user_data = context.user_data
    hours_back = get_hours_back(context.bot_data)
    keywords = get_keywords(user_data, user_id=user_id)

    query = build_arxiv_query(keywords=keywords)

    if not query:
        user_data["papers"] = []
        user_data["last_query"] = ""
        user_data["last_request_url"] = ""
        user_data["last_raw_entry_count"] = 0
        user_data["cache_hours_back"] = hours_back
        logger.info("No active query; waiting for keywords")
        return []

    logger.info("Refreshing arXiv cache for user %s with query: %s", user_id, query)

    entries, request_url = await asyncio.to_thread(
        fetch_arxiv_entries, query, DEFAULT_MAX_RESULTS
    )
    logger.info("Fetched %d raw entries from arXiv", len(entries))

    papers = entries_to_recent_papers(entries, hours_back)

    user_data["papers"] = papers
    user_data["last_query"] = query
    user_data["last_request_url"] = request_url
    user_data["last_refresh_utc"] = datetime.now(timezone.utc)
    user_data["last_raw_entry_count"] = len(entries)
    user_data["cache_hours_back"] = hours_back
    return papers


def get_cached_papers(context: ContextTypes.DEFAULT_TYPE) -> List[Paper]:
    if context.user_data.get("cache_hours_back") != get_hours_back(context.bot_data):
        return []
    return list(context.user_data.get("papers", []))


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Commands:\n"
        "/today - list recent matching arXiv papers\n"
        "/keywords - show active keywords and time window\n"
        "/setkeywords kw1, kw2, kw3 - set keywords from Telegram\n"
        "/clearkeywords - clear active keywords\n"
        "/sethours 72 - set recent window in hours\n"
        "/debugquery - show the current arXiv query and raw counts\n"
        "/refresh - refresh arXiv cache\n"
        "/help - show this help"
    )
    if update.message:
        await update.message.reply_text(text)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_cmd(update, context)


async def keywords_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    keywords = get_keywords(context.user_data, user_id=user_id)
    hours_back = get_hours_back(context.bot_data)

    if keywords:
        body = "\n".join(f"- {k}" for k in keywords)
    else:
        body = "(none)"

    if update.message:
        await update.message.reply_text(
            f"Recent window: last {hours_back} hours\n"
            f"Keywords:\n{body}"
        )


async def setkeywords_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not determine which Telegram user sent this command.")
        return

    if not context.args:
        if update.message:
            await update.message.reply_text(
                "Usage:\n"
                "/setkeywords keyword1, keyword2, keyword3\n\n"
                "For phrases, separate them with commas.\n"
                "Example:\n"
                "/setkeywords dark matter, dna, cancer"
            )
        return

    raw = " ".join(context.args).strip()
    keywords = parse_keywords_input(raw)

    if not keywords:
        if update.message:
            await update.message.reply_text("No valid keywords found.")
        return

    context.user_data["custom_keywords"] = keywords
    _save_user_setting(user_id, "custom_keywords", keywords)

    try:
        papers = await refresh_cache(update, context)
    except Exception as exc:
        logger.exception("Failed to refresh after setting keywords")
        if update.message:
            await update.message.reply_text(f"Keywords updated, but refresh failed:\n{exc}")
        return

    if update.message:
        await update.message.reply_text(
            "Keywords updated:\n- "
            + "\n- ".join(keywords)
            + f"\n\nFound {len(papers)} matching paper(s)."
        )


async def clearkeywords_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not determine which Telegram user sent this command.")
        return

    context.user_data["custom_keywords"] = []
    _save_user_setting(user_id, "custom_keywords", [])

    try:
        papers = await refresh_cache(update, context)
    except Exception as exc:
        logger.exception("Failed to refresh after clearing keywords")
        if update.message:
            await update.message.reply_text(f"Keywords cleared, but refresh failed:\n{exc}")
        return

    if update.message:
        await update.message.reply_text(
            f"Keywords cleared.\nFound {len(papers)} matching paper(s)."
        )


async def sethours_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        if update.message:
            await update.message.reply_text("Usage:\n/sethours 168")
        return

    try:
        hours = int(context.args[0])
    except ValueError:
        if update.message:
            await update.message.reply_text("Hours must be an integer.")
        return

    if hours <= 0:
        if update.message:
            await update.message.reply_text("Hours must be positive.")
        return

    context.bot_data["hours_back"] = hours

    settings = load_settings()
    settings["hours_back"] = hours
    save_settings(settings)

    try:
        papers = await refresh_cache(update, context)
    except Exception as exc:
        logger.exception("Failed to refresh after setting hours")
        if update.message:
            await update.message.reply_text(f"Recent window updated, but refresh failed:\n{exc}")
        return

    if update.message:
        await update.message.reply_text(
            f"Recent window updated to {hours} hours.\nFound {len(papers)} matching paper(s)."
        )


async def debugquery_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    query = context.user_data.get("last_query", "")
    raw_count = context.user_data.get("last_raw_entry_count", 0)
    request_url = context.user_data.get("last_request_url", "")
    keywords = get_keywords(context.user_data, user_id=user_id)
    hours_back = get_hours_back(context.bot_data)

    if not query:
        query = build_arxiv_query(keywords=keywords) or "(no active query)"

    msg = (
        f"Hours back: {hours_back}\n"
        f"Keywords: {keywords if keywords else '(none)'}\n\n"
        f"Current arXiv query:\n{query}\n\n"
        f"Last raw entry count: {raw_count}"
    )

    if request_url:
        msg += f"\n\nLast request URL:\n{request_url}"

    if update.message:
        await update.message.reply_text(msg)


async def refresh_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text("Refreshing arXiv feed...")

    try:
        papers = await refresh_cache(update, context)
    except Exception as exc:
        logger.exception("Refresh failed")
        if update.message:
            await update.message.reply_text(f"Refresh failed:\n{exc}")
        return

    raw_count = context.user_data.get("last_raw_entry_count", 0)
    if update.message:
        await update.message.reply_text(
            f"Refresh complete. Raw entries: {raw_count}. Matching recent papers: {len(papers)}."
        )


async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    papers = get_cached_papers(context)
    if not papers:
        try:
            papers = await refresh_cache(update, context)
        except Exception as exc:
            logger.exception("Initial refresh failed")
            if update.message:
                await update.message.reply_text(f"Could not fetch arXiv papers:\n{exc}")
            return

    if not papers:
        hours = get_hours_back(context.bot_data)
        raw_count = context.user_data.get("last_raw_entry_count", 0)
        query = context.user_data.get("last_query", "")

        if not query:
            text = (
                "No active query is set.\n\n"
                "Use /setkeywords to define one or more keywords.\n"
                "Example:\n"
                "/setkeywords cancer, dna, dark matter"
            )
        else:
            text = (
                f"No matching papers found in the last {hours} hours.\n"
                f"Raw entries fetched from arXiv: {raw_count}\n\n"
                f"Use /debugquery to inspect the current query."
            )

        if update.message:
            await update.message.reply_text(text)
        return

    for paper in papers:
        if update.message:
            await update.message.reply_text(
                format_paper_line(paper),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=build_paper_reply_markup(paper),
            )


async def pdf_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle PDF download requests from inline `📄` buttons.

    Parameters
    ----------
    update : Update
        Telegram update containing the callback query.
    context : ContextTypes.DEFAULT_TYPE
        Telegram callback context holding the per-user paper cache.

    Returns
    -------
    None
        Answers the callback query and sends the selected PDF document when
        available.

    Notes
    -----
    The callback payload stores the paper arXiv identifier, not the list index,
    so the requested paper remains stable across cache refreshes.
    """
    query = update.callback_query
    chat = update.effective_chat
    if query is None:
        return

    data = query.data or ""
    if not data.startswith("pdf:"):
        await query.answer()
        return

    arxiv_id = data.removeprefix("pdf:").strip()
    if not arxiv_id:
        await query.answer("Missing paper identifier.", show_alert=True)
        return

    paper = find_cached_paper_by_arxiv_id(context, arxiv_id)
    if paper is None:
        try:
            await refresh_cache(update, context)
        except Exception as exc:
            logger.exception("Refresh before PDF callback failed")
            await query.answer("Could not refresh the paper list.", show_alert=True)
            if chat is not None:
                await context.bot.send_message(
                    chat_id=chat.id,
                    text=f"Could not fetch arXiv papers:\n{exc}",
                )
            return
        paper = find_cached_paper_by_arxiv_id(context, arxiv_id)

    if paper is None:
        await query.answer(
            "This paper is no longer in your current list. Run /today again.",
            show_alert=True,
        )
        return

    if not paper.link_pdf:
        await query.answer("No PDF link is available for this paper.", show_alert=True)
        return

    caption = f"{paper.title}\n{paper.arxiv_id}"
    if len(caption) > 1024:
        caption = f"{paper.title[:1000].rstrip()}...\n{paper.arxiv_id}"

    await query.answer("Sending PDF...")
    try:
        if chat is None:
            raise RuntimeError("Could not determine the Telegram chat for the PDF download.")
        await context.bot.send_document(
            chat_id=chat.id,
            document=paper.link_pdf,
            caption=caption,
        )
    except Exception:
        logger.exception("Sending PDF through Telegram callback failed")
        if chat is not None:
            await context.bot.send_message(
                chat_id=chat.id,
                text=(
                    "Could not send the PDF file through Telegram.\n\n"
                    f"Direct PDF link:\n{paper.link_pdf}"
                ),
            )


async def post_init(application: Application) -> None:
    try:
        settings = load_settings()

        if "hours_back" in settings:
            application.bot_data["hours_back"] = int(settings["hours_back"])
        logger.info("Startup initialization completed")
    except Exception:
        logger.exception("Startup initialization failed")


def validate_environment() -> str:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN environment variable.")
    if not re.fullmatch(r"\d{6,}:[A-Za-z0-9_-]{30,}", token):
        raise RuntimeError(
            f"Malformed TELEGRAM_BOT_TOKEN: {token!r}\n"
            "Copy it again from BotFather. It should look like 123456789:AA..."
        )
    return token


def main() -> None:
    token = validate_environment()

    app = Application.builder().token(token).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("today", today_cmd))
    app.add_handler(CommandHandler("keywords", keywords_cmd))
    app.add_handler(CommandHandler("setkeywords", setkeywords_cmd))
    app.add_handler(CommandHandler("clearkeywords", clearkeywords_cmd))
    app.add_handler(CommandHandler("sethours", sethours_cmd))
    app.add_handler(CommandHandler("debugquery", debugquery_cmd))
    app.add_handler(CommandHandler("refresh", refresh_cmd))
    app.add_handler(CallbackQueryHandler(pdf_callback, pattern=r"^pdf:"))

    logger.info("Bot starting")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
