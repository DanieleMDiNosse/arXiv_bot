#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import csv
import html
import json
import logging
import math
import os
import re
import shlex
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, List, Optional, Sequence
from zoneinfo import ZoneInfo, available_timezones

import feedparser
import requests
from dateutil import parser as dateparser
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, MenuButtonDefault, ReplyKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

ARXIV_API_URL = "http://export.arxiv.org/api/query"
PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
DEFAULT_MAX_RESULTS = int(os.getenv("MAX_RESULTS", "300"))
TODAY_HOURS_BACK = 24
DAILY_RECAP_HOURS = 24
DEFAULT_DAILY_RECAP_TIME = "09:00"
DEFAULT_DAILY_RECAP_TIMEZONE = "UTC"
MAX_DAILY_RECAP_ITEMS = int(os.getenv("DAILY_RECAP_MAX_ITEMS", "20"))
MAX_ABSTRACT_CHARS = int(os.getenv("MAX_ABSTRACT_CHARS", "1600"))
PUBMED_FUTURE_GRACE_DAYS = int(os.getenv("PUBMED_FUTURE_GRACE_DAYS", "2"))
RECAP_TIMEZONE_PAGE_SIZE = max(1, int(os.getenv("RECAP_TIMEZONE_PAGE_SIZE", "8")))
COFFEE_URL = os.getenv("COFFEE_URL", "").strip()
COFFEE_TEXT = os.getenv("COFFEE_TEXT", "Support this bot").strip() or "Support this bot"
SETTINGS_FILE = Path("bot_settings.json")
MENU_BTN_TODAY = "Today"
MENU_BTN_KEYWORDS = "Keywords"
MENU_BTN_ADD_ARXIV_KEYWORD = "➕ arXiv keywords"
MENU_BTN_REMOVE_ARXIV_KEYWORD = "➖ arXiv keywords"
MENU_BTN_CLEAR_ARXIV_KEYWORD = "🧹 arXiv keywords"
MENU_BTN_ADD_PUBMED_KEYWORD = "➕ PubMed keywords"
MENU_BTN_REMOVE_PUBMED_KEYWORD = "➖ PubMed keywords"
MENU_BTN_CLEAR_PUBMED_KEYWORD = "🧹 PubMed keywords"
MENU_BTN_SEARCH_HOURS = "Search Hours"
MENU_BTN_DAILY_RECAP = "Recap On/Off"
MENU_BTN_SET_RECAP_TIME = "Recap Time"
MENU_BTN_RECAP_STATUS = "Recap Status"
MENU_BTN_BOOKMARKS = "Bookmarks"
MENU_BTN_HELP = "Help"
MENU_BTN_COFFEE = "Pay me a coffee"
SOURCE_ARXIV = "arxiv"
SOURCE_PUBMED = "pubmed"
WELCOME_SHOWN_AT_KEY = "welcome_shown_at"
SEARCH_SCOPE_TODAY = "today"
SEARCH_SCOPE_HOURS = "hours"
ARXIV_ANNOUNCEMENT_TZ = ZoneInfo("America/New_York")
ARXIV_ANNOUNCEMENT_HOUR = 20
ARXIV_ANNOUNCEMENT_WEEKDAYS = {0, 1, 2, 3, 6}


def build_main_menu_markup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [MENU_BTN_TODAY, MENU_BTN_SEARCH_HOURS],
            [MENU_BTN_KEYWORDS, MENU_BTN_BOOKMARKS],
            [
                MENU_BTN_ADD_ARXIV_KEYWORD,
                MENU_BTN_REMOVE_ARXIV_KEYWORD,
                MENU_BTN_CLEAR_ARXIV_KEYWORD,
            ],
            [
                MENU_BTN_ADD_PUBMED_KEYWORD,
                MENU_BTN_REMOVE_PUBMED_KEYWORD,
                MENU_BTN_CLEAR_PUBMED_KEYWORD,
            ],
            [MENU_BTN_DAILY_RECAP],
            [MENU_BTN_SET_RECAP_TIME, MENU_BTN_RECAP_STATUS],
            [MENU_BTN_HELP, MENU_BTN_COFFEE],
        ],
        resize_keyboard=True,
    )


def build_coffee_markup() -> Optional[InlineKeyboardMarkup]:
    if not COFFEE_URL:
        return None
    if not (COFFEE_URL.startswith("https://") or COFFEE_URL.startswith("http://")):
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(text="Open support link", url=COFFEE_URL)]]
    )


def build_recap_timezone_regions_markup() -> InlineKeyboardMarkup:
    """Build the inline keyboard used to browse timezone groups."""
    rows: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for group in RECAP_TIMEZONE_GROUPS:
        current_row.append(
            InlineKeyboardButton(
                text=group,
                callback_data=f"rtzpage:{group}:0",
            )
        )
        if len(current_row) == 3:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    return InlineKeyboardMarkup(rows)


def build_recap_timezone_choices_markup(group: str, page: int = 0) -> InlineKeyboardMarkup:
    """Build one paginated timezone selection keyboard for a given group."""
    zones = get_recap_timezones_for_group(group)
    if not zones:
        return build_recap_timezone_regions_markup()

    safe_page = max(0, page)
    total_pages = max(1, math.ceil(len(zones) / RECAP_TIMEZONE_PAGE_SIZE))
    safe_page = min(safe_page, total_pages - 1)
    start = safe_page * RECAP_TIMEZONE_PAGE_SIZE
    end = start + RECAP_TIMEZONE_PAGE_SIZE

    rows: List[List[InlineKeyboardButton]] = []
    for zone_name in zones[start:end]:
        rows.append(
            [
                InlineKeyboardButton(
                    text=zone_name,
                    callback_data=f"rtzpick:{RECAP_TIMEZONE_INDEX[zone_name]}",
                )
            ]
        )

    navigation: List[InlineKeyboardButton] = []
    if safe_page > 0:
        navigation.append(
            InlineKeyboardButton(
                text="Prev",
                callback_data=f"rtzpage:{group}:{safe_page - 1}",
            )
        )
    navigation.append(InlineKeyboardButton(text="Regions", callback_data="rtzregions"))
    if safe_page + 1 < total_pages:
        navigation.append(
            InlineKeyboardButton(
                text="Next",
                callback_data=f"rtzpage:{group}:{safe_page + 1}",
            )
        )
    rows.append(navigation)
    return InlineKeyboardMarkup(rows)


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
    source: str = SOURCE_ARXIV


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


def _list_available_recap_timezones() -> List[str]:
    """Return the canonical timezone names offered for recap scheduling."""
    try:
        raw_timezones = available_timezones()
    except Exception:
        logger.exception("Could not load available time zones; falling back to UTC only.")
        return [DEFAULT_DAILY_RECAP_TIMEZONE]

    filtered: set[str] = {DEFAULT_DAILY_RECAP_TIMEZONE}
    for zone_name in raw_timezones:
        candidate = str(zone_name or "").strip()
        if not candidate:
            continue
        if candidate in {"localtime", "build/etc/localtime"}:
            continue
        if candidate.startswith(("posix/", "right/", "build/")):
            continue
        filtered.add(candidate)

    return sorted(
        filtered,
        key=lambda zone_name: (0 if zone_name == DEFAULT_DAILY_RECAP_TIMEZONE else 1, zone_name.casefold()),
    )


def _recap_timezone_group(zone_name: str) -> str:
    """Map one timezone name to its browse group."""
    if zone_name == DEFAULT_DAILY_RECAP_TIMEZONE:
        return DEFAULT_DAILY_RECAP_TIMEZONE
    if "/" in zone_name:
        return zone_name.split("/", 1)[0]
    return "Other"


AVAILABLE_RECAP_TIMEZONES = _list_available_recap_timezones()
RECAP_TIMEZONE_NAME_MAP = {zone_name.casefold(): zone_name for zone_name in AVAILABLE_RECAP_TIMEZONES}
RECAP_TIMEZONE_INDEX = {zone_name: idx for idx, zone_name in enumerate(AVAILABLE_RECAP_TIMEZONES)}
RECAP_TIMEZONE_GROUPS = sorted(
    {_recap_timezone_group(zone_name) for zone_name in AVAILABLE_RECAP_TIMEZONES},
    key=lambda group: (
        0 if group == DEFAULT_DAILY_RECAP_TIMEZONE else 2 if group == "Other" else 1,
        group.casefold(),
    ),
)


def resolve_recap_timezone_name(raw: str) -> Optional[str]:
    """Resolve a user-supplied timezone name to a canonical available timezone."""
    cleaned = normalize_text(raw).replace(" ", "_")
    if not cleaned:
        return None
    return RECAP_TIMEZONE_NAME_MAP.get(cleaned.casefold())


def get_recap_timezones_for_group(group: str) -> List[str]:
    """Return all available timezone names for one browse group."""
    group_name = normalize_text(group)
    if not group_name:
        return []
    return [zone_name for zone_name in AVAILABLE_RECAP_TIMEZONES if _recap_timezone_group(zone_name) == group_name]


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


def _unique_strings(values: Sequence[Any]) -> List[str]:
    unique: List[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        unique.append(item)
    return unique


def parse_paper_ref(raw: str, default_source: str = SOURCE_ARXIV) -> Optional[tuple[str, str]]:
    token = str(raw or "").strip()
    if not token:
        return None

    if ":" in token:
        source, paper_id = token.split(":", 1)
        source = source.strip().casefold()
        paper_id = paper_id.strip()
        if source in {SOURCE_ARXIV, SOURCE_PUBMED} and paper_id:
            return source, paper_id

    return default_source, token


def make_paper_ref(source: str, paper_id: str) -> str:
    source_norm = str(source or "").strip().casefold()
    if source_norm not in {SOURCE_ARXIV, SOURCE_PUBMED}:
        source_norm = SOURCE_ARXIV
    return f"{source_norm}:{(paper_id or '').strip()}"


def paper_ref_for(paper: Paper) -> str:
    return make_paper_ref(paper.source, paper.arxiv_id)


def paper_source_label(source: str) -> str:
    return "PubMed" if str(source).casefold() == SOURCE_PUBMED else "arXiv"


def get_bookmarks(
    user_data: Optional[dict[str, Any]] = None,
    user_id: Optional[int] = None,
) -> List[str]:
    def _normalize_bookmark_values(values: Sequence[Any]) -> List[str]:
        normalized: List[str] = []
        seen: set[str] = set()
        for value in values:
            parsed = parse_paper_ref(str(value))
            if parsed is None:
                continue
            key = make_paper_ref(parsed[0], parsed[1])
            if key in seen:
                continue
            seen.add(key)
            normalized.append(key)
        return normalized

    if user_data is not None and "bookmarks" in user_data:
        cached = user_data["bookmarks"]
        if isinstance(cached, Sequence) and not isinstance(cached, (str, bytes, bytearray)):
            return _normalize_bookmark_values(cached)
        return []

    if user_id is None:
        return []

    settings = load_settings()
    user_settings = _get_user_settings(settings, user_id)
    bookmarks = user_settings.get("bookmarks", [])
    if not isinstance(bookmarks, Sequence) or isinstance(bookmarks, (str, bytes, bytearray)):
        return []

    cleaned = _normalize_bookmark_values(bookmarks)
    if user_data is not None:
        user_data["bookmarks"] = cleaned
    return cleaned


def set_bookmarks(
    user_id: int,
    bookmarks: Sequence[str],
    user_data: Optional[dict[str, Any]] = None,
) -> List[str]:
    cleaned: List[str] = []
    seen: set[str] = set()
    for value in bookmarks:
        parsed = parse_paper_ref(str(value))
        if parsed is None:
            continue
        key = make_paper_ref(parsed[0], parsed[1])
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(key)
    if user_data is not None:
        user_data["bookmarks"] = cleaned
    _save_user_setting(user_id, "bookmarks", cleaned if cleaned else None)
    return cleaned


def _get_user_id(update: Update) -> Optional[int]:
    user = update.effective_user
    if user is None:
        return None
    return int(user.id)


def parse_keyword_source(raw: str) -> Optional[str]:
    token = str(raw or "").strip().casefold()
    if token in {"arxiv", "ax"}:
        return SOURCE_ARXIV
    if token in {"pubmed", "pm"}:
        return SOURCE_PUBMED
    return None


def _keyword_cache_key_for_source(source: str) -> str:
    source_norm = SOURCE_PUBMED if source == SOURCE_PUBMED else SOURCE_ARXIV
    return f"custom_keywords_{source_norm}"


def get_keywords_for_source(
    source: str,
    user_data: Optional[dict[str, Any]] = None,
    user_id: Optional[int] = None,
) -> List[str]:
    source_norm = SOURCE_PUBMED if source == SOURCE_PUBMED else SOURCE_ARXIV
    cache_key = _keyword_cache_key_for_source(source_norm)
    if user_data is not None and cache_key in user_data:
        return list(user_data[cache_key])

    settings = load_settings()
    if user_id is not None:
        user_settings = _get_user_settings(settings, user_id)
        if cache_key in user_settings:
            keywords = list(user_settings[cache_key])
            if user_data is not None:
                user_data[cache_key] = keywords
            return keywords
        if "custom_keywords" in user_settings:
            keywords = list(user_settings["custom_keywords"])
            if user_data is not None:
                user_data[cache_key] = keywords
            return keywords

    env_name = "ARXIV_KEYWORDS" if source_norm == SOURCE_ARXIV else "PUBMED_KEYWORDS"
    raw = os.getenv(env_name, "").strip()
    if not raw and source_norm == SOURCE_PUBMED:
        # Backward compatibility: reuse ARXIV_KEYWORDS if source-specific env
        # is not configured yet.
        raw = os.getenv("ARXIV_KEYWORDS", "").strip()
    if not raw:
        return []
    return [k.strip() for k in raw.split(",") if k.strip()]


def get_keywords_for_pubmed(
    user_data: Optional[dict[str, Any]] = None,
    user_id: Optional[int] = None,
) -> List[str]:
    return get_keywords_for_source(
        SOURCE_PUBMED,
        user_data=user_data,
        user_id=user_id,
    )


def get_keywords_by_source(
    user_data: Optional[dict[str, Any]] = None,
    user_id: Optional[int] = None,
) -> dict[str, List[str]]:
    return {
        SOURCE_ARXIV: get_keywords_for_source(
            SOURCE_ARXIV,
            user_data=user_data,
            user_id=user_id,
        ),
        SOURCE_PUBMED: get_keywords_for_pubmed(user_data=user_data, user_id=user_id),
    }


def set_keywords_for_source(
    user_id: int,
    source: str,
    keywords: Sequence[str],
    user_data: Optional[dict[str, Any]] = None,
) -> List[str]:
    source_norm = SOURCE_PUBMED if source == SOURCE_PUBMED else SOURCE_ARXIV
    cache_key = _keyword_cache_key_for_source(source_norm)

    cleaned: List[str] = []
    seen: set[str] = set()
    for item in keywords:
        if not isinstance(item, str):
            continue
        value = normalize_text(item)
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(value)

    if user_data is not None:
        user_data[cache_key] = cleaned
    _save_user_setting(user_id, cache_key, cleaned if cleaned else None)
    return cleaned


def get_keywords(
    user_data: Optional[dict[str, Any]] = None,
    user_id: Optional[int] = None,
) -> List[str]:
    # Backward-compatible alias for existing call sites. This now means arXiv.
    return get_keywords_for_source(
        SOURCE_ARXIV,
        user_data=user_data,
        user_id=user_id,
    )


def parse_daily_recap_time(raw: str) -> Optional[str]:
    raw = raw.strip()
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", raw)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    return f"{hour:02d}:{minute:02d}"


def parse_daily_recap_times(raw: str) -> Optional[List[str]]:
    parts = [part for part in re.split(r"[\s,;]+", raw.strip()) if part]
    if not parts:
        return []

    parsed: List[str] = []
    seen: set[str] = set()
    for part in parts:
        parsed_time = parse_daily_recap_time(part)
        if parsed_time is None:
            return None
        if parsed_time in seen:
            continue
        seen.add(parsed_time)
        parsed.append(parsed_time)
    return parsed


def _coerce_daily_recap_times(value: Any) -> List[str]:
    if isinstance(value, str):
        parsed = parse_daily_recap_times(value)
        return parsed if parsed is not None else []

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        parsed: List[str] = []
        seen: set[str] = set()
        for item in value:
            candidate = parse_daily_recap_time(str(item).strip())
            if candidate is None or candidate in seen:
                continue
            seen.add(candidate)
            parsed.append(candidate)
        return parsed

    return [] 


def _coerce_daily_recap_timezone(value: Any) -> str:
    if isinstance(value, str):
        resolved = resolve_recap_timezone_name(value)
        if resolved is not None:
            return resolved
    return DEFAULT_DAILY_RECAP_TIMEZONE


def daily_recap_time_to_time(time_str: str, tz_name: str = DEFAULT_DAILY_RECAP_TIMEZONE) -> time:
    """Convert one recap time string to a timezone-aware `datetime.time`."""
    hour_text, minute_text = time_str.split(":", 1)
    try:
        tzinfo = ZoneInfo(_coerce_daily_recap_timezone(tz_name))
    except Exception:
        logger.exception("Could not load timezone %r for recap scheduling; falling back to UTC.", tz_name)
        tzinfo = timezone.utc
    return time(hour=int(hour_text), minute=int(minute_text), tzinfo=tzinfo)


def get_daily_recap_timezone(user_id: int) -> str:
    """Return the configured local timezone used for one user's recap schedule."""
    settings = load_settings()
    user_settings = _get_user_settings(settings, user_id)
    return _coerce_daily_recap_timezone(user_settings.get("daily_recap_timezone"))


def get_daily_recap_config(user_id: int) -> tuple[bool, List[str], Optional[int]]:
    settings = load_settings()
    user_settings = _get_user_settings(settings, user_id)

    enabled = bool(user_settings.get("daily_recap_enabled", False))
    recap_times = _coerce_daily_recap_times(user_settings.get("daily_recap_times"))
    if not recap_times:
        legacy_raw_time = str(user_settings.get("daily_recap_time", DEFAULT_DAILY_RECAP_TIME)).strip()
        legacy_time = parse_daily_recap_time(legacy_raw_time)
        if legacy_time is not None:
            recap_times = [legacy_time]
    if not recap_times:
        recap_times = [DEFAULT_DAILY_RECAP_TIME]

    raw_chat_id = user_settings.get("daily_recap_chat_id")
    chat_id: Optional[int]
    try:
        chat_id = int(raw_chat_id) if raw_chat_id is not None else None
    except (TypeError, ValueError):
        chat_id = None

    return enabled, recap_times, chat_id


def daily_recap_job_name(user_id: int) -> str:
    return f"daily_recap:{user_id}"


def _get_job_queue(application: Application) -> Any:
    # Avoid PTB warning from `application.job_queue` property when job-queue extra
    # is not installed; `_job_queue` is None in that case.
    return getattr(application, "_job_queue", None)


def _get_daily_recap_tasks(application: Application) -> dict[int, asyncio.Task[None]]:
    tasks = application.bot_data.get("_daily_recap_tasks")
    if not isinstance(tasks, dict):
        tasks = {}
        application.bot_data["_daily_recap_tasks"] = tasks
    return tasks


def remove_daily_recap_job(application: Application, user_id: int) -> None:
    job_queue = _get_job_queue(application)
    if job_queue is not None:
        for job in job_queue.get_jobs_by_name(daily_recap_job_name(user_id)):
            job.schedule_removal()

    tasks = _get_daily_recap_tasks(application)
    task = tasks.pop(user_id, None)
    if task is not None and not task.done():
        task.cancel()


async def daily_recap_fallback_loop(application: Application, user_id: int) -> None:
    while True:
        enabled, recap_times, chat_id = get_daily_recap_config(user_id)
        recap_timezone = get_daily_recap_timezone(user_id)
        if not enabled:
            return
        if chat_id is None:
            await asyncio.sleep(3600)
            continue

        now = datetime.now(timezone.utc)
        candidates: List[tuple[datetime, str]] = []
        for recap_time in recap_times:
            target_time = daily_recap_time_to_time(recap_time, recap_timezone)
            local_now = now.astimezone(target_time.tzinfo or timezone.utc)
            run_at_local = local_now.replace(
                hour=target_time.hour,
                minute=target_time.minute,
                second=0,
                microsecond=0,
            )
            if run_at_local <= local_now:
                run_at_local += timedelta(days=1)
            run_at = run_at_local.astimezone(timezone.utc)
            candidates.append((run_at, recap_time))

        if not candidates:
            await asyncio.sleep(3600)
            continue

        run_at, expected_time = min(candidates, key=lambda item: item[0])

        delay_seconds = max(1.0, (run_at - now).total_seconds())
        await asyncio.sleep(delay_seconds)

        enabled_now, recap_times_now, chat_id_now = get_daily_recap_config(user_id)
        if not enabled_now:
            return
        if expected_time not in recap_times_now:
            continue
        if chat_id_now is None:
            continue

        await send_daily_recap_for_user(
            application=application,
            user_id=user_id,
            chat_id=chat_id_now,
        )


def schedule_daily_recap_job(
    application: Application,
    user_id: int,
    chat_id: int,
    recap_times: Sequence[str],
    recap_timezone: str,
) -> None:
    remove_daily_recap_job(application, user_id)
    valid_times: List[str] = []
    seen: set[str] = set()
    for recap_time in recap_times:
        parsed = parse_daily_recap_time(str(recap_time))
        if parsed is None or parsed in seen:
            continue
        seen.add(parsed)
        valid_times.append(parsed)
    if not valid_times:
        valid_times = [DEFAULT_DAILY_RECAP_TIME]

    job_queue = _get_job_queue(application)
    if job_queue is not None:
        for recap_time in valid_times:
            job_queue.run_daily(
                callback=daily_recap_job_callback,
                time=daily_recap_time_to_time(recap_time, recap_timezone),
                name=daily_recap_job_name(user_id),
                user_id=user_id,
                chat_id=chat_id,
            )
        logger.info(
            "Scheduled daily recap (JobQueue) for user %s at %s %s (chat_id=%s)",
            user_id,
            ",".join(valid_times),
            recap_timezone,
            chat_id,
        )
        return

    tasks = _get_daily_recap_tasks(application)
    task = asyncio.create_task(
        daily_recap_fallback_loop(application, user_id),
        name=daily_recap_job_name(user_id),
    )
    tasks[user_id] = task

    def _cleanup(done_task: asyncio.Task[None]) -> None:
        active = _get_daily_recap_tasks(application)
        if active.get(user_id) is done_task:
            active.pop(user_id, None)

    task.add_done_callback(_cleanup)
    logger.info(
        "Scheduled daily recap (fallback loop) for user %s at %s %s (chat_id=%s)",
        user_id,
        ",".join(valid_times),
        recap_timezone,
        chat_id,
    )


def restore_daily_recap_jobs(application: Application) -> None:
    settings = load_settings()
    users = settings.get("users", {})
    if not isinstance(users, dict):
        return

    for user_id_str in users:
        try:
            user_id = int(user_id_str)
        except ValueError:
            continue

        enabled, recap_times, chat_id = get_daily_recap_config(user_id)
        remove_daily_recap_job(application, user_id)

        if not enabled:
            continue
        if chat_id is None:
            logger.warning(
                "Daily recap enabled for user %s but missing chat_id; schedule skipped.",
                user_id,
            )
            continue

        try:
            schedule_daily_recap_job(
                application,
                user_id,
                chat_id,
                recap_times,
                get_daily_recap_timezone(user_id),
            )
        except Exception:
            logger.exception("Could not schedule daily recap for user %s", user_id)


def parse_keywords_input(raw: str) -> List[str]:
    raw = raw.strip()
    if not raw:
        return []

    if "," in raw:
        try:
            items = next(csv.reader([raw], skipinitialspace=True))
        except Exception:
            items = [part.strip() for part in raw.split(",")]
    else:
        # Keep backward-compatible comma-separated lists, but when commas are
        # missing default to a single keyword so multi-word phrases work.
        if ("\"" in raw or "'" in raw) and "+" not in raw:
            try:
                items = shlex.split(raw)
            except Exception:
                items = [raw]
        else:
            items = [raw]

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


def parse_single_keyword_input(raw: str) -> Optional[str]:
    text = normalize_text(raw)
    if not text:
        return None

    if text.startswith('"') and text.endswith('"') and len(text) >= 2:
        text = normalize_text(text[1:-1])
    elif text.startswith("'") and text.endswith("'") and len(text) >= 2:
        text = normalize_text(text[1:-1])

    if not text:
        return None
    return text


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_search_scope(scope: Any) -> str:
    token = normalize_text(str(scope or "")).casefold()
    if token == SEARCH_SCOPE_TODAY:
        return SEARCH_SCOPE_TODAY
    return SEARCH_SCOPE_HOURS


def _describe_search_window(scope: str, hours_back: int) -> str:
    if _normalize_search_scope(scope) == SEARCH_SCOPE_TODAY:
        return "the latest arXiv announcement batch and the last 24 hours on PubMed"
    return f"the last {int(hours_back)} hours"


def _latest_arxiv_announcement_time(now_utc: Optional[datetime] = None) -> datetime:
    """Return the latest scheduled arXiv announcement time in UTC.

    Parameters
    ----------
    now_utc : datetime, optional
        Reference timestamp in UTC. If omitted, the current UTC time is used.

    Returns
    -------
    datetime
        UTC timestamp for the latest scheduled arXiv announcement batch.

    Notes
    -----
    arXiv publishes announcement batches at 20:00 Eastern US on Sunday
    through Thursday. Friday and Saturday have no scheduled announcements.
    This follows arXiv's regular published schedule and does not model ad hoc
    holiday deferrals.

    Examples
    --------
    >>> isinstance(_latest_arxiv_announcement_time(), datetime)
    True
    """
    reference_utc = ensure_utc(now_utc or datetime.now(timezone.utc))
    reference_et = reference_utc.astimezone(ARXIV_ANNOUNCEMENT_TZ)

    candidate_date = reference_et.date()
    candidate_time = datetime.combine(
        candidate_date,
        time(hour=ARXIV_ANNOUNCEMENT_HOUR),
        tzinfo=ARXIV_ANNOUNCEMENT_TZ,
    )
    if reference_et < candidate_time:
        candidate_date -= timedelta(days=1)

    while candidate_date.weekday() not in ARXIV_ANNOUNCEMENT_WEEKDAYS:
        candidate_date -= timedelta(days=1)

    return datetime.combine(
        candidate_date,
        time(hour=ARXIV_ANNOUNCEMENT_HOUR),
        tzinfo=ARXIV_ANNOUNCEMENT_TZ,
    ).astimezone(timezone.utc)


def _entries_to_latest_arxiv_batch_papers(
    entries: Sequence[Any],
    now_utc: Optional[datetime] = None,
) -> List[Paper]:
    """Convert arXiv API entries to papers from the latest announcement batch.

    Parameters
    ----------
    entries : Sequence[Any]
        Parsed arXiv API entries for a keyword query.
    now_utc : datetime, optional
        Reference timestamp in UTC. If omitted, the current UTC time is used.

    Returns
    -------
    List[Paper]
        Papers whose published timestamp falls in the most recent scheduled
        arXiv announcement batch.

    Notes
    -----
    The Today view should mirror arXiv's batch announcements rather than a
    rolling 24-hour window, because arXiv does not publish continuously.

    Examples
    --------
    >>> _entries_to_latest_arxiv_batch_papers([])
    []
    """
    latest_batch_utc = _latest_arxiv_announcement_time(now_utc)
    papers: List[Paper] = []

    for entry in entries:
        paper = entry_to_paper(entry, index=len(papers) + 1)
        if paper is None:
            continue
        if paper.published >= latest_batch_utc:
            papers.append(paper)

    return papers


def build_arxiv_query(keywords: Sequence[str]) -> Optional[str]:
    def build_term_clause(term: str) -> Optional[str]:
        term_clean = term.replace('"', "").strip()
        if not term_clean:
            return None
        return f'(ti:"{term_clean}" OR abs:"{term_clean}" OR all:"{term_clean}")'

    keyword_groups: List[str] = []
    for kw in keywords:
        term_clauses = [clause for clause in (build_term_clause(part) for part in kw.split("+")) if clause]
        if not term_clauses:
            continue
        keyword_groups.append("(" + " AND ".join(term_clauses) + ")")

    if not keyword_groups:
        return None

    return "(" + " OR ".join(keyword_groups) + ")"


def build_pubmed_query(keywords: Sequence[str]) -> Optional[str]:
    def build_term_clause(term: str) -> Optional[str]:
        term_clean = term.replace('"', "").strip()
        if not term_clean:
            return None
        return f'("{term_clean}"[Title/Abstract] OR "{term_clean}"[MeSH Terms])'

    clauses: List[str] = []
    for kw in keywords:
        term_clauses = [clause for clause in (build_term_clause(part) for part in kw.split("+")) if clause]
        if not term_clauses:
            continue
        clauses.append("(" + " AND ".join(term_clauses) + ")")

    if not clauses:
        return None

    return "(" + " OR ".join(clauses) + ")"


def _pubmed_month_to_int(raw: str) -> int:
    token = (raw or "").strip()
    if not token:
        return 1

    if token.isdigit():
        value = int(token)
        return min(max(value, 1), 12)

    token3 = token[:3].casefold()
    mapping = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    return mapping.get(token3, 1)


def _pubmed_date_from_node(node: Optional[ET.Element]) -> Optional[datetime]:
    if node is None:
        return None

    year_text = (node.findtext("Year") or "").strip()
    month_text = (node.findtext("Month") or "").strip()
    day_text = (node.findtext("Day") or "").strip()
    medline_date = (node.findtext("MedlineDate") or "").strip()

    if year_text.isdigit():
        year = int(year_text)
        month = _pubmed_month_to_int(month_text)
        day_match = re.search(r"\d{1,2}", day_text)
        day = int(day_match.group(0)) if day_match else 1
        try:
            return datetime(year=year, month=month, day=day, tzinfo=timezone.utc)
        except ValueError:
            return datetime(year=year, month=month, day=1, tzinfo=timezone.utc)

    if medline_date:
        try:
            parsed = dateparser.parse(medline_date, default=datetime(1970, 1, 1))
            if parsed is not None:
                return ensure_utc(parsed)
        except Exception:
            return None

    return None


def _pubmed_history_dates(article: ET.Element) -> dict[str, datetime]:
    history: dict[str, datetime] = {}
    for node in article.findall(".//PubmedData/History/PubMedPubDate"):
        status = (node.attrib.get("PubStatus") or "").strip().casefold()
        if not status:
            continue
        parsed = _pubmed_date_from_node(node)
        if parsed is None:
            continue
        history[status] = parsed
    return history


def _pick_first_non_future(
    candidates: Sequence[Optional[datetime]],
    future_limit: datetime,
) -> Optional[datetime]:
    for dt in candidates:
        if dt is not None and dt <= future_limit:
            return dt
    return None


def _pubmed_article_to_paper(article: ET.Element, index: int) -> Optional[Paper]:
    pmid = (article.findtext(".//MedlineCitation/PMID") or "").strip()
    if not pmid:
        return None

    title_node = article.find(".//Article/ArticleTitle")
    title = normalize_text("".join(title_node.itertext())) if title_node is not None else ""
    if title.startswith("[") and title.endswith("]") and len(title) >= 2:
        title = normalize_text(title[1:-1])
    if not title:
        title = "(untitled)"

    abstract_parts: List[str] = []
    for abstract_node in article.findall(".//Article/Abstract/AbstractText"):
        part = normalize_text("".join(abstract_node.itertext()))
        if not part:
            continue
        label = (abstract_node.attrib.get("Label") or "").strip()
        if label:
            abstract_parts.append(f"{label}: {part}")
        else:
            abstract_parts.append(part)
    summary = "\n".join(abstract_parts)

    authors: List[str] = []
    for author_node in article.findall(".//Article/AuthorList/Author"):
        collective_name = normalize_text(author_node.findtext("CollectiveName") or "")
        if collective_name:
            authors.append(collective_name)
            continue

        last = normalize_text(author_node.findtext("LastName") or "")
        first = normalize_text(author_node.findtext("ForeName") or "")
        initials = normalize_text(author_node.findtext("Initials") or "")
        full_name = normalize_text(" ".join(part for part in [first, last] if part))
        if full_name:
            authors.append(full_name)
        elif last:
            authors.append(last)
        elif initials:
            authors.append(initials)

    article_date = _pubmed_date_from_node(article.find(".//Article/ArticleDate"))
    journal_pub_date = _pubmed_date_from_node(article.find(".//Journal/JournalIssue/PubDate"))
    completed_date = _pubmed_date_from_node(article.find(".//DateCompleted"))
    revised_date = _pubmed_date_from_node(article.find(".//DateRevised"))
    history_dates = _pubmed_history_dates(article)

    # PubMed often contains a future print-issue date (ppublish). Prefer an
    # online/indexing date that is not in the future relative to "now".
    now_utc = datetime.now(timezone.utc)
    future_limit = now_utc + timedelta(days=PUBMED_FUTURE_GRACE_DAYS)

    published_candidates = [
        article_date,
        history_dates.get("epublish"),
        history_dates.get("aheadofprint"),
        history_dates.get("pubmed"),
        history_dates.get("entrez"),
        completed_date,
        journal_pub_date,
        history_dates.get("ppublish"),
        history_dates.get("medline"),
    ]
    published = _pick_first_non_future(published_candidates, future_limit)
    if published is None:
        # If every source date is still in the future, skip the record for now.
        return None

    updated_candidates = [
        revised_date,
        history_dates.get("revised"),
        history_dates.get("pubmed"),
        history_dates.get("entrez"),
        published,
    ]
    updated = _pick_first_non_future(updated_candidates, future_limit) or published
    if published is None:
        return None
    if updated is None:
        updated = published

    journal = normalize_text(article.findtext(".//Article/Journal/ISOAbbreviation") or "")
    if not journal:
        journal = normalize_text(article.findtext(".//Article/Journal/Title") or "")

    pmc_id = ""
    for id_node in article.findall(".//PubmedData/ArticleIdList/ArticleId"):
        id_type = (id_node.attrib.get("IdType") or "").strip().casefold()
        if id_type != "pmc":
            continue
        candidate = normalize_text("".join(id_node.itertext()))
        if candidate:
            pmc_id = candidate
            break

    if pmc_id and not pmc_id.upper().startswith("PMC"):
        pmc_id = f"PMC{pmc_id}"
    pubmed_pdf_link = (
        f"https://pmc.ncbi.nlm.nih.gov/articles/{pmc_id}/pdf/"
        if pmc_id
        else ""
    )

    return Paper(
        index=index,
        arxiv_id=pmid,
        title=title,
        summary=summary,
        authors=authors,
        published=published,
        updated=updated,
        published_raw=published.strftime("%Y-%m-%d"),
        updated_raw=updated.strftime("%Y-%m-%d"),
        primary_category=journal,
        link_abs=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
        link_pdf=pubmed_pdf_link,
        source=SOURCE_PUBMED,
    )


def fetch_pubmed_ids(
    query: str,
    max_results: int = DEFAULT_MAX_RESULTS,
    hours_back: Optional[int] = None,
) -> tuple[list[str], str]:
    params = {
        "db": "pubmed",
        "retmode": "json",
        "retmax": max_results,
        "sort": "pub date",
        "term": query,
    }
    if hours_back is not None and hours_back > 0:
        # Use Entrez indexing date window so "recent" results are less affected
        # by future print-issue dates in journal metadata.
        params["datetype"] = "edat"
        params["reldate"] = max(1, int(math.ceil(hours_back / 24)))

    response = requests.get(
        PUBMED_ESEARCH_URL,
        params=params,
        timeout=30,
        headers={"User-Agent": "telegram-arxiv-codex-bot/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    id_list = payload.get("esearchresult", {}).get("idlist", [])
    if not isinstance(id_list, list):
        return [], response.url
    ids = [str(item).strip() for item in id_list if str(item).strip()]
    return ids, response.url


def fetch_pubmed_articles_by_ids(pubmed_ids: Sequence[str]) -> tuple[list[Paper], str]:
    ids = [paper_id.strip() for paper_id in pubmed_ids if isinstance(paper_id, str) and paper_id.strip()]
    if not ids:
        return [], ""

    params = {
        "db": "pubmed",
        "retmode": "xml",
        "id": ",".join(ids),
    }

    response = requests.get(
        PUBMED_EFETCH_URL,
        params=params,
        timeout=30,
        headers={"User-Agent": "telegram-arxiv-codex-bot/1.0"},
    )
    response.raise_for_status()

    root = ET.fromstring(response.text)
    papers: List[Paper] = []
    for article in root.findall(".//PubmedArticle"):
        paper = _pubmed_article_to_paper(article, index=0)
        if paper is not None:
            papers.append(paper)

    return papers, response.url


def fetch_recent_pubmed_papers(
    query: str,
    hours_back: int,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    ids, search_url = fetch_pubmed_ids(
        query,
        max_results=max_results,
        hours_back=hours_back,
    )
    if not ids:
        return [], search_url, 0

    papers, fetch_url = fetch_pubmed_articles_by_ids(ids)
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=hours_back)
    future_limit = now_utc + timedelta(days=PUBMED_FUTURE_GRACE_DAYS)

    filtered = [
        paper
        for paper in papers
        if cutoff <= paper.published <= future_limit
    ]
    filtered.sort(key=lambda paper: paper.published, reverse=True)
    for idx, paper in enumerate(filtered, start=1):
        paper.index = idx

    request_url = search_url if not fetch_url else f"{search_url}\n{fetch_url}"
    return filtered, request_url, len(papers)


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


def fetch_arxiv_entries_by_ids(arxiv_ids: Sequence[str]) -> tuple[list[Any], str]:
    ids = [paper_id.strip() for paper_id in arxiv_ids if isinstance(paper_id, str) and paper_id.strip()]
    if not ids:
        return [], ""

    params = {
        "id_list": ",".join(ids),
        "max_results": len(ids),
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


def canonical_arxiv_id(arxiv_id: str) -> str:
    return re.sub(r"v\d+$", "", (arxiv_id or "").strip())


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


def entry_to_paper(entry: Any, index: int) -> Optional[Paper]:
    try:
        published = ensure_utc(dateparser.parse(getattr(entry, "published")))
        updated = ensure_utc(dateparser.parse(getattr(entry, "updated")))
    except Exception:
        logger.exception("Could not parse entry timestamps")
        return None

    authors: List[str] = []
    for author in getattr(entry, "authors", []):
        name = getattr(author, "name", "").strip()
        if name:
            authors.append(name)

    entry_id = getattr(entry, "id", "")
    arxiv_id = entry_id.rstrip("/").split("/")[-1] if entry_id else ""

    return Paper(
        index=index,
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


def entries_to_recent_papers(entries: Sequence[Any], hours_back: int) -> List[Paper]:
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=hours_back)

    papers: List[Paper] = []

    for entry in entries:
        paper = entry_to_paper(entry, index=len(papers) + 1)
        if paper is None:
            continue

        if paper.published < cutoff:
            break

        papers.append(paper)

    return papers


async def fetch_papers_by_arxiv_ids(arxiv_ids: Sequence[str]) -> tuple[List[Paper], List[str]]:
    requested_ids = _unique_strings(arxiv_ids)
    if not requested_ids:
        return [], []

    entries, _request_url = await asyncio.to_thread(fetch_arxiv_entries_by_ids, requested_ids)

    exact_map: dict[str, Paper] = {}
    canonical_map: dict[str, Paper] = {}
    for entry in entries:
        paper = entry_to_paper(entry, index=0)
        if paper is None or not paper.arxiv_id:
            continue
        exact_map[paper.arxiv_id] = paper
        canonical_map[canonical_arxiv_id(paper.arxiv_id)] = paper

    ordered: List[Paper] = []
    missing: List[str] = []
    for idx, requested_id in enumerate(requested_ids, start=1):
        paper = exact_map.get(requested_id)
        if paper is None:
            paper = canonical_map.get(canonical_arxiv_id(requested_id))
        if paper is None:
            missing.append(requested_id)
            continue
        paper.index = idx
        ordered.append(paper)

    return ordered, missing


async def fetch_papers_by_pubmed_ids(pubmed_ids: Sequence[str]) -> tuple[List[Paper], List[str]]:
    requested_ids = _unique_strings(pubmed_ids)
    if not requested_ids:
        return [], []

    papers, _request_url = await asyncio.to_thread(fetch_pubmed_articles_by_ids, requested_ids)
    by_id: dict[str, Paper] = {}
    for paper in papers:
        if paper.arxiv_id:
            by_id[paper.arxiv_id] = paper

    ordered: List[Paper] = []
    missing: List[str] = []
    for idx, requested_id in enumerate(requested_ids, start=1):
        paper = by_id.get(requested_id)
        if paper is None:
            missing.append(requested_id)
            continue
        paper.index = idx
        ordered.append(paper)

    return ordered, missing


async def fetch_papers_by_refs(paper_refs: Sequence[str]) -> tuple[List[Paper], List[str]]:
    normalized_refs = _unique_strings(paper_refs)
    if not normalized_refs:
        return [], []

    arxiv_ids: List[str] = []
    pubmed_ids: List[str] = []
    parsed_refs: List[tuple[str, str, str]] = []
    for raw_ref in normalized_refs:
        parsed = parse_paper_ref(raw_ref)
        if parsed is None:
            continue
        source, paper_id = parsed
        canonical_ref = make_paper_ref(source, paper_id)
        parsed_refs.append((canonical_ref, source, paper_id))
        if source == SOURCE_PUBMED:
            pubmed_ids.append(paper_id)
        else:
            arxiv_ids.append(paper_id)

    arxiv_papers, arxiv_missing = await fetch_papers_by_arxiv_ids(arxiv_ids)
    pubmed_papers, pubmed_missing = await fetch_papers_by_pubmed_ids(pubmed_ids)

    resolved: dict[str, Paper] = {}
    for paper in arxiv_papers:
        resolved[make_paper_ref(SOURCE_ARXIV, paper.arxiv_id)] = paper
    for paper in pubmed_papers:
        resolved[make_paper_ref(SOURCE_PUBMED, paper.arxiv_id)] = paper

    ordered: List[Paper] = []
    missing: List[str] = []
    for idx, (canonical_ref, _source, _paper_id) in enumerate(parsed_refs, start=1):
        paper = resolved.get(canonical_ref)
        if paper is None:
            missing.append(canonical_ref)
            continue
        paper.index = idx
        ordered.append(paper)

    # Include source-specific missing IDs from fetchers if needed for diagnostics.
    for arxiv_id in arxiv_missing:
        key = make_paper_ref(SOURCE_ARXIV, arxiv_id)
        if key not in missing:
            missing.append(key)
    for pubmed_id in pubmed_missing:
        key = make_paper_ref(SOURCE_PUBMED, pubmed_id)
        if key not in missing:
            missing.append(key)

    return ordered, missing


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
    title_text = normalize_text(paper.title)
    if len(title_text) > 450:
        title_text = title_text[:447].rstrip() + "..."

    authors = ", ".join(paper.authors[:3])
    if len(paper.authors) > 3:
        authors += ", et al."
    if len(authors) > 240:
        authors = authors[:237].rstrip() + "..."

    published_label = paper.published.strftime("%Y-%m-%d %H:%M UTC")
    abstract_html = render_expandable_abstract_html(paper.summary, max_chars=MAX_ABSTRACT_CHARS)
    source_label = paper_source_label(paper.source)
    category_text = normalize_text(paper.primary_category)
    if len(category_text) > 100:
        category_text = category_text[:97].rstrip() + "..."
    meta_parts = [
        f"<code>{html.escape(paper.arxiv_id)}</code>",
        html.escape(source_label),
    ]
    if category_text:
        meta_parts.append(html.escape(category_text))
    meta_parts.append(html.escape(published_label))

    message = (
        f"<b>{paper.index}.</b> {html.escape(title_text)}\n"
        f"<i>{html.escape(authors)}</i>\n"
        + " | ".join(meta_parts)
    )
    if abstract_html:
        message += f"\n{abstract_html}"

    if len(message) > 3900 and abstract_html:
        # Last-resort safety for Telegram 4096-char message limit.
        message = (
            f"<b>{paper.index}.</b> {html.escape(title_text)}\n"
            f"<i>{html.escape(authors)}</i>\n"
            + " | ".join(meta_parts)
            + "\n"
            + render_expandable_abstract_html(paper.summary, max_chars=600)
        )
    return message


def build_paper_reply_markup(
    paper: Paper,
    bookmarked: bool = False,
) -> Optional[InlineKeyboardMarkup]:
    """Build inline buttons for a paper entry.

    Parameters
    ----------
    paper : Paper
        Paper metadata shown in the `/today` list.

    Returns
    -------
    InlineKeyboardMarkup | None
        Inline keyboard with an arXiv link button and, when available, a PDF
        download callback button plus a bookmark toggle. Returns `None` if no
        buttons can be built.

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
            InlineKeyboardButton(text="📄", callback_data=f"pdf:{paper_ref_for(paper)}")
        )
    paper_ref = paper_ref_for(paper)
    if paper_ref:
        buttons.append(
            InlineKeyboardButton(
                text="✅" if bookmarked else "⭐",
                callback_data=f"bm:{paper_ref}",
            )
        )

    if not buttons:
        return None
    return InlineKeyboardMarkup([buttons])


def find_cached_paper_by_ref(
    context: ContextTypes.DEFAULT_TYPE,
    source: str,
    paper_id: str,
) -> Optional[Paper]:
    """Return a cached paper matching a given arXiv identifier.

    Parameters
    ----------
    context : ContextTypes.DEFAULT_TYPE
        Telegram callback context holding the per-user paper cache.
    source : str
        Source embedded in callback payload (e.g. `arxiv`, `pubmed`).
    paper_id : str
        Source-specific paper identifier embedded in callback payload.

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
    source_norm = str(source or "").casefold()
    for paper in list(context.user_data.get("papers", [])):
        if paper.source.casefold() == source_norm and paper.arxiv_id == paper_id:
            return paper
    return None


def update_bookmark_button_markup(
    markup: Optional[InlineKeyboardMarkup],
    paper_ref: str,
    bookmarked: bool,
) -> Optional[InlineKeyboardMarkup]:
    if markup is None:
        return None

    target_callbacks = {f"bm:{paper_ref}"}
    parsed = parse_paper_ref(paper_ref)
    if parsed is not None and parsed[0] == SOURCE_ARXIV:
        target_callbacks.add(f"bm:{parsed[1]}")
    updated_rows: List[List[InlineKeyboardButton]] = []
    changed = False

    for row in markup.inline_keyboard:
        updated_row: List[InlineKeyboardButton] = []
        for button in row:
            if button.callback_data in target_callbacks:
                updated_row.append(
                    InlineKeyboardButton(
                        text="✅" if bookmarked else "⭐",
                        callback_data=f"bm:{paper_ref}",
                    )
                )
                changed = True
            else:
                updated_row.append(button)
        updated_rows.append(updated_row)

    if not changed:
        return None
    return InlineKeyboardMarkup(updated_rows)


def render_expandable_abstract_html(abstract: str, max_chars: int = MAX_ABSTRACT_CHARS) -> str:
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
    if max_chars > 0 and len(cleaned) > max_chars:
        cleaned = cleaned[: max_chars - 3].rstrip() + "..."
    return f"<blockquote expandable>{html.escape(cleaned)}</blockquote>"


async def refresh_cache(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    hours_back: Optional[int] = None,
    scope: Optional[str] = None,
) -> List[Paper]:
    user_id = _get_user_id(update)
    if user_id is None:
        raise RuntimeError("Could not determine the Telegram user for this request.")

    user_data = context.user_data
    cached_hours_back = int(user_data.get("cache_hours_back", TODAY_HOURS_BACK))
    effective_scope = _normalize_search_scope(
        scope if scope is not None else user_data.get("cache_scope", SEARCH_SCOPE_TODAY)
    )
    effective_hours_back = int(hours_back) if hours_back is not None else cached_hours_back
    keywords_by_source = get_keywords_by_source(user_data=user_data, user_id=user_id)
    arxiv_keywords = keywords_by_source[SOURCE_ARXIV]
    pubmed_keywords = keywords_by_source[SOURCE_PUBMED]

    arxiv_query = build_arxiv_query(keywords=arxiv_keywords)
    pubmed_query = build_pubmed_query(keywords=pubmed_keywords)

    if not arxiv_query and not pubmed_query:
        user_data["papers"] = []
        user_data["last_query"] = ""
        user_data["last_request_url"] = ""
        user_data["last_raw_entry_count"] = 0
        user_data["last_raw_entry_breakdown"] = {}
        user_data["cache_hours_back"] = effective_hours_back
        user_data["cache_scope"] = effective_scope
        logger.info("No active query; waiting for keywords")
        return []

    logger.info(
        "Refreshing paper cache for user %s (arXiv=%s, PubMed=%s)",
        user_id,
        bool(arxiv_query),
        bool(pubmed_query),
    )

    arxiv_papers: List[Paper] = []
    arxiv_request_url = ""
    arxiv_raw_count = 0
    if arxiv_query:
        entries, arxiv_request_url = await asyncio.to_thread(
            fetch_arxiv_entries,
            arxiv_query,
            DEFAULT_MAX_RESULTS,
        )
        arxiv_raw_count = len(entries)
        if effective_scope == SEARCH_SCOPE_TODAY:
            # Today mirrors arXiv's scheduled announcement batches, not a
            # rolling 24-hour window.
            arxiv_papers = _entries_to_latest_arxiv_batch_papers(entries)
        else:
            arxiv_papers = entries_to_recent_papers(entries, effective_hours_back)

    pubmed_papers: List[Paper] = []
    pubmed_request_url = ""
    pubmed_raw_count = 0
    if pubmed_query:
        pubmed_papers, pubmed_request_url, pubmed_raw_count = await asyncio.to_thread(
            fetch_recent_pubmed_papers,
            pubmed_query,
            effective_hours_back,
            DEFAULT_MAX_RESULTS,
        )

    papers = arxiv_papers + pubmed_papers
    papers.sort(key=lambda paper: paper.published, reverse=True)
    for idx, paper in enumerate(papers, start=1):
        paper.index = idx

    query_lines: List[str] = []
    if arxiv_query:
        query_lines.append(f"arXiv: {arxiv_query}")
    if pubmed_query:
        query_lines.append(f"PubMed: {pubmed_query}")

    request_lines: List[str] = []
    if arxiv_request_url:
        request_lines.append(f"arXiv: {arxiv_request_url}")
    if pubmed_request_url:
        request_lines.append(f"PubMed: {pubmed_request_url}")

    raw_breakdown = {
        "arxiv": arxiv_raw_count,
        "pubmed": pubmed_raw_count,
    }
    raw_total = arxiv_raw_count + pubmed_raw_count

    user_data["papers"] = papers
    user_data["last_query"] = "\n".join(query_lines)
    user_data["last_request_url"] = "\n".join(request_lines)
    user_data["last_refresh_utc"] = datetime.now(timezone.utc)
    user_data["last_raw_entry_count"] = raw_total
    user_data["last_raw_entry_breakdown"] = raw_breakdown
    user_data["cache_hours_back"] = effective_hours_back
    user_data["cache_scope"] = effective_scope
    return papers


async def fetch_recent_papers_for_user(
    user_id: int,
    hours_back: int,
) -> tuple[list[Paper], str, int, dict[str, int]]:
    arxiv_keywords = get_keywords_for_source(SOURCE_ARXIV, user_id=user_id)
    pubmed_keywords = get_keywords_for_pubmed(user_id=user_id)
    arxiv_query = build_arxiv_query(keywords=arxiv_keywords)
    pubmed_query = build_pubmed_query(keywords=pubmed_keywords)
    if not arxiv_query and not pubmed_query:
        return [], "", 0, {}

    arxiv_papers: List[Paper] = []
    arxiv_raw = 0
    if arxiv_query:
        entries, _arxiv_url = await asyncio.to_thread(fetch_arxiv_entries, arxiv_query, DEFAULT_MAX_RESULTS)
        arxiv_raw = len(entries)
        arxiv_papers = entries_to_recent_papers(entries, hours_back)

    pubmed_papers: List[Paper] = []
    pubmed_raw = 0
    if pubmed_query:
        pubmed_papers, _pubmed_url, pubmed_raw = await asyncio.to_thread(
            fetch_recent_pubmed_papers,
            pubmed_query,
            hours_back,
            DEFAULT_MAX_RESULTS,
        )

    papers = arxiv_papers + pubmed_papers
    papers.sort(key=lambda paper: paper.published, reverse=True)
    for idx, paper in enumerate(papers, start=1):
        paper.index = idx

    query_lines: List[str] = []
    if arxiv_query:
        query_lines.append(f"arXiv: {arxiv_query}")
    if pubmed_query:
        query_lines.append(f"PubMed: {pubmed_query}")

    raw_breakdown = {"arxiv": arxiv_raw, "pubmed": pubmed_raw}
    return papers, "\n".join(query_lines), arxiv_raw + pubmed_raw, raw_breakdown


async def send_daily_recap_for_user(
    application: Application,
    user_id: int,
    chat_id: int,
) -> None:
    enabled, _, configured_chat_id = get_daily_recap_config(user_id)
    if not enabled:
        remove_daily_recap_job(application, user_id)
        return
    if configured_chat_id is None:
        logger.warning("Daily recap enabled for user %s but chat_id is missing.", user_id)
        return

    try:
        papers, query, raw_count, raw_breakdown = await fetch_recent_papers_for_user(
            user_id=user_id,
            hours_back=DAILY_RECAP_HOURS,
        )
    except Exception:
        logger.exception("Daily recap fetch failed for user %s", user_id)
        await application.bot.send_message(
            chat_id=chat_id,
            text="Daily recap failed due to a source fetch error.",
            reply_markup=build_main_menu_markup(),
        )
        return

    if not query:
        await application.bot.send_message(
            chat_id=chat_id,
            text=(
                "Daily recap skipped: no active keywords.\n"
                "Set keywords first using the keyword buttons in the menu."
            ),
            reply_markup=build_main_menu_markup(),
        )
        return

    now_label = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if not papers:
        arxiv_raw = raw_breakdown.get("arxiv", 0)
        pubmed_raw = raw_breakdown.get("pubmed", 0)
        await application.bot.send_message(
            chat_id=chat_id,
            text=(
                f"Daily recap ({now_label})\n"
                f"No matching papers in the last {DAILY_RECAP_HOURS} hours.\n"
                f"Raw entries fetched: {raw_count} "
                f"(arXiv: {arxiv_raw}, PubMed: {pubmed_raw})"
            ),
            reply_markup=build_main_menu_markup(),
        )
        return

    shown = min(len(papers), MAX_DAILY_RECAP_ITEMS)
    await application.bot.send_message(
        chat_id=chat_id,
        text=(
            f"Daily recap ({now_label})\n"
            f"Found {len(papers)} matching paper(s) in the last {DAILY_RECAP_HOURS} hours."
        ),
        reply_markup=build_main_menu_markup(),
    )

    for paper in papers[:shown]:
        await application.bot.send_message(
            chat_id=chat_id,
            text=format_paper_line(paper),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=build_paper_reply_markup(paper),
        )

    if len(papers) > shown:
        await application.bot.send_message(
            chat_id=chat_id,
            text=(
                f"Recap truncated: showing {shown} of {len(papers)} papers. "
                f"Use {MENU_BTN_TODAY} to see current cached results."
            ),
            reply_markup=build_main_menu_markup(),
        )


async def daily_recap_job_callback(context: ContextTypes.DEFAULT_TYPE) -> None:
    job = context.job
    user_id = job.user_id
    chat_id = job.chat_id

    if user_id is None or chat_id is None:
        logger.warning("Daily recap job missing user_id/chat_id; skipping.")
        return

    await send_daily_recap_for_user(
        application=context.application,
        user_id=user_id,
        chat_id=chat_id,
    )


def get_cached_papers(
    context: ContextTypes.DEFAULT_TYPE,
    hours_back: Optional[int] = None,
    scope: Optional[str] = None,
) -> List[Paper]:
    expected_hours = int(hours_back) if hours_back is not None else TODAY_HOURS_BACK
    if context.user_data.get("cache_hours_back") != expected_hours:
        return []
    expected_scope = _normalize_search_scope(scope if scope is not None else SEARCH_SCOPE_TODAY)
    cached_scope = _normalize_search_scope(context.user_data.get("cache_scope", SEARCH_SCOPE_TODAY))
    if cached_scope != expected_scope:
        return []
    return list(context.user_data.get("papers", []))


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return

    user_id = _get_user_id(update)
    is_first_open = False
    if user_id is not None:
        user_settings = _get_user_settings(load_settings(), user_id)
        is_first_open = WELCOME_SHOWN_AT_KEY not in user_settings
        if is_first_open:
            _save_user_setting(
                user_id,
                WELCOME_SHOWN_AT_KEY,
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )

    if is_first_open:
        welcome_text = (
            "<b>Welcome to dailyArXiv</b>\n\n"
            "This bot tracks new papers from arXiv + PubMed based on your keywords.\n\n"
            "<b>Quick Start</b>\n"
            f"1. Add keywords with <b>{html.escape(MENU_BTN_ADD_ARXIV_KEYWORD)}</b> and "
            f"<b>{html.escape(MENU_BTN_ADD_PUBMED_KEYWORD)}</b>\n"
            f"2. Tap <b>{html.escape(MENU_BTN_TODAY)}</b> for the latest arXiv batch and the last 24 hours on PubMed\n"
            f"3. Tap <b>{html.escape(MENU_BTN_SEARCH_HOURS)}</b> for a custom time window\n"
            f"4. Use <b>{html.escape(MENU_BTN_DAILY_RECAP)}</b> and "
            f"<b>{html.escape(MENU_BTN_SET_RECAP_TIME)}</b> for automatic updates at one or more local times\n\n"
            "<b>Tip</b>: use commas for OR and <code>+</code> inside one clause for AND, "
            'e.g. <code>"quantum mechanics" + entanglement, superconductivity</code>.'
        )
        await update.message.reply_text(
            welcome_text,
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )

    await update.message.reply_text(
        build_help_text(),
        reply_markup=build_main_menu_markup(),
        parse_mode=ParseMode.HTML,
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            build_help_text(),
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )


def build_help_text() -> str:
    return (
        "<b>Menu Actions</b>\n"
        f"• <b>{html.escape(MENU_BTN_TODAY)}</b>: show the latest arXiv announcement batch plus the last 24 hours on PubMed\n"
        f"• <b>{html.escape(MENU_BTN_SEARCH_HOURS)}</b>: run a custom rolling-hour search window for both sources\n"
        f"• <b>{html.escape(MENU_BTN_KEYWORDS)}</b>: show active arXiv and PubMed keyword lists\n"
        f"• <b>{html.escape(MENU_BTN_BOOKMARKS)}</b>: show saved papers\n\n"
        "<b>Keyword Management</b>\n"
        f"• <b>{html.escape(MENU_BTN_ADD_ARXIV_KEYWORD)}</b>: add one or more arXiv keywords\n"
        f"• <b>{html.escape(MENU_BTN_REMOVE_ARXIV_KEYWORD)}</b>: remove one or more arXiv keywords\n"
        f"• <b>{html.escape(MENU_BTN_CLEAR_ARXIV_KEYWORD)}</b>: clear all arXiv keywords\n"
        f"• <b>{html.escape(MENU_BTN_ADD_PUBMED_KEYWORD)}</b>: add one or more PubMed keywords\n"
        f"• <b>{html.escape(MENU_BTN_REMOVE_PUBMED_KEYWORD)}</b>: remove one or more PubMed keywords\n"
        f"• <b>{html.escape(MENU_BTN_CLEAR_PUBMED_KEYWORD)}</b>: clear all PubMed keywords\n\n"
        "<b>Recap</b>\n"
        f"• <b>{html.escape(MENU_BTN_DAILY_RECAP)}</b>: toggle daily recap\n"
        f"• <b>{html.escape(MENU_BTN_SET_RECAP_TIME)}</b>: choose a time zone and set one or more local recap times (HH:MM)\n"
        f"• <b>{html.escape(MENU_BTN_RECAP_STATUS)}</b>: show recap status and configured times\n\n"
        "<b>Other</b>\n"
        f"• <b>{html.escape(MENU_BTN_HELP)}</b>: show this guide\n"
        f"• <b>{html.escape(MENU_BTN_COFFEE)}</b>: open support link\n\n"
        "<b>Tip</b>: use commas for OR and <code>+</code> inside one clause for AND, "
        'e.g. <code>"quantum mechanics" + entanglement, superconductivity</code>.'
    )


async def coffee_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return

    markup = build_coffee_markup()
    if markup is None:
        await update.message.reply_text(
            "Support link not configured yet. Set COFFEE_URL in your environment.",
            reply_markup=build_main_menu_markup(),
        )
        return

    await update.message.reply_text(
        COFFEE_TEXT,
        reply_markup=markup,
        disable_web_page_preview=True,
    )


async def keywords_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    arxiv_keywords = get_keywords_for_source(
        SOURCE_ARXIV,
        context.user_data,
        user_id=user_id,
    )
    pubmed_keywords = get_keywords_for_pubmed(
        context.user_data,
        user_id=user_id,
    )
    arxiv_body = (
        "\n".join(f"- {html.escape(k)}" for k in arxiv_keywords)
        if arxiv_keywords
        else "(none)"
    )
    pubmed_body = (
        "\n".join(f"- {html.escape(k)}" for k in pubmed_keywords)
        if pubmed_keywords
        else "(none)"
    )

    if update.message:
        await update.message.reply_text(
            "<b>Active Keywords</b>\n\n"
            "<b>Today window</b>\n"
            "arXiv: latest announcement batch\n"
            f"PubMed: last {TODAY_HOURS_BACK} hours\n\n"
            "<b>arXiv</b>\n"
            f"<pre>{arxiv_body}</pre>\n\n"
            "<b>PubMed</b>\n"
            f"<pre>{pubmed_body}</pre>\n\n"
            f"Use <b>{html.escape(MENU_BTN_SEARCH_HOURS)}</b> for a custom time window.\n"
            "Tip: use commas for OR and <code>+</code> inside one clause for AND, "
            'e.g. <code>"quantum mechanics" + entanglement, superconductivity</code>.',
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )


def _clear_pending_input_flags(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("awaiting_keywords_input", None)
    context.user_data.pop("awaiting_add_keyword_source", None)
    context.user_data.pop("awaiting_remove_keyword_source", None)
    context.user_data.pop("awaiting_search_hours_input", None)
    context.user_data.pop("awaiting_hours_input", None)
    context.user_data.pop("awaiting_recap_timezone_input", None)
    context.user_data.pop("awaiting_recap_time_input", None)
    context.user_data.pop("pending_recap_timezone", None)


async def prompt_setkeywords_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _clear_pending_input_flags(context)
    context.user_data["awaiting_keywords_input"] = True
    if update.message:
        await update.message.reply_text(
            "<b>Set Keywords (Both Sources)</b>\n\n"
            "Send keywords as comma-separated values.\n\n"
            "<b>Example</b>\n"
            "<code>astronomy, climate change + photosynthesis</code>\n\n"
            "Use <code>+</code> inside one clause to require all terms.\n\n"
            "To edit one source only, use the dedicated Add, Remove and Clear buttons.",
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )


async def prompt_add_keyword_for_source(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    source: str,
) -> None:
    _clear_pending_input_flags(context)
    context.user_data["awaiting_add_keyword_source"] = source
    source_label = paper_source_label(source)
    if update.message:
        await update.message.reply_text(
            f"<b>Add Keywords • {html.escape(source_label)}</b>\n\n"
            "Send one or more keywords to add.\n"
            "Use commas to separate multiple values.\n\n"
            "<b>Examples</b>\n"
            "<code>quantum mechanics</code>\n"
            "<code>astronomy, climate change + photosynthesis</code>\n"
            "<code>\"quantum mechanics\" + entanglement</code>",
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )


async def prompt_remove_keyword_for_source(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    source: str,
) -> None:
    _clear_pending_input_flags(context)
    context.user_data["awaiting_remove_keyword_source"] = source
    source_label = paper_source_label(source)
    user_id = _get_user_id(update)
    current = get_keywords_for_source(source, context.user_data, user_id=user_id)
    body = "\n".join(f"- {k}" for k in current) if current else "(none)"
    body_html = html.escape(body)
    if update.message:
        await update.message.reply_text(
            f"<b>Remove Keywords • {html.escape(source_label)}</b>\n\n"
            "Send one or more keywords to remove.\n"
            "Use commas to separate multiple values.\n\n"
            "<b>Current list</b>\n"
            f"<pre>{body_html}</pre>\n"
            "<b>Examples</b>\n"
            "<code>astronomy</code>\n"
            "<code>astronomy, photosynthesis</code>",
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )


async def prompt_add_arxiv_keyword_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await prompt_add_keyword_for_source(update, context, SOURCE_ARXIV)


async def prompt_remove_arxiv_keyword_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await prompt_remove_keyword_for_source(update, context, SOURCE_ARXIV)


async def prompt_add_pubmed_keyword_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await prompt_add_keyword_for_source(update, context, SOURCE_PUBMED)


async def prompt_remove_pubmed_keyword_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await prompt_remove_keyword_for_source(update, context, SOURCE_PUBMED)


async def clear_arxiv_keywords_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await clear_keywords_for_source(update, context, SOURCE_ARXIV)


async def clear_pubmed_keywords_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await clear_keywords_for_source(update, context, SOURCE_PUBMED)


async def prompt_searchhours_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _clear_pending_input_flags(context)
    context.user_data["awaiting_search_hours_input"] = True
    if update.message:
        await update.message.reply_text(
            "<b>Search Hours</b>\n\n"
            "How many hours back do you want to search?\n\n"
            "<b>Example</b>\n"
            "<code>72</code>",
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )


def build_open_results_markup(
    hours_back: int,
    scope: str = SEARCH_SCOPE_HOURS,
) -> InlineKeyboardMarkup:
    safe_hours = max(1, int(hours_back))
    safe_scope = _normalize_search_scope(scope)
    button_text = "Open Today results" if safe_scope == SEARCH_SCOPE_TODAY else f"Open results ({safe_hours}h)"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(text=button_text, callback_data=f"open_matches:{safe_scope}:{safe_hours}")]]
    )


async def send_open_results_prompt(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    papers_count: int,
    hours_back: int,
    scope: str = SEARCH_SCOPE_HOURS,
) -> None:
    chat = update.effective_chat
    scope_text = html.escape(_describe_search_window(scope, hours_back))
    if update.message is not None:
        await update.message.reply_text(
            (
                f"<b>Found {papers_count} matching paper(s)</b> in {scope_text}.\n"
                "Tap the button below to open results now."
            ),
            reply_markup=build_open_results_markup(hours_back, scope=scope),
            parse_mode=ParseMode.HTML,
        )
        return
    if chat is not None:
        await context.bot.send_message(
            chat_id=chat.id,
            text=(
                f"<b>Found {papers_count} matching paper(s)</b> in {scope_text}.\n"
                "Tap the button below to open results now."
            ),
            reply_markup=build_open_results_markup(hours_back, scope=scope),
            parse_mode=ParseMode.HTML,
        )


async def apply_keywords_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    raw: str,
) -> bool:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not determine which Telegram user sent this command.")
        return False

    keywords = parse_keywords_input(raw)
    if not keywords:
        if update.message:
            await update.message.reply_text("No valid keywords found.")
        return False

    # Legacy behavior: /setkeywords without source sets both lists.
    arxiv_keywords = set_keywords_for_source(
        user_id=user_id,
        source=SOURCE_ARXIV,
        keywords=keywords,
        user_data=context.user_data,
    )
    pubmed_keywords = set_keywords_for_source(
        user_id=user_id,
        source=SOURCE_PUBMED,
        keywords=keywords,
        user_data=context.user_data,
    )

    try:
        papers = await refresh_cache(update, context)
    except Exception as exc:
        logger.exception("Failed to refresh after setting keywords")
        if update.message:
            await update.message.reply_text(f"Keywords updated, but refresh failed:\n{exc}")
        return False

    if update.message:
        await update.message.reply_text(
            "Keywords updated for BOTH sources.\n\n"
            "arXiv:\n- "
            + "\n- ".join(arxiv_keywords if arxiv_keywords else ["(none)"])
            + "\n\nPubMed:\n- "
            + "\n- ".join(pubmed_keywords if pubmed_keywords else ["(none)"]),
            reply_markup=build_main_menu_markup(),
        )
    await send_open_results_prompt(
        update=update,
        context=context,
        papers_count=len(papers),
        hours_back=int(context.user_data.get("cache_hours_back", TODAY_HOURS_BACK)),
        scope=_normalize_search_scope(context.user_data.get("cache_scope", SEARCH_SCOPE_TODAY)),
    )
    return True


async def apply_keywords_input_for_source(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    raw: str,
    source: str,
    mode: str,
) -> bool:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not determine which Telegram user sent this command.")
        return False

    source_label = paper_source_label(source)
    current = get_keywords_for_source(source, context.user_data, user_id=user_id)
    by_fold = {item.casefold(): item for item in current}

    changed = False
    added: List[str] = []
    skipped: List[str] = []
    removed: List[str] = []
    missing: List[str] = []

    if mode == "add":
        new_keywords = parse_keywords_input(raw)
        if not new_keywords:
            if update.message:
                await update.message.reply_text("No valid keywords found.")
            return False

        for keyword in new_keywords:
            key = keyword.casefold()
            if key in by_fold:
                skipped.append(by_fold[key])
                continue
            current.append(keyword)
            by_fold[key] = keyword
            added.append(keyword)
            changed = True
    elif mode == "remove":
        remove_keywords = parse_keywords_input(raw)
        if not remove_keywords:
            if update.message:
                await update.message.reply_text("No valid keywords found.")
            return False

        remove_keys = {keyword.casefold() for keyword in remove_keywords}
        missing = [keyword for keyword in remove_keywords if keyword.casefold() not in by_fold]

        updated_current: List[str] = []
        for item in current:
            if item.casefold() in remove_keys:
                removed.append(item)
                changed = True
                continue
            updated_current.append(item)
        current = updated_current
    else:
        raise RuntimeError(f"Unsupported keyword mode: {mode}")

    if not changed:
        if update.message:
            if mode == "add":
                await update.message.reply_text(
                    f"No new keywords to add for {source_label}.",
                    reply_markup=build_main_menu_markup(),
                )
            else:
                details = ""
                if missing:
                    details = "\nNot found:\n- " + "\n- ".join(missing)
                await update.message.reply_text(
                    f"No matching keywords found in {source_label}.{details}",
                    reply_markup=build_main_menu_markup(),
                )
        return False

    updated = set_keywords_for_source(
        user_id=user_id,
        source=source,
        keywords=current,
        user_data=context.user_data,
    )

    def _html_bullets(values: Sequence[str]) -> str:
        return "\n".join(f"• {html.escape(v)}" for v in values)

    body_html = _html_bullets(updated) if updated else "(none)"
    if mode == "remove":
        if update.message:
            lines = [
                f"<b>Updated {html.escape(source_label)} keywords</b>",
                "",
                f"<b>Removed {len(removed)} keyword(s)</b>",
                _html_bullets(removed),
            ]
            if missing:
                lines.extend(
                    [
                        "",
                        "<b>Not found</b>",
                        _html_bullets(missing),
                    ]
                )
            lines.extend(
                [
                    "",
                    f"<b>{html.escape(source_label)} keywords</b>",
                    body_html,
                ]
            )
            await update.message.reply_text(
                "\n".join(lines),
                reply_markup=build_main_menu_markup(),
                parse_mode=ParseMode.HTML,
            )
        return True

    # Add mode: refresh now so the user can immediately open matching results.
    try:
        papers = await refresh_cache(update, context)
    except Exception as exc:
        logger.exception("Failed to refresh after keyword change")
        if update.message:
            await update.message.reply_text(
                f"{source_label} keywords updated, but refresh failed:\n{exc}",
                reply_markup=build_main_menu_markup(),
            )
        return False

    if update.message:
        hours_back = int(context.user_data.get("cache_hours_back", TODAY_HOURS_BACK))
        lines: List[str] = [
            f"<b>Added {len(added)} keyword(s) to {html.escape(source_label)}</b>",
            _html_bullets(added),
        ]
        if skipped:
            lines.extend(
                [
                    "",
                    "<b>Skipped (already present)</b>",
                    _html_bullets(skipped),
                ]
            )
        lines.extend(
            [
                "",
                f"<b>{html.escape(source_label)} keywords</b>",
                body_html,
                "",
                (
                    f"<b>Found {len(papers)} matching paper(s)</b> in the last "
                    f"<b>{hours_back} hours</b>.\n"
                    "Tap the button below to open results now."
                ),
            ]
        )
        await update.message.reply_text(
            "\n".join(lines),
            reply_markup=build_open_results_markup(hours_back),
            parse_mode=ParseMode.HTML,
        )
        return True

    await send_open_results_prompt(
        update=update,
        context=context,
        papers_count=len(papers),
        hours_back=int(context.user_data.get("cache_hours_back", TODAY_HOURS_BACK)),
        scope=_normalize_search_scope(context.user_data.get("cache_scope", SEARCH_SCOPE_TODAY)),
    )
    return True


async def apply_set_keywords_for_source(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    raw: str,
    source: str,
) -> bool:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not determine which Telegram user sent this command.")
        return False

    keywords = parse_keywords_input(raw)
    if not keywords:
        if update.message:
            await update.message.reply_text("No valid keywords found.")
        return False

    source_label = paper_source_label(source)
    updated = set_keywords_for_source(
        user_id=user_id,
        source=source,
        keywords=keywords,
        user_data=context.user_data,
    )

    try:
        papers = await refresh_cache(update, context)
    except Exception as exc:
        logger.exception("Failed to refresh after setting source keywords")
        if update.message:
            await update.message.reply_text(
                f"{source_label} keywords updated, but refresh failed:\n{exc}",
                reply_markup=build_main_menu_markup(),
            )
        return False

    if update.message:
        await update.message.reply_text(
            f"{source_label} keywords set to:\n- "
            + "\n- ".join(updated if updated else ["(none)"])
            ,
            reply_markup=build_main_menu_markup(),
        )
    await send_open_results_prompt(
        update=update,
        context=context,
        papers_count=len(papers),
        hours_back=int(context.user_data.get("cache_hours_back", TODAY_HOURS_BACK)),
        scope=_normalize_search_scope(context.user_data.get("cache_scope", SEARCH_SCOPE_TODAY)),
    )
    return True


async def apply_search_hours_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    hours: int,
) -> bool:
    if hours <= 0:
        if update.message:
            await update.message.reply_text("Hours must be positive.")
        return False

    return await run_search_for_hours(
        update=update,
        context=context,
        hours_back=hours,
        force_refresh=True,
        scope=SEARCH_SCOPE_HOURS,
    )


async def setkeywords_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await prompt_setkeywords_input(update, context)
        return

    _clear_pending_input_flags(context)
    source = parse_keyword_source(context.args[0]) if context.args else None
    if source is not None:
        if len(context.args) < 2:
            if update.message:
                await update.message.reply_text(
                    "Format:\n"
                    "arxiv kw1, kw2 + kw3\n"
                    "pubmed kw1, kw2 + kw3\n\n"
                    "Use commas for OR and + for AND inside one clause.\n\n"
                    f"You can also use {MENU_BTN_ADD_ARXIV_KEYWORD} or {MENU_BTN_ADD_PUBMED_KEYWORD}.",
                    reply_markup=build_main_menu_markup(),
                )
            return
        raw = " ".join(context.args[1:]).strip()
        await apply_set_keywords_for_source(update, context, raw, source)
        return

    raw = " ".join(context.args).strip()
    await apply_keywords_input(update, context, raw)


async def clear_keywords_for_source(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    source: str,
) -> bool:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not determine which Telegram user sent this command.")
        return False

    source_label = paper_source_label(source)
    set_keywords_for_source(
        user_id=user_id,
        source=source,
        keywords=[],
        user_data=context.user_data,
    )

    if update.message:
        await update.message.reply_text(
            f"🧹 Cleared all keywords for {source_label}.",
            reply_markup=build_main_menu_markup(),
        )
    return True


async def clearkeywords_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not determine which Telegram user sent this command.")
        return

    _clear_pending_input_flags(context)
    source = parse_keyword_source(context.args[0]) if context.args else None
    if context.args and source is None:
        if update.message:
            await update.message.reply_text(
                "Choose one source to clear:\n"
                "- arxiv\n"
                "- pubmed\n\n"
                "Or use the clear buttons in the menu.",
                reply_markup=build_main_menu_markup(),
            )
        return

    if source is not None:
        await clear_keywords_for_source(update, context, source)
        return
    else:
        set_keywords_for_source(
            user_id=user_id,
            source=SOURCE_ARXIV,
            keywords=[],
            user_data=context.user_data,
        )
        set_keywords_for_source(
            user_id=user_id,
            source=SOURCE_PUBMED,
            keywords=[],
            user_data=context.user_data,
        )
        _save_user_setting(user_id, "custom_keywords", None)

    if update.message:
        await update.message.reply_text(
            "🧹 Cleared keywords for both sources.",
            reply_markup=build_main_menu_markup(),
        )


async def addkeyword_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        if update.message:
            await update.message.reply_text(
                "Format:\n"
                "arxiv quantum mechanics\n"
                "arxiv astronomy, climate change + photosynthesis\n"
                "pubmed genetics\n\n"
                "Use commas for OR and + for AND inside one clause.\n\n"
                f"Or use {MENU_BTN_ADD_ARXIV_KEYWORD} / {MENU_BTN_ADD_PUBMED_KEYWORD}.",
                reply_markup=build_main_menu_markup(),
            )
        return
    source = parse_keyword_source(context.args[0])
    if source is None:
        if update.message:
            await update.message.reply_text(
                "Source must be `arxiv` or `pubmed`.",
                reply_markup=build_main_menu_markup(),
            )
        return
    _clear_pending_input_flags(context)
    await apply_keywords_input_for_source(
        update,
        context,
        raw=" ".join(context.args[1:]),
        source=source,
        mode="add",
    )


async def removekeyword_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        if update.message:
            await update.message.reply_text(
                "Format:\n"
                "arxiv quantum mechanics\n"
                "arxiv astronomy, climate change + photosynthesis\n"
                "pubmed genetics\n\n"
                "Use the exact saved clause when removing keywords.\n\n"
                f"Or use {MENU_BTN_REMOVE_ARXIV_KEYWORD} / {MENU_BTN_REMOVE_PUBMED_KEYWORD}.",
                reply_markup=build_main_menu_markup(),
            )
        return
    source = parse_keyword_source(context.args[0])
    if source is None:
        if update.message:
            await update.message.reply_text(
                "Source must be `arxiv` or `pubmed`.",
                reply_markup=build_main_menu_markup(),
            )
        return
    _clear_pending_input_flags(context)
    await apply_keywords_input_for_source(
        update,
        context,
        raw=" ".join(context.args[1:]),
        source=source,
        mode="remove",
    )


async def searchhours_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await prompt_searchhours_input(update, context)
        return

    _clear_pending_input_flags(context)
    try:
        hours = int(context.args[0])
    except ValueError:
        if update.message:
            await update.message.reply_text("Hours must be an integer.")
        return

    await apply_search_hours_input(update, context, hours)


async def sethours_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Backward-compatible alias.
    await searchhours_cmd(update, context)


def format_recap_timezone_clock(tz_name: str) -> str:
    """Format the current clock time for one recap timezone."""
    try:
        local_now = datetime.now(timezone.utc).astimezone(ZoneInfo(_coerce_daily_recap_timezone(tz_name)))
    except Exception:
        logger.exception("Could not format local recap time for timezone %r.", tz_name)
        local_now = datetime.now(timezone.utc)
    return local_now.strftime("%Y-%m-%d %H:%M %Z")


async def show_recap_timezone_picker(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    group: Optional[str] = None,
    page: int = 0,
) -> None:
    user_id = _get_user_id(update)
    current_timezone = get_daily_recap_timezone(user_id) if user_id is not None else DEFAULT_DAILY_RECAP_TIMEZONE
    current_local_time = format_recap_timezone_clock(current_timezone)

    if group:
        zones = get_recap_timezones_for_group(group)
        total_pages = max(1, math.ceil(len(zones) / RECAP_TIMEZONE_PAGE_SIZE)) if zones else 1
        safe_page = min(max(0, page), total_pages - 1)
        text = (
            "<b>Set Recap Time</b>\n\n"
            "Choose a time zone for your local recap schedule.\n\n"
            f"<b>Current time zone</b>: <code>{html.escape(current_timezone)}</code>\n"
            f"<b>Current local time</b>: {html.escape(current_local_time)}\n"
            f"<b>Browsing</b>: {html.escape(group)} ({safe_page + 1}/{total_pages})\n\n"
            "Tap a time zone below or send an exact zone name such as "
            "<code>Europe/Rome</code>."
        )
        reply_markup = build_recap_timezone_choices_markup(group, safe_page)
    else:
        text = (
            "<b>Set Recap Time</b>\n\n"
            "1. Choose your time zone.\n"
            "2. Then send one or more local recap times as HH:MM.\n\n"
            f"<b>Current time zone</b>: <code>{html.escape(current_timezone)}</code>\n"
            f"<b>Current local time</b>: {html.escape(current_local_time)}\n\n"
            "Browse all available time zones by region below, or send an exact "
            "zone name such as <code>Europe/Rome</code>."
        )
        reply_markup = build_recap_timezone_regions_markup()

    query = update.callback_query
    chat = update.effective_chat
    if query is not None and query.message is not None:
        await query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )
        return
    if update.message is not None:
        await update.message.reply_text(
            text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )
        return
    if chat is not None:
        await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )


def _set_pending_recap_timezone(context: ContextTypes.DEFAULT_TYPE, tz_name: str) -> str:
    resolved = _coerce_daily_recap_timezone(tz_name)
    context.user_data["pending_recap_timezone"] = resolved
    context.user_data.pop("awaiting_recap_timezone_input", None)
    context.user_data["awaiting_recap_time_input"] = True
    return resolved


async def prompt_recap_local_time_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    tz_name: str,
) -> None:
    resolved_timezone = _set_pending_recap_timezone(context, tz_name)
    local_now_label = format_recap_timezone_clock(resolved_timezone)
    text = (
        "<b>Set Recap Time</b>\n\n"
        f"<b>Time zone</b>: <code>{html.escape(resolved_timezone)}</code>\n"
        f"<b>Current local time</b>: {html.escape(local_now_label)}\n\n"
        "Send one or more local recap times as HH:MM.\n"
        "Use commas or spaces to separate multiple times.\n\n"
        "<b>Examples</b>\n"
        "<code>09:30</code>\n"
        "<code>09:30, 21:00</code>"
    )

    query = update.callback_query
    chat = update.effective_chat
    if query is not None and query.message is not None:
        await query.edit_message_text(
            text=text,
            parse_mode=ParseMode.HTML,
        )
        return
    if update.message is not None:
        await update.message.reply_text(
            text,
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )
        return
    if chat is not None:
        await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )


async def apply_recap_timezone_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    raw: str,
) -> bool:
    cleaned = normalize_text(raw)
    if not cleaned:
        if update.message:
            await update.message.reply_text(
                "Please choose a time zone from the list or send an exact name such as Europe/Rome.",
                reply_markup=build_main_menu_markup(),
            )
        return False

    parts = cleaned.split(maxsplit=1)
    tz_name = resolve_recap_timezone_name(parts[0])
    if tz_name is None:
        if update.message:
            await update.message.reply_text(
                "Invalid time zone.\n"
                "Send an exact available name such as Europe/Rome, US/Eastern, or UTC.",
                reply_markup=build_main_menu_markup(),
            )
        return False

    if len(parts) > 1 and parts[1].strip():
        _set_pending_recap_timezone(context, tz_name)
        return await apply_recap_time_input(update, context, parts[1].strip())

    await prompt_recap_local_time_input(update, context, tz_name)
    return True


async def dailyrecap_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not determine which Telegram user sent this command.")
        return

    enabled, recap_times, chat_id = get_daily_recap_config(user_id)
    recap_timezone = get_daily_recap_timezone(user_id)
    status = "enabled" if enabled else "disabled"
    chat_label = str(chat_id) if chat_id is not None else "(not set)"
    times_label = ", ".join(recap_times)

    if update.message:
        await update.message.reply_text(
            "Daily recap status:\n"
            f"- Status: {status}\n"
            f"- Time zone: {recap_timezone}\n"
            f"- Times (local): {times_label}\n"
            f"- Current local time: {format_recap_timezone_clock(recap_timezone)}\n"
            f"- Chat ID: {chat_label}\n\n"
            f"Use {MENU_BTN_DAILY_RECAP} and {MENU_BTN_SET_RECAP_TIME}.",
            reply_markup=build_main_menu_markup(),
        )


async def prompt_setrecaptime_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _clear_pending_input_flags(context)
    context.user_data["awaiting_recap_timezone_input"] = True
    await show_recap_timezone_picker(update, context)


async def apply_recap_time_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    raw: str,
) -> bool:
    user_id = _get_user_id(update)
    chat = update.effective_chat
    if user_id is None or chat is None:
        if update.message:
            await update.message.reply_text("Could not determine user/chat for recap settings.")
        return False

    recap_timezone = _coerce_daily_recap_timezone(
        context.user_data.get("pending_recap_timezone") or get_daily_recap_timezone(user_id)
    )
    recap_times = parse_daily_recap_times(raw)
    if recap_times is None:
        if update.message:
            await update.message.reply_text(
                "Invalid time format.\n"
                f"Use one or more HH:MM times in {recap_timezone}.\n"
                "Examples: 09:30 or 09:30, 21:00."
            )
        return False
    if not recap_times:
        if update.message:
            await update.message.reply_text(
                "Please provide at least one time.\n"
                f"Example in {recap_timezone}: 09:30 or 09:30, 21:00."
            )
        return False

    _save_user_setting(user_id, "daily_recap_times", recap_times)
    # Keep legacy single-time key for backward compatibility.
    _save_user_setting(user_id, "daily_recap_time", recap_times[0])
    _save_user_setting(user_id, "daily_recap_timezone", recap_timezone)
    _save_user_setting(user_id, "daily_recap_chat_id", int(chat.id))

    enabled, _, _ = get_daily_recap_config(user_id)
    if enabled:
        try:
            schedule_daily_recap_job(
                context.application,
                user_id=user_id,
                chat_id=int(chat.id),
                recap_times=recap_times,
                recap_timezone=recap_timezone,
            )
        except Exception as exc:
            logger.exception("Could not reschedule daily recap after time update")
            if update.message:
                await update.message.reply_text(f"Time saved, but scheduling failed:\n{exc}")
            return False

    if update.message:
        times_label = ", ".join(recap_times)
        await update.message.reply_text(
            f"Daily recap times set to {times_label} in {recap_timezone}."
            + (" Recap is active." if enabled else " Recap is currently OFF."),
            reply_markup=build_main_menu_markup(),
        )
    context.user_data.pop("awaiting_recap_time_input", None)
    context.user_data.pop("awaiting_recap_timezone_input", None)
    context.user_data.pop("pending_recap_timezone", None)
    return True


async def setrecaptime_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await prompt_setrecaptime_input(update, context)
        return

    _clear_pending_input_flags(context)
    maybe_timezone = resolve_recap_timezone_name(context.args[0])
    if maybe_timezone is not None:
        if len(context.args) == 1:
            await prompt_recap_local_time_input(update, context, maybe_timezone)
            return
        _set_pending_recap_timezone(context, maybe_timezone)
        await apply_recap_time_input(update, context, " ".join(context.args[1:]).strip())
        return

    raw = " ".join(context.args).strip()
    await apply_recap_time_input(update, context, raw)


async def set_daily_recap_enabled(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    enabled: bool,
) -> bool:
    user_id = _get_user_id(update)
    chat = update.effective_chat
    if user_id is None or chat is None:
        if update.message:
            await update.message.reply_text("Could not determine user/chat for recap settings.")
        return False

    _, recap_times, _ = get_daily_recap_config(user_id)
    recap_timezone = get_daily_recap_timezone(user_id)
    _save_user_setting(user_id, "daily_recap_enabled", bool(enabled))
    _save_user_setting(user_id, "daily_recap_times", recap_times)
    _save_user_setting(user_id, "daily_recap_time", recap_times[0])
    _save_user_setting(user_id, "daily_recap_timezone", recap_timezone)
    _save_user_setting(user_id, "daily_recap_chat_id", int(chat.id))

    if enabled:
        try:
            schedule_daily_recap_job(
                context.application,
                user_id=user_id,
                chat_id=int(chat.id),
                recap_times=recap_times,
                recap_timezone=recap_timezone,
            )
        except Exception as exc:
            logger.exception("Could not enable daily recap")
            if update.message:
                await update.message.reply_text(f"Could not enable daily recap:\n{exc}")
            return False
        times_label = ", ".join(recap_times)
        message = (
            f"Daily recap enabled. You will receive updates every day at {times_label} in {recap_timezone}.\n"
            f"Each recap includes papers from the last {DAILY_RECAP_HOURS} hours."
        )
    else:
        remove_daily_recap_job(context.application, user_id)
        message = "Daily recap disabled."

    if update.message:
        await update.message.reply_text(message, reply_markup=build_main_menu_markup())
    return True


async def toggledailyrecap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not determine which Telegram user sent this command.")
        return
    enabled, _, _ = get_daily_recap_config(user_id)
    await set_daily_recap_enabled(update, context, not enabled)


async def dailyrecap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await dailyrecap_status_cmd(update, context)
        return

    arg = context.args[0].strip().casefold()
    if arg in {"on", "enable", "enabled"}:
        await set_daily_recap_enabled(update, context, True)
        return
    if arg in {"off", "disable", "disabled"}:
        await set_daily_recap_enabled(update, context, False)
        return
    if arg in {"status", "state"}:
        await dailyrecap_status_cmd(update, context)
        return

    if update.message:
        await update.message.reply_text(
            f"Use {MENU_BTN_DAILY_RECAP} to toggle recap and {MENU_BTN_RECAP_STATUS} to check status.",
            reply_markup=build_main_menu_markup(),
        )


async def recap_timezone_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return

    data = query.data or ""
    if data == "rtzregions":
        await query.answer()
        await show_recap_timezone_picker(update, context)
        return

    if data.startswith("rtzpage:"):
        payload = data.removeprefix("rtzpage:")
        try:
            group, page_text = payload.rsplit(":", 1)
            page = int(page_text)
        except ValueError:
            await query.answer("Invalid time zone page.", show_alert=True)
            return
        await query.answer()
        await show_recap_timezone_picker(update, context, group=group, page=page)
        return

    if data.startswith("rtzpick:"):
        raw_index = data.removeprefix("rtzpick:").strip()
        try:
            zone_name = AVAILABLE_RECAP_TIMEZONES[int(raw_index)]
        except (ValueError, IndexError):
            await query.answer("Invalid time zone.", show_alert=True)
            return
        await query.answer(f"Selected {zone_name}")
        await prompt_recap_local_time_input(update, context, zone_name)
        return

    await query.answer()


async def open_matches_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return

    data = query.data or ""
    if not data.startswith("open_matches:"):
        await query.answer()
        return

    raw_hours = data.removeprefix("open_matches:").strip()
    raw_scope = SEARCH_SCOPE_HOURS
    if ":" in raw_hours:
        raw_scope, raw_hours = raw_hours.split(":", 1)
    scope = _normalize_search_scope(raw_scope)
    try:
        hours_back = int(raw_hours)
    except ValueError:
        await query.answer("Invalid range.", show_alert=True)
        return

    if hours_back <= 0:
        await query.answer("Invalid range.", show_alert=True)
        return

    await query.answer("Fetching results...")
    await run_search_for_hours(
        update=update,
        context=context,
        hours_back=hours_back,
        force_refresh=True,
        scope=scope,
    )


async def menu_text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return

    text = (message.text or "").strip()
    if not text:
        return

    menu_actions = {
        MENU_BTN_TODAY.casefold(): today_cmd,
        MENU_BTN_KEYWORDS.casefold(): keywords_cmd,
        MENU_BTN_ADD_ARXIV_KEYWORD.casefold(): prompt_add_arxiv_keyword_input,
        MENU_BTN_REMOVE_ARXIV_KEYWORD.casefold(): prompt_remove_arxiv_keyword_input,
        MENU_BTN_CLEAR_ARXIV_KEYWORD.casefold(): clear_arxiv_keywords_cmd,
        MENU_BTN_ADD_PUBMED_KEYWORD.casefold(): prompt_add_pubmed_keyword_input,
        MENU_BTN_REMOVE_PUBMED_KEYWORD.casefold(): prompt_remove_pubmed_keyword_input,
        MENU_BTN_CLEAR_PUBMED_KEYWORD.casefold(): clear_pubmed_keywords_cmd,
        MENU_BTN_SEARCH_HOURS.casefold(): prompt_searchhours_input,
        MENU_BTN_DAILY_RECAP.casefold(): toggledailyrecap_cmd,
        MENU_BTN_SET_RECAP_TIME.casefold(): prompt_setrecaptime_input,
        MENU_BTN_RECAP_STATUS.casefold(): dailyrecap_status_cmd,
        MENU_BTN_BOOKMARKS.casefold(): bookmarks_cmd,
        MENU_BTN_HELP.casefold(): help_cmd,
        MENU_BTN_COFFEE.casefold(): coffee_cmd,
    }

    pending_flags = [
        "awaiting_keywords_input",
        "awaiting_add_keyword_source",
        "awaiting_remove_keyword_source",
        "awaiting_search_hours_input",
        "awaiting_hours_input",
        "awaiting_recap_timezone_input",
        "awaiting_recap_time_input",
    ]
    has_pending_action = any(context.user_data.get(flag, False) for flag in pending_flags)

    action = menu_actions.get(text.casefold())
    if action is not None:
        if has_pending_action:
            _clear_pending_input_flags(context)
        await action(update, context)
        return

    if context.user_data.get("awaiting_keywords_input", False):
        await apply_keywords_input(update, context, text)
        context.user_data.pop("awaiting_keywords_input", None)
        return

    if context.user_data.get("awaiting_add_keyword_source", False):
        source = str(context.user_data["awaiting_add_keyword_source"])
        await apply_keywords_input_for_source(
            update,
            context,
            raw=text,
            source=source,
            mode="add",
        )
        context.user_data.pop("awaiting_add_keyword_source", None)
        return

    if context.user_data.get("awaiting_remove_keyword_source", False):
        source = str(context.user_data["awaiting_remove_keyword_source"])
        await apply_keywords_input_for_source(
            update,
            context,
            raw=text,
            source=source,
            mode="remove",
        )
        context.user_data.pop("awaiting_remove_keyword_source", None)
        return

    if context.user_data.get("awaiting_search_hours_input", False) or context.user_data.get("awaiting_hours_input", False):
        try:
            hours = int(text)
        except ValueError:
            await message.reply_text("Hours must be an integer. Action canceled.")
            context.user_data.pop("awaiting_search_hours_input", None)
            context.user_data.pop("awaiting_hours_input", None)
            return
        await apply_search_hours_input(update, context, hours)
        context.user_data.pop("awaiting_search_hours_input", None)
        context.user_data.pop("awaiting_hours_input", None)
        return

    if context.user_data.get("awaiting_recap_timezone_input", False):
        await apply_recap_timezone_input(update, context, text)
        return

    if context.user_data.get("awaiting_recap_time_input", False):
        await apply_recap_time_input(update, context, text)
        return

    await message.reply_text(
        "Use the keyboard buttons below.",
        reply_markup=build_main_menu_markup(),
    )


async def debugquery_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    query = context.user_data.get("last_query", "")
    raw_count = context.user_data.get("last_raw_entry_count", 0)
    raw_breakdown = context.user_data.get("last_raw_entry_breakdown", {})
    request_url = context.user_data.get("last_request_url", "")
    keywords_by_source = get_keywords_by_source(context.user_data, user_id=user_id)
    arxiv_keywords = keywords_by_source[SOURCE_ARXIV]
    pubmed_keywords = keywords_by_source[SOURCE_PUBMED]
    hours_back = int(context.user_data.get("cache_hours_back", TODAY_HOURS_BACK))
    scope = _normalize_search_scope(context.user_data.get("cache_scope", SEARCH_SCOPE_TODAY))

    if not query:
        arxiv_query = build_arxiv_query(keywords=arxiv_keywords)
        pubmed_query = build_pubmed_query(keywords=pubmed_keywords)
        query_lines: List[str] = []
        if arxiv_query:
            query_lines.append(f"arXiv: {arxiv_query}")
        if pubmed_query:
            query_lines.append(f"PubMed: {pubmed_query}")
        query = "\n".join(query_lines) if query_lines else "(no active query)"

    arxiv_raw = 0
    pubmed_raw = 0
    if isinstance(raw_breakdown, dict):
        arxiv_raw = int(raw_breakdown.get("arxiv", 0) or 0)
        pubmed_raw = int(raw_breakdown.get("pubmed", 0) or 0)

    msg = (
        f"Search scope: {_describe_search_window(scope, hours_back)}\n"
        f"Hours back: {hours_back}\n"
        f"arXiv keywords: {arxiv_keywords if arxiv_keywords else '(none)'}\n"
        f"PubMed keywords: {pubmed_keywords if pubmed_keywords else '(none)'}\n\n"
        f"Current queries:\n{query}\n\n"
        f"Last raw entry count: {raw_count} "
        f"(arXiv: {arxiv_raw}, PubMed: {pubmed_raw})"
    )

    if request_url:
        msg += f"\n\nLast request URL:\n{request_url}"

    if update.message:
        await update.message.reply_text(msg)


async def refresh_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Backward-compatible alias: refresh now behaves exactly like Today.
    await today_cmd(update, context)


async def run_search_for_hours(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    hours_back: int,
    force_refresh: bool = False,
    scope: str = SEARCH_SCOPE_HOURS,
) -> bool:
    user_id = _get_user_id(update)
    chat = update.effective_chat
    effective_scope = _normalize_search_scope(scope)

    async def send_text(
        text: str,
        *,
        reply_markup: Optional[Any] = None,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: Optional[bool] = None,
    ) -> None:
        if update.message is not None:
            await update.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )
            return
        if chat is not None:
            await context.bot.send_message(
                chat_id=chat.id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )

    papers = [] if force_refresh else get_cached_papers(context, hours_back=hours_back, scope=effective_scope)
    if not papers:
        try:
            papers = await refresh_cache(
                update,
                context,
                hours_back=hours_back,
                scope=effective_scope,
            )
        except Exception as exc:
            logger.exception("Search refresh failed")
            await send_text(f"Could not fetch papers:\n{exc}")
            return False

    if not papers:
        raw_count = context.user_data.get("last_raw_entry_count", 0)
        raw_breakdown = context.user_data.get("last_raw_entry_breakdown", {})
        arxiv_raw = int(raw_breakdown.get("arxiv", 0) or 0) if isinstance(raw_breakdown, dict) else 0
        pubmed_raw = int(raw_breakdown.get("pubmed", 0) or 0) if isinstance(raw_breakdown, dict) else 0
        query = context.user_data.get("last_query", "")

        if not query:
            text = (
                "No active query is set.\n\n"
                f"Use {MENU_BTN_ADD_ARXIV_KEYWORD} and {MENU_BTN_ADD_PUBMED_KEYWORD} "
                "to define your keywords."
            )
        else:
            text = (
                f"No matching papers found in {_describe_search_window(effective_scope, hours_back)}.\n"
                f"Raw entries fetched: {raw_count} "
                f"(arXiv: {arxiv_raw}, PubMed: {pubmed_raw})\n\n"
                f"Adjust keywords via {MENU_BTN_KEYWORDS} and try again."
            )

        await send_text(text, reply_markup=build_main_menu_markup())
        return True

    await send_text(
        f"Found {len(papers)} matching paper(s) in {_describe_search_window(effective_scope, hours_back)}.",
        reply_markup=build_main_menu_markup(),
    )

    bookmarks = set(get_bookmarks(context.user_data, user_id=user_id))
    for paper in papers:
        await send_text(
            format_paper_line(paper),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=build_paper_reply_markup(
                paper,
                bookmarked=paper_ref_for(paper) in bookmarks,
            ),
        )
    return True


async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await run_search_for_hours(
        update=update,
        context=context,
        hours_back=TODAY_HOURS_BACK,
        force_refresh=True,
        scope=SEARCH_SCOPE_TODAY,
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

    parsed_ref = parse_paper_ref(data.removeprefix("pdf:").strip())
    if parsed_ref is None:
        await query.answer("Missing paper identifier.", show_alert=True)
        return
    source, paper_id = parsed_ref

    paper = find_cached_paper_by_ref(context, source, paper_id)
    if paper is None:
        try:
            await refresh_cache(update, context)
        except Exception as exc:
            logger.exception("Refresh before PDF callback failed")
            await query.answer("Could not refresh the paper list.", show_alert=True)
            if chat is not None:
                await context.bot.send_message(
                    chat_id=chat.id,
                    text=f"Could not fetch papers:\n{exc}",
                )
            return
        paper = find_cached_paper_by_ref(context, source, paper_id)

    if paper is None:
        await query.answer(
            f"This paper is no longer in your current list. Run {MENU_BTN_TODAY} again.",
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


async def bookmark_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return

    data = query.data or ""
    if not data.startswith("bm:"):
        await query.answer()
        return

    parsed_ref = parse_paper_ref(data.removeprefix("bm:").strip())
    if parsed_ref is None:
        await query.answer("Missing paper identifier.", show_alert=True)
        return
    source, paper_id = parsed_ref
    paper_ref = make_paper_ref(source, paper_id)

    user_id = _get_user_id(update)
    if user_id is None:
        await query.answer("Could not determine Telegram user.", show_alert=True)
        return

    bookmarks = get_bookmarks(context.user_data, user_id=user_id)
    if paper_ref in bookmarks:
        bookmarks = [item for item in bookmarks if item != paper_ref]
        is_bookmarked = False
    else:
        bookmarks.append(paper_ref)
        is_bookmarked = True
    set_bookmarks(user_id, bookmarks, context.user_data)

    paper = find_cached_paper_by_ref(context, source, paper_id)
    reply_markup: Optional[InlineKeyboardMarkup] = None
    if query.message is not None:
        if paper is not None:
            reply_markup = build_paper_reply_markup(
                paper,
                bookmarked=is_bookmarked,
            )
        else:
            reply_markup = update_bookmark_button_markup(
                query.message.reply_markup,
                paper_ref=paper_ref,
                bookmarked=is_bookmarked,
            )

    if query.message is not None and reply_markup is not None:
        try:
            await query.edit_message_reply_markup(
                reply_markup=reply_markup
            )
        except Exception:
            logger.exception("Could not update bookmark button state")

    await query.answer("Saved to bookmarks." if is_bookmarked else "Removed from bookmarks.")


async def bookmarks_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not determine which Telegram user sent this command.")
        return

    bookmark_ids = get_bookmarks(context.user_data, user_id=user_id)
    if not bookmark_ids:
        if update.message:
            await update.message.reply_text(
                f"No bookmarks yet. Use ⭐ on a paper from {MENU_BTN_TODAY}.",
                reply_markup=build_main_menu_markup(),
            )
        return

    try:
        papers, missing = await fetch_papers_by_refs(bookmark_ids)
    except Exception as exc:
        logger.exception("Failed to fetch bookmarked papers")
        if update.message:
            await update.message.reply_text(
                f"Could not load bookmarks:\n{exc}",
                reply_markup=build_main_menu_markup(),
            )
        return

    if not papers:
        if update.message:
            await update.message.reply_text(
                "No bookmarked papers could be retrieved from sources right now.",
                reply_markup=build_main_menu_markup(),
            )
        return

    for paper in papers:
        if update.message:
            await update.message.reply_text(
                format_paper_line(paper),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=build_paper_reply_markup(paper, bookmarked=True),
            )

    if missing and update.message:
        missing_preview = ", ".join(missing[:5])
        suffix = "..." if len(missing) > 5 else ""
        await update.message.reply_text(
            f"Could not resolve {len(missing)} bookmark(s): {missing_preview}{suffix}",
            reply_markup=build_main_menu_markup(),
        )


async def post_init(application: Application) -> None:
    try:
        # Keep slash commands working via handlers, but hide Telegram's command menu.
        await application.bot.delete_my_commands()
        await application.bot.set_chat_menu_button(menu_button=MenuButtonDefault())

        restore_daily_recap_jobs(application)
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
    app.add_handler(CommandHandler("addkeyword", addkeyword_cmd))
    app.add_handler(CommandHandler("removekeyword", removekeyword_cmd))
    app.add_handler(CommandHandler("searchhours", searchhours_cmd))
    app.add_handler(CommandHandler("sethours", sethours_cmd))
    app.add_handler(CommandHandler("bookmarks", bookmarks_cmd))
    app.add_handler(CommandHandler("coffee", coffee_cmd))
    app.add_handler(CommandHandler("dailyrecap", dailyrecap_cmd))
    app.add_handler(CommandHandler("setrecaptime", setrecaptime_cmd))
    app.add_handler(CommandHandler("debugquery", debugquery_cmd))
    app.add_handler(CommandHandler("refresh", refresh_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_text_router))
    app.add_handler(CallbackQueryHandler(recap_timezone_callback, pattern=r"^rtz"))
    app.add_handler(CallbackQueryHandler(open_matches_callback, pattern=r"^open_matches:"))
    app.add_handler(CallbackQueryHandler(pdf_callback, pattern=r"^pdf:"))
    app.add_handler(CallbackQueryHandler(bookmark_callback, pattern=r"^bm:"))

    logger.info("Bot starting")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
