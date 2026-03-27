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
import sqlite3
import time as systime
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List, Optional, Sequence
from urllib.parse import parse_qs, urlparse
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
    TypeHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

ARXIV_API_URL = "http://export.arxiv.org/api/query"
RXIV_DETAILS_API_URL = "https://api.biorxiv.org/details"
PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
CROSSREF_WORKS_URL = "https://api.crossref.org/works"
OPENALEX_WORKS_URL = "https://api.openalex.org/works"
DEFAULT_MAX_RESULTS = int(os.getenv("MAX_RESULTS", "300"))
TODAY_HOURS_BACK = 24
DAILY_RECAP_HOURS = 24
RESULTS_PAGE_SIZE = max(1, int(os.getenv("RESULTS_PAGE_SIZE", "10")))
DEFAULT_DAILY_RECAP_TIME = "09:00"
DEFAULT_DAILY_RECAP_TIMEZONE = "UTC"
MAX_ABSTRACT_CHARS = int(os.getenv("MAX_ABSTRACT_CHARS", "1600"))
PUBMED_FUTURE_GRACE_DAYS = int(os.getenv("PUBMED_FUTURE_GRACE_DAYS", "2"))
RECAP_TIMEZONE_PAGE_SIZE = max(1, int(os.getenv("RECAP_TIMEZONE_PAGE_SIZE", "8")))
COFFEE_URL = os.getenv("COFFEE_URL", "").strip()
COFFEE_TEXT = os.getenv("COFFEE_TEXT", "Support this bot").strip() or "Support this bot"
COFFEE_EVM_ADDRESS = os.getenv(
    "COFFEE_EVM_ADDRESS",
    "0x566dbD781299dC574e886e8c6072C72eFA01204a",
).strip()
COFFEE_SOLANA_ADDRESS = os.getenv(
    "COFFEE_SOLANA_ADDRESS",
    "BKNb6zJNiL1qpqmZtr9nWsNN2SKq5FHtLtwgoytLpH4e",
).strip()
COFFEE_BTC_ADDRESS = os.getenv(
    "COFFEE_BTC_ADDRESS",
    "bc1qmjk0d9msq40vephdfjd9sz3x4acng0fd6x4jyc",
).strip()
REPORT_TEXT = os.getenv(
    "REPORT_TEXT",
    "Report malfunctions or send feature requests",
).strip() or "Report malfunctions or send feature requests"
REPORT_FORWARD_CHAT_ID = os.getenv("REPORT_FORWARD_CHAT_ID", "").strip()
REPORT_ADMIN_USER_ID = os.getenv("REPORT_ADMIN_USER_ID", "").strip()
FEEDBACK_DAILY_LIMIT = 10
SETTINGS_FILE = Path("bot_settings.json")
METRICS_DB_FILE = Path("bot_metrics.sqlite3")
MENU_BTN_TODAY = "Last 24h"
MENU_BTN_KEYWORDS = "Keywords"
MENU_BTN_ADD_KEYWORDS = "Add keywords"
MENU_BTN_REMOVE_KEYWORDS = "Remove keywords"
MENU_BTN_CLEAR_KEYWORDS = "🧹 Clear keywords"
MENU_BTN_SEARCH_HOURS = "Search Last N Hours"
MENU_BTN_DAILY_RECAP = "Recap On/Off"
MENU_BTN_SET_RECAP_TIME = "Recap Time"
MENU_BTN_RECAP_STATUS = "Recap Status"
MENU_BTN_BOOKMARKS = "Bookmarks"
MENU_BTN_HELP = "Help"
MENU_BTN_REPORT = "Feedback"
MENU_BTN_COFFEE = "Pay me a coffee"
MENU_BTN_MORE = "More"
MENU_BTN_GLOBAL_SEARCH = "Global Search"
MORE_MENU_MESSAGE_TEXT = "Additional features are shown below"
SOURCE_ARXIV = "arxiv"
SOURCE_BIORXIV = "biorxiv"
SOURCE_MEDRXIV = "medrxiv"
SOURCE_CHEMRXIV = "chemrxiv"
SOURCE_SSRN = "ssrn"
SOURCE_PUBMED = "pubmed"
SOURCE_IEEE = "ieee"
KEYWORD_SCOPE_ALL = "all"
WELCOME_SHOWN_AT_KEY = "welcome_shown_at"
SEARCH_SCOPE_TODAY = "today"
SEARCH_SCOPE_HOURS = "hours"
SEARCH_SCOPE_GLOBAL = "global"

ARXIV_FAMILY_SOURCES = {
    SOURCE_ARXIV,
    SOURCE_BIORXIV,
    SOURCE_MEDRXIV,
    SOURCE_CHEMRXIV,
}
ALL_PAPER_SOURCES = ARXIV_FAMILY_SOURCES | {SOURCE_SSRN, SOURCE_IEEE, SOURCE_PUBMED}
KEYWORD_SOURCES_ORDER = [
    SOURCE_ARXIV,
    SOURCE_BIORXIV,
    SOURCE_MEDRXIV,
    SOURCE_CHEMRXIV,
    SOURCE_SSRN,
    SOURCE_IEEE,
    SOURCE_PUBMED,
]
OPENALEX_SOURCE_IDS = {
    SOURCE_BIORXIV: "S4306402567",
    SOURCE_MEDRXIV: "S3005729997",
    SOURCE_CHEMRXIV: "S4393918830",
    SOURCE_SSRN: "S4210172589",
}


def build_main_menu_markup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [MENU_BTN_TODAY, MENU_BTN_SEARCH_HOURS],
            [MENU_BTN_KEYWORDS, MENU_BTN_BOOKMARKS],
            [MENU_BTN_ADD_KEYWORDS, MENU_BTN_REMOVE_KEYWORDS, MENU_BTN_CLEAR_KEYWORDS],
            [MENU_BTN_DAILY_RECAP, MENU_BTN_SET_RECAP_TIME, MENU_BTN_RECAP_STATUS],
            [MENU_BTN_HELP],
            [MENU_BTN_MORE],
        ],
        resize_keyboard=True,
    )


def _keyword_action_label(action: str) -> str:
    action_norm = str(action or "").strip().casefold()
    if action_norm == "add":
        return MENU_BTN_ADD_KEYWORDS
    if action_norm == "remove":
        return MENU_BTN_REMOVE_KEYWORDS
    if action_norm == "clear":
        return MENU_BTN_CLEAR_KEYWORDS
    return "Keywords"


def build_keyword_scope_markup(action: str) -> InlineKeyboardMarkup:
    """Inline submenu to choose where one keyword action should apply."""
    action_norm = str(action or "").strip().casefold()
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text="arXiv", callback_data=f"kwmenu:{action_norm}:{SOURCE_ARXIV}"),
            InlineKeyboardButton(text="bioRxiv", callback_data=f"kwmenu:{action_norm}:{SOURCE_BIORXIV}"),
        ],
        [
            InlineKeyboardButton(text="medRxiv", callback_data=f"kwmenu:{action_norm}:{SOURCE_MEDRXIV}"),
            InlineKeyboardButton(text="ChemRxiv", callback_data=f"kwmenu:{action_norm}:{SOURCE_CHEMRXIV}"),
        ],
        [
            InlineKeyboardButton(text="SSRN", callback_data=f"kwmenu:{action_norm}:{SOURCE_SSRN}"),
            InlineKeyboardButton(text="IEEE", callback_data=f"kwmenu:{action_norm}:{SOURCE_IEEE}"),
        ],
        [
            InlineKeyboardButton(text="PubMed", callback_data=f"kwmenu:{action_norm}:{SOURCE_PUBMED}"),
        ],
        [
            InlineKeyboardButton(text="All sources", callback_data=f"kwmenu:{action_norm}:{KEYWORD_SCOPE_ALL}"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def build_coffee_markup() -> Optional[InlineKeyboardMarkup]:
    if not COFFEE_URL:
        return None
    if not (COFFEE_URL.startswith("https://") or COFFEE_URL.startswith("http://")):
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(text="Open support link", url=COFFEE_URL)]]
    )


def build_more_menu_markup() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text=MENU_BTN_GLOBAL_SEARCH, callback_data="moremenu:globalsearch"),
        ],
        [
            InlineKeyboardButton(text=MENU_BTN_REPORT, callback_data="moremenu:report"),
            InlineKeyboardButton(text=MENU_BTN_COFFEE, callback_data="moremenu:coffee"),
        ]
    ]
    return InlineKeyboardMarkup(rows)


def build_global_search_sources_markup(selected_sources: Sequence[str]) -> InlineKeyboardMarkup:
    selected = {
        source
        for source in (
            str(item or "").strip().casefold()
            for item in selected_sources
        )
        if source in ALL_PAPER_SOURCES
    }

    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text=f"{'✅ ' if SOURCE_ARXIV in selected else ''}arXiv",
                callback_data=f"gsearch:toggle:{SOURCE_ARXIV}",
            ),
            InlineKeyboardButton(
                text=f"{'✅ ' if SOURCE_BIORXIV in selected else ''}bioRxiv",
                callback_data=f"gsearch:toggle:{SOURCE_BIORXIV}",
            ),
        ],
        [
            InlineKeyboardButton(
                text=f"{'✅ ' if SOURCE_MEDRXIV in selected else ''}medRxiv",
                callback_data=f"gsearch:toggle:{SOURCE_MEDRXIV}",
            ),
            InlineKeyboardButton(
                text=f"{'✅ ' if SOURCE_CHEMRXIV in selected else ''}ChemRxiv",
                callback_data=f"gsearch:toggle:{SOURCE_CHEMRXIV}",
            ),
        ],
        [
            InlineKeyboardButton(
                text=f"{'✅ ' if SOURCE_SSRN in selected else ''}SSRN",
                callback_data=f"gsearch:toggle:{SOURCE_SSRN}",
            ),
            InlineKeyboardButton(
                text=f"{'✅ ' if SOURCE_IEEE in selected else ''}IEEE",
                callback_data=f"gsearch:toggle:{SOURCE_IEEE}",
            ),
        ],
        [
            InlineKeyboardButton(
                text=f"{'✅ ' if SOURCE_PUBMED in selected else ''}PubMed",
                callback_data=f"gsearch:toggle:{SOURCE_PUBMED}",
            ),
        ],
        [
            InlineKeyboardButton(text="Select All", callback_data="gsearch:all"),
            InlineKeyboardButton(text="Clear All", callback_data="gsearch:clear"),
        ],
        [
            InlineKeyboardButton(text="Search", callback_data="gsearch:start"),
            InlineKeyboardButton(text="Cancel", callback_data="gsearch:cancel"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def _parse_report_chat_ids(raw_value: Any, *, source_name: str) -> List[int]:
    if raw_value is None:
        return []
    tokens: List[str]
    if isinstance(raw_value, Sequence) and not isinstance(raw_value, (str, bytes, bytearray)):
        tokens = [str(item).strip() for item in raw_value]
    else:
        text = str(raw_value).strip()
        if not text:
            return []
        tokens = [part.strip() for part in re.split(r"[,\s;]+", text) if part.strip()]

    parsed: List[int] = []
    seen: set[int] = set()
    for token in tokens:
        try:
            chat_id = int(token)
        except (TypeError, ValueError):
            logger.warning("Invalid %s value ignored: %r", source_name, token)
            continue
        if chat_id in seen:
            continue
        seen.add(chat_id)
        parsed.append(chat_id)
    return parsed


def get_report_forward_chat_ids() -> List[int]:
    return _parse_report_chat_ids(
        REPORT_FORWARD_CHAT_ID,
        source_name="REPORT_FORWARD_CHAT_ID",
    )


def get_report_forward_chat_id() -> Optional[int]:
    chat_ids = get_report_forward_chat_ids()
    return chat_ids[0] if chat_ids else None


def get_report_admin_user_ids() -> List[int]:
    return _parse_report_chat_ids(
        REPORT_ADMIN_USER_ID,
        source_name="REPORT_ADMIN_USER_ID",
    )


def get_report_admin_user_id() -> Optional[int]:
    user_ids = get_report_admin_user_ids()
    return user_ids[0] if user_ids else None


def _is_admin_user(user_id: Optional[int]) -> bool:
    if user_id is None:
        return False
    return int(user_id) in set(get_report_admin_user_ids())


def set_report_forward_chat_id(chat_id: int) -> None:
    settings = load_settings()
    normalized = int(chat_id)
    settings["report_forward_chat_id"] = normalized
    settings["report_forward_chat_ids"] = [normalized]
    save_settings(settings)


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


def _normalize_utc_datetime(now: Optional[datetime] = None) -> datetime:
    reference = now if now is not None else datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return reference.astimezone(timezone.utc)


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


def _utc_date_key(now: Optional[datetime] = None) -> str:
    return _normalize_utc_datetime(now).date().isoformat()


def _utc_timestamp_text(now: Optional[datetime] = None) -> str:
    return _normalize_utc_datetime(now).isoformat(timespec="seconds")


def _open_metrics_db() -> sqlite3.Connection:
    connection = sqlite3.connect(METRICS_DB_FILE, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _initialize_metrics_db() -> None:
    with _open_metrics_db() as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                last_chat_id INTEGER,
                last_username TEXT,
                last_full_name TEXT,
                recap_enabled INTEGER NOT NULL DEFAULT 0 CHECK (recap_enabled IN (0, 1))
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS user_activity_days (
                user_id INTEGER NOT NULL,
                activity_date TEXT NOT NULL,
                PRIMARY KEY (user_id, activity_date),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_activity_days_date
            ON user_activity_days (activity_date)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_users_recap_enabled
            ON users (recap_enabled)
            """
        )


def _coerce_metrics_timestamp(raw_value: Any, *, fallback: Optional[datetime] = None) -> str:
    fallback_dt = _normalize_utc_datetime(fallback)
    if isinstance(raw_value, str):
        cleaned = raw_value.strip()
        if cleaned:
            normalized = cleaned[:-1] + "+00:00" if cleaned.endswith("Z") else cleaned
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                parsed = None
            if parsed is not None:
                return _utc_timestamp_text(parsed)
    return fallback_dt.isoformat(timespec="seconds")


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _record_user_interaction(
    user_id: int,
    *,
    username: Optional[str] = None,
    full_name: Optional[str] = None,
    chat_id: Optional[int] = None,
    now: Optional[datetime] = None,
) -> None:
    recorded_at = _utc_timestamp_text(now)
    activity_date = _utc_date_key(now)
    clean_username = normalize_text(username or "") or None
    clean_full_name = normalize_text(full_name or "") or None

    with _open_metrics_db() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO users (
                user_id,
                first_seen_at,
                last_seen_at,
                last_chat_id,
                last_username,
                last_full_name,
                recap_enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, 0)
            """,
            (user_id, recorded_at, recorded_at, chat_id, clean_username, clean_full_name),
        )
        connection.execute(
            """
            UPDATE users
            SET last_seen_at = ?,
                last_chat_id = ?,
                last_username = ?,
                last_full_name = ?
            WHERE user_id = ?
            """,
            (recorded_at, chat_id, clean_username, clean_full_name, user_id),
        )
        # Keep one row per user per UTC day so DAU/WAU/MAU queries stay cheap
        # without writing one activity event for every message or button press.
        connection.execute(
            """
            INSERT OR IGNORE INTO user_activity_days (user_id, activity_date)
            VALUES (?, ?)
            """,
            (user_id, activity_date),
        )


def _set_metrics_recap_enabled(
    user_id: int,
    enabled: bool,
    *,
    chat_id: Optional[int] = None,
    now: Optional[datetime] = None,
    first_seen_at: Optional[str] = None,
) -> None:
    recorded_at = first_seen_at or _utc_timestamp_text(now)

    with _open_metrics_db() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO users (
                user_id,
                first_seen_at,
                last_seen_at,
                last_chat_id,
                last_username,
                last_full_name,
                recap_enabled
            )
            VALUES (?, ?, ?, ?, NULL, NULL, ?)
            """,
            (user_id, recorded_at, recorded_at, chat_id, 1 if enabled else 0),
        )
        connection.execute(
            """
            UPDATE users
            SET recap_enabled = ?,
                last_chat_id = COALESCE(?, last_chat_id)
            WHERE user_id = ?
            """,
            (1 if enabled else 0, chat_id, user_id),
        )


def _sync_metrics_users_from_settings() -> None:
    settings = load_settings()
    users = settings.get("users", {})
    if not isinstance(users, dict):
        return

    fallback_now = _normalize_utc_datetime()
    for user_id_str, user_settings in users.items():
        try:
            user_id = int(user_id_str)
        except (TypeError, ValueError):
            continue
        if not isinstance(user_settings, dict):
            continue

        first_seen_at = _coerce_metrics_timestamp(
            user_settings.get(WELCOME_SHOWN_AT_KEY),
            fallback=fallback_now,
        )
        chat_id = _coerce_int(user_settings.get("daily_recap_chat_id"))
        _set_metrics_recap_enabled(
            user_id,
            bool(user_settings.get("daily_recap_enabled", False)),
            chat_id=chat_id,
            first_seen_at=first_seen_at,
        )


def _get_user_metrics_summary(*, now: Optional[datetime] = None) -> dict[str, Any]:
    reference_time = _normalize_utc_datetime(now)
    today = reference_time.date()
    wau_start = (today - timedelta(days=6)).isoformat()
    mau_start = (today - timedelta(days=29)).isoformat()
    today_key = today.isoformat()

    with _open_metrics_db() as connection:
        total_users = int(connection.execute("SELECT COUNT(*) FROM users").fetchone()[0])
        daily_active_users = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM user_activity_days
                WHERE activity_date = ?
                """,
                (today_key,),
            ).fetchone()[0]
        )
        weekly_active_users = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM user_activity_days
                WHERE activity_date >= ?
                """,
                (wau_start,),
            ).fetchone()[0]
        )
        monthly_active_users = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM user_activity_days
                WHERE activity_date >= ?
                """,
                (mau_start,),
            ).fetchone()[0]
        )
        recap_enabled_users = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM users
                WHERE recap_enabled = 1
                """
            ).fetchone()[0]
        )

    return {
        "total_users": total_users,
        "daily_active_users": daily_active_users,
        "weekly_active_users": weekly_active_users,
        "monthly_active_users": monthly_active_users,
        "recap_enabled_users": recap_enabled_users,
        "as_of_utc": reference_time.isoformat(timespec="seconds"),
    }


def _coerce_feedback_daily_usage(value: Any, *, current_date: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"date": current_date, "count": 0}

    raw_date = str(value.get("date", "")).strip()
    try:
        count = int(value.get("count", 0))
    except (TypeError, ValueError):
        count = 0

    if count < 0:
        count = 0

    if raw_date != current_date:
        return {"date": current_date, "count": 0}

    return {"date": current_date, "count": count}


def _get_feedback_daily_usage(
    user_id: int,
    user_data: Optional[dict[str, Any]] = None,
    *,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    current_date = _utc_date_key(now)
    cache_key = "feedback_daily_usage"

    if user_data is not None and cache_key in user_data:
        usage = _coerce_feedback_daily_usage(user_data[cache_key], current_date=current_date)
        user_data[cache_key] = usage
        return usage

    settings = load_settings()
    user_settings = _get_user_settings(settings, user_id)
    usage = _coerce_feedback_daily_usage(
        user_settings.get(cache_key),
        current_date=current_date,
    )
    if user_data is not None:
        user_data[cache_key] = usage
    return usage


def _remaining_feedback_slots(
    user_id: int,
    user_data: Optional[dict[str, Any]] = None,
    *,
    now: Optional[datetime] = None,
) -> int:
    usage = _get_feedback_daily_usage(user_id, user_data, now=now)
    return max(0, FEEDBACK_DAILY_LIMIT - int(usage["count"]))


def _record_feedback_submission(
    user_id: int,
    user_data: Optional[dict[str, Any]] = None,
    *,
    now: Optional[datetime] = None,
) -> int:
    current_date = _utc_date_key(now)
    usage = _get_feedback_daily_usage(user_id, user_data, now=now)
    updated = {
        "date": current_date,
        "count": min(FEEDBACK_DAILY_LIMIT, int(usage["count"]) + 1),
    }
    if user_data is not None:
        user_data["feedback_daily_usage"] = updated
    _save_user_setting(user_id, "feedback_daily_usage", updated)
    return int(updated["count"])


def _feedback_limit_reached_text() -> str:
    return (
        f"You have already sent {FEEDBACK_DAILY_LIMIT} feedback messages today. "
        "Please try again tomorrow."
    )


def _get_feedback_submission_lock(application: Application, user_id: int) -> asyncio.Lock:
    locks = application.bot_data.get("_feedback_submission_locks")
    if not isinstance(locks, dict):
        locks = {}
        application.bot_data["_feedback_submission_locks"] = locks

    lock = locks.get(user_id)
    if not isinstance(lock, asyncio.Lock):
        lock = asyncio.Lock()
        locks[user_id] = lock
    return lock


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
        if source in ALL_PAPER_SOURCES and paper_id:
            return source, paper_id

    return default_source, token


def make_paper_ref(source: str, paper_id: str) -> str:
    source_norm = str(source or "").strip().casefold()
    if source_norm not in ALL_PAPER_SOURCES:
        source_norm = SOURCE_ARXIV
    return f"{source_norm}:{(paper_id or '').strip()}"


def paper_ref_for(paper: Paper) -> str:
    return make_paper_ref(paper.source, paper.arxiv_id)


def paper_source_label(source: str) -> str:
    source_norm = str(source).casefold()
    if source_norm == SOURCE_PUBMED:
        return "PubMed"
    if source_norm == SOURCE_IEEE:
        return "IEEE"
    if source_norm == SOURCE_BIORXIV:
        return "bioRxiv"
    if source_norm == SOURCE_MEDRXIV:
        return "medRxiv"
    if source_norm == SOURCE_CHEMRXIV:
        return "ChemRxiv"
    if source_norm == SOURCE_SSRN:
        return "SSRN"
    return "arXiv"


def keyword_source_label(source: str) -> str:
    source_norm = str(source).casefold()
    if source_norm == KEYWORD_SCOPE_ALL:
        return "All sources"
    return paper_source_label(source_norm)


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
    if token in {"biorxiv", "bio", "bx"}:
        return SOURCE_BIORXIV
    if token in {"medrxiv", "med", "mx"}:
        return SOURCE_MEDRXIV
    if token in {"chemrxiv", "chem", "cx"}:
        return SOURCE_CHEMRXIV
    if token in {"ssrn", "sr"}:
        return SOURCE_SSRN
    if token in {"ieee", "ie", "xplore", "ix"}:
        return SOURCE_IEEE
    if token in {"pubmed", "pm"}:
        return SOURCE_PUBMED
    if token in {"all", "*"}:
        return KEYWORD_SCOPE_ALL
    return None


def _keyword_cache_key_for_source(source: str) -> str:
    source_norm = str(source or "").strip().casefold()
    if source_norm not in ALL_PAPER_SOURCES:
        source_norm = SOURCE_ARXIV
    return f"custom_keywords_{source_norm}"


def keyword_sources() -> List[str]:
    return [source for source in KEYWORD_SOURCES_ORDER if source in ALL_PAPER_SOURCES]


def get_keywords_for_source(
    source: str,
    user_data: Optional[dict[str, Any]] = None,
    user_id: Optional[int] = None,
) -> List[str]:
    source_norm = str(source or "").strip().casefold()
    if source_norm not in ALL_PAPER_SOURCES:
        source_norm = SOURCE_ARXIV
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
        if source_norm == SOURCE_ARXIV and "custom_keywords" in user_settings:
            keywords = list(user_settings["custom_keywords"])
            if user_data is not None:
                user_data[cache_key] = keywords
            return keywords
        # Backward compatibility: old setup had one shared arXiv-family list.
        if (
            source_norm in ARXIV_FAMILY_SOURCES
            and source_norm != SOURCE_ARXIV
            and _keyword_cache_key_for_source(SOURCE_ARXIV) in user_settings
        ):
            keywords = list(user_settings[_keyword_cache_key_for_source(SOURCE_ARXIV)])
            if user_data is not None:
                user_data[cache_key] = keywords
            return keywords

    env_name_map = {
        SOURCE_ARXIV: "ARXIV_KEYWORDS",
        SOURCE_BIORXIV: "BIORXIV_KEYWORDS",
        SOURCE_MEDRXIV: "MEDRXIV_KEYWORDS",
        SOURCE_CHEMRXIV: "CHEMRXIV_KEYWORDS",
        SOURCE_SSRN: "SSRN_KEYWORDS",
        SOURCE_IEEE: "IEEE_KEYWORDS",
        SOURCE_PUBMED: "PUBMED_KEYWORDS",
    }
    raw = os.getenv(env_name_map[source_norm], "").strip()
    if not raw and source_norm in ARXIV_FAMILY_SOURCES and source_norm != SOURCE_ARXIV:
        # Backward compatibility: shared ARXIV_KEYWORDS for the preprint family.
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
        source: get_keywords_for_source(
            source,
            user_data=user_data,
            user_id=user_id,
        )
        for source in keyword_sources()
    }


def set_keywords_for_source(
    user_id: int,
    source: str,
    keywords: Sequence[str],
    user_data: Optional[dict[str, Any]] = None,
) -> List[str]:
    source_norm = str(source or "").strip().casefold()
    if source_norm not in ALL_PAPER_SOURCES:
        source_norm = SOURCE_ARXIV
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
    _save_user_setting(user_id, cache_key, cleaned)
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

    multiline_items: List[str] = []
    if "\n" in raw:
        for line in raw.splitlines():
            item = line.strip()
            if not item:
                continue
            bullet_match = re.match(r"^[-*•]\s+(.*)$", item)
            if bullet_match is not None:
                item = bullet_match.group(1).strip()
            multiline_items.append(item)

    if len(multiline_items) > 1:
        items = multiline_items
    elif "," in raw:
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
        # Keep '+' as logical AND operator but normalize spacing so duplicate
        # detection is stable across inputs like "a+b" and "a + b".
        item = re.sub(r"\s*\+\s*", " + ", item).strip()
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
    if token == SEARCH_SCOPE_GLOBAL:
        return SEARCH_SCOPE_GLOBAL
    if token == SEARCH_SCOPE_TODAY:
        return SEARCH_SCOPE_TODAY
    return SEARCH_SCOPE_HOURS


def _describe_search_window(scope: str, hours_back: int) -> str:
    normalized_scope = _normalize_search_scope(scope)
    if normalized_scope == SEARCH_SCOPE_GLOBAL:
        return "all time"
    if normalized_scope == SEARCH_SCOPE_TODAY:
        return f"the last {TODAY_HOURS_BACK} hours"
    return f"the last {int(hours_back)} hours"


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


def _keywords_match_text(keywords: Sequence[str], text: str) -> bool:
    """Return True when text matches OR-of-clauses keyword logic.

    Each keyword entry is one OR clause. Inside one entry, '+' means AND.
    """
    haystack = normalize_text(text).casefold()
    if not keywords:
        return True
    if not haystack:
        return False

    for keyword in keywords:
        terms = [normalize_text(term).casefold() for term in str(keyword).split("+")]
        terms = [term for term in terms if term]
        if terms and all(term in haystack for term in terms):
            return True
    return False


def _keywords_to_search_query(keywords: Sequence[str]) -> str:
    terms: List[str] = []
    seen: set[str] = set()
    for keyword in keywords:
        for part in str(keyword).split("+"):
            cleaned = normalize_text(part).strip('"').strip("'").strip()
            if not cleaned:
                continue
            key = cleaned.casefold()
            if key in seen:
                continue
            seen.add(key)
            terms.append(cleaned)
    return " ".join(terms)


def _parse_datetime_or_none(raw: Any) -> Optional[datetime]:
    text = normalize_text(str(raw or ""))
    if not text:
        return None
    try:
        parsed = dateparser.parse(text)
    except Exception:
        return None
    if parsed is None:
        return None
    return ensure_utc(parsed)


def _parse_crossref_date_parts(node: Any) -> Optional[datetime]:
    if not isinstance(node, dict):
        return None
    date_parts = node.get("date-parts")
    if not isinstance(date_parts, list) or not date_parts:
        return None
    first = date_parts[0]
    if not isinstance(first, list) or not first:
        return None
    try:
        year = int(first[0])
        month = int(first[1]) if len(first) > 1 else 1
        day = int(first[2]) if len(first) > 2 else 1
        return datetime(year=year, month=month, day=day, tzinfo=timezone.utc)
    except Exception:
        return None


def _rxiv_record_to_paper(
    record: dict[str, Any],
    *,
    source: str,
    host: str,
) -> Optional[Paper]:
    doi = normalize_text(str(record.get("doi") or ""))
    if not doi:
        return None

    version_raw = normalize_text(str(record.get("version") or ""))
    version_suffix = f"v{version_raw}" if version_raw and version_raw.isdigit() else ""
    paper_id = f"{doi}{version_suffix}" if version_suffix else doi

    published = _parse_datetime_or_none(record.get("date"))
    if published is None:
        published = _parse_datetime_or_none(record.get("published"))
    if published is None:
        return None
    updated = published

    authors_raw = normalize_text(str(record.get("authors") or ""))
    if ";" in authors_raw:
        authors = [normalize_text(item) for item in authors_raw.split(";") if normalize_text(item)]
    else:
        authors = [authors_raw] if authors_raw else []

    title = normalize_text(str(record.get("title") or ""))
    if not title:
        title = "(untitled)"
    summary = normalize_text(str(record.get("abstract") or ""))

    category = normalize_text(str(record.get("category") or ""))
    content_token = f"{doi}{version_suffix}" if version_suffix else doi
    link_abs = f"https://{host}/content/{content_token}"
    link_pdf = f"{link_abs}.full.pdf"

    return Paper(
        index=0,
        arxiv_id=paper_id,
        title=title,
        summary=summary,
        authors=authors,
        published=published,
        updated=updated,
        published_raw=published.strftime("%Y-%m-%d"),
        updated_raw=updated.strftime("%Y-%m-%d"),
        primary_category=category,
        link_abs=link_abs,
        link_pdf=link_pdf,
        source=source,
    )


def fetch_recent_rxiv_papers(
    source: str,
    keywords: Sequence[str],
    hours_back: int,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    return fetch_rxiv_papers(
        source=source,
        keywords=keywords,
        hours_back=hours_back,
        max_results=max_results,
    )


def fetch_rxiv_papers(
    source: str,
    keywords: Sequence[str],
    hours_back: Optional[int] = None,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    source_norm = str(source).casefold()
    if source_norm not in {SOURCE_BIORXIV, SOURCE_MEDRXIV}:
        return [], "", 0
    # bioRxiv/medRxiv's official details endpoint exposes date windows but no
    # title/abstract keyword query. Use OpenAlex source-scoped search so these
    # sources match arXiv's "keyword query first, local recency filter second"
    # behavior.
    return fetch_openalex_preprint_papers(
        source=source_norm,
        keywords=keywords,
        hours_back=hours_back,
        max_results=max_results,
    )


def _crossref_item_to_preprint_paper(
    item: dict[str, Any],
    *,
    source: str,
) -> Optional[Paper]:
    doi = normalize_text(str(item.get("DOI") or ""))
    if not doi:
        return None

    title_list = item.get("title", [])
    title = ""
    if isinstance(title_list, list) and title_list:
        title = normalize_text(str(title_list[0] or ""))
    if not title:
        title = "(untitled)"

    abstract_raw = str(item.get("abstract") or "")
    abstract_clean = html.unescape(abstract_raw)
    abstract_clean = re.sub(r"<[^>]+>", " ", abstract_clean)
    summary = normalize_text(abstract_clean)

    authors: List[str] = []
    for author in item.get("author", []):
        if not isinstance(author, dict):
            continue
        given = normalize_text(str(author.get("given") or ""))
        family = normalize_text(str(author.get("family") or ""))
        full = normalize_text(" ".join(part for part in [given, family] if part))
        if full:
            authors.append(full)

    published_candidates = [
        _parse_datetime_or_none(item.get("created", {}).get("date-time") if isinstance(item.get("created"), dict) else None),
        _parse_datetime_or_none(item.get("published-online", {}).get("date-time") if isinstance(item.get("published-online"), dict) else None),
        _parse_datetime_or_none(item.get("published-print", {}).get("date-time") if isinstance(item.get("published-print"), dict) else None),
        _parse_datetime_or_none(item.get("indexed", {}).get("date-time") if isinstance(item.get("indexed"), dict) else None),
        _parse_crossref_date_parts(item.get("posted")),
        _parse_crossref_date_parts(item.get("published-online")),
        _parse_crossref_date_parts(item.get("published-print")),
        _parse_crossref_date_parts(item.get("issued")),
    ]
    published = next((candidate for candidate in published_candidates if candidate is not None), None)
    if published is None:
        return None

    updated_candidates = [
        _parse_datetime_or_none(item.get("updated", {}).get("date-time") if isinstance(item.get("updated"), dict) else None),
        _parse_datetime_or_none(item.get("indexed", {}).get("date-time") if isinstance(item.get("indexed"), dict) else None),
        _parse_datetime_or_none(item.get("created", {}).get("date-time") if isinstance(item.get("created"), dict) else None),
        published,
    ]
    updated = next((candidate for candidate in updated_candidates if candidate is not None), published)

    link_abs = normalize_text(str(item.get("URL") or ""))
    if not link_abs:
        link_abs = f"https://doi.org/{doi}"

    link_pdf = ""
    for link in item.get("link", []):
        if not isinstance(link, dict):
            continue
        content_type = normalize_text(str(link.get("content-type") or "")).casefold()
        url = normalize_text(str(link.get("URL") or ""))
        if not url:
            continue
        if "pdf" in content_type or url.endswith(".pdf"):
            link_pdf = url
            break
    if not link_pdf:
        link_pdf = _guess_pdf_link_for_source(source=source, paper_id=doi, link_abs=link_abs)

    category = ""
    container_titles = item.get("container-title", [])
    if isinstance(container_titles, list) and container_titles:
        category = normalize_text(str(container_titles[0] or ""))

    return Paper(
        index=0,
        arxiv_id=doi,
        title=title,
        summary=summary,
        authors=authors,
        published=published,
        updated=updated,
        published_raw=published.strftime("%Y-%m-%d"),
        updated_raw=updated.strftime("%Y-%m-%d"),
        primary_category=category,
        link_abs=link_abs,
        link_pdf=link_pdf,
        source=source,
    )


def fetch_crossref_preprint_papers(
    source: str,
    doi_prefix: str,
    keywords: Sequence[str],
    hours_back: Optional[int] = None,
    query_text: str = "",
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    rows = max(1, min(100, int(max_results)))
    cursor = "*"
    papers: List[Paper] = []
    seen_refs: set[str] = set()
    request_urls: List[str] = []
    raw_count = 0
    now_utc = datetime.now(timezone.utc)
    cutoff = None
    if hours_back is not None and int(hours_back) > 0:
        cutoff = now_utc - timedelta(hours=int(hours_back))
    filter_tokens = [f"prefix:{doi_prefix}"]
    if cutoff is not None:
        from_date = (cutoff - timedelta(days=2)).date().isoformat()
        to_date = now_utc.date().isoformat()
        filter_tokens.extend(
            [
                f"from-created-date:{from_date}",
                f"until-created-date:{to_date}",
            ]
        )

    # Cursor pagination; keep a hard cap to avoid runaway loops.
    for _ in range(20):
        params = {
            "filter": ",".join(filter_tokens),
            "sort": "updated",
            "order": "desc",
            "rows": rows,
            "cursor": cursor,
            "select": (
                "DOI,title,abstract,author,created,indexed,"
                "published-online,published-print,posted,issued,URL,link,container-title"
            ),
        }
        if query_text:
            params["query.bibliographic"] = query_text
        response = requests.get(
            CROSSREF_WORKS_URL,
            params=params,
            timeout=30,
            headers={"User-Agent": "telegram-arxiv-codex-bot/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        request_urls.append(response.url)

        message = payload.get("message", {})
        if not isinstance(message, dict):
            break
        items = message.get("items", [])
        if not isinstance(items, list) or not items:
            break

        raw_count += len(items)
        for item in items:
            if not isinstance(item, dict):
                continue
            paper = _crossref_item_to_preprint_paper(item, source=source)
            if paper is None:
                continue
            recency = max(paper.published, paper.updated)
            if cutoff is not None and recency < cutoff:
                continue
            if not _keywords_match_text(keywords, f"{paper.title}\n{paper.summary}"):
                continue
            ref = paper_ref_for(paper)
            if ref in seen_refs:
                continue
            seen_refs.add(ref)
            papers.append(paper)
            if len(papers) >= max_results:
                break

        if len(papers) >= max_results:
            break
        next_cursor = normalize_text(str(message.get("next-cursor") or ""))
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor

    papers.sort(key=lambda paper: max(paper.published, paper.updated), reverse=True)
    for idx, paper in enumerate(papers, start=1):
        paper.index = idx

    request_url = "\n".join(request_urls)
    return papers, request_url, raw_count


def fetch_recent_crossref_preprint_papers(
    source: str,
    doi_prefix: str,
    keywords: Sequence[str],
    hours_back: int,
    query_text: str = "",
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    return fetch_crossref_preprint_papers(
        source=source,
        doi_prefix=doi_prefix,
        keywords=keywords,
        hours_back=hours_back,
        query_text=query_text,
        max_results=max_results,
    )


def fetch_recent_chemrxiv_papers(
    keywords: Sequence[str],
    hours_back: int,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    return fetch_openalex_preprint_papers(
        source=SOURCE_CHEMRXIV,
        keywords=keywords,
        hours_back=hours_back,
        max_results=max_results,
    )


def fetch_chemrxiv_papers(
    keywords: Sequence[str],
    hours_back: Optional[int] = None,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    return fetch_openalex_preprint_papers(
        source=SOURCE_CHEMRXIV,
        keywords=keywords,
        hours_back=hours_back,
        max_results=max_results,
    )


def fetch_recent_ieee_papers(
    keywords: Sequence[str],
    hours_back: int,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    # IEEE content is indexed in Crossref primarily under DOI prefix 10.1109.
    query_text = _keywords_to_search_query(keywords)
    return fetch_recent_crossref_preprint_papers(
        source=SOURCE_IEEE,
        doi_prefix="10.1109",
        keywords=keywords,
        hours_back=hours_back,
        query_text=query_text,
        max_results=max_results,
    )


def fetch_ieee_papers(
    keywords: Sequence[str],
    hours_back: Optional[int] = None,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    query_text = _keywords_to_search_query(keywords)
    return fetch_crossref_preprint_papers(
        source=SOURCE_IEEE,
        doi_prefix="10.1109",
        keywords=keywords,
        hours_back=hours_back,
        query_text=query_text,
        max_results=max_results,
    )


def _openalex_abstract_from_inverted_index(raw: Any) -> str:
    if not isinstance(raw, dict) or not raw:
        return ""
    max_pos = -1
    for positions in raw.values():
        if not isinstance(positions, list):
            continue
        for pos in positions:
            if isinstance(pos, int) and pos > max_pos:
                max_pos = pos
    if max_pos < 0:
        return ""
    tokens = [""] * (max_pos + 1)
    for word, positions in raw.items():
        if not isinstance(word, str) or not isinstance(positions, list):
            continue
        for pos in positions:
            if isinstance(pos, int) and 0 <= pos < len(tokens) and not tokens[pos]:
                tokens[pos] = word
    return normalize_text(" ".join(token for token in tokens if token))


def _openalex_item_to_preprint_paper(item: dict[str, Any], *, source: str) -> Optional[Paper]:
    doi_raw = normalize_text(str(item.get("doi") or ""))
    doi = doi_raw.removeprefix("https://doi.org/").removeprefix("http://doi.org/")
    doi = normalize_text(doi).strip()
    if not doi:
        return None

    title = normalize_text(str(item.get("title") or "")) or "(untitled)"
    summary = _openalex_abstract_from_inverted_index(item.get("abstract_inverted_index"))

    authors: List[str] = []
    for authorship in item.get("authorships", []):
        if not isinstance(authorship, dict):
            continue
        author = authorship.get("author")
        if not isinstance(author, dict):
            continue
        display_name = normalize_text(str(author.get("display_name") or ""))
        if display_name:
            authors.append(display_name)

    # For user-facing "last N hours" windows we must use the real publication
    # date, not indexing/creation timestamps that can shift much later.
    published = _parse_datetime_or_none(item.get("publication_date"))
    if published is None:
        return None
    updated = _parse_datetime_or_none(item.get("updated_date")) or published

    primary_location = item.get("primary_location")
    best_oa_location = item.get("best_oa_location")
    content_urls = item.get("content_urls")
    link_pdf = ""
    if isinstance(primary_location, dict):
        link_pdf = normalize_text(str(primary_location.get("pdf_url") or ""))
    if not link_pdf and isinstance(best_oa_location, dict):
        link_pdf = normalize_text(str(best_oa_location.get("pdf_url") or ""))
    if not link_pdf and isinstance(content_urls, dict):
        link_pdf = normalize_text(str(content_urls.get("pdf") or ""))

    link_abs = ""
    if isinstance(primary_location, dict):
        link_abs = normalize_text(str(primary_location.get("landing_page_url") or ""))
    if not link_abs and isinstance(best_oa_location, dict):
        link_abs = normalize_text(str(best_oa_location.get("landing_page_url") or ""))
    if not link_abs:
        link_abs = f"https://doi.org/{doi}"
    if not link_pdf:
        link_pdf = _guess_pdf_link_for_source(source=source, paper_id=doi, link_abs=link_abs)

    category = ""
    primary_topic = item.get("primary_topic")
    if isinstance(primary_topic, dict):
        category = normalize_text(str(primary_topic.get("display_name") or ""))
    if not category and isinstance(primary_location, dict):
        source_node = primary_location.get("source")
        if isinstance(source_node, dict):
            category = normalize_text(str(source_node.get("display_name") or ""))

    return Paper(
        index=0,
        arxiv_id=doi,
        title=title,
        summary=summary,
        authors=authors,
        published=published,
        updated=updated,
        published_raw=published.strftime("%Y-%m-%d"),
        updated_raw=updated.strftime("%Y-%m-%d"),
        primary_category=category,
        link_abs=link_abs,
        link_pdf=link_pdf,
        source=source,
    )


def _ssrn_abstract_id_from_text(raw: str) -> str:
    token = normalize_text(raw).casefold()
    if not token:
        return ""
    match = re.search(r"10\.2139/ssrn\.?(\d+)", token)
    if match:
        return match.group(1)
    return ""


def _ssrn_abstract_id_from_url(url: str) -> str:
    parsed = urlparse(normalize_text(url))
    if not parsed.netloc:
        return ""
    query = parse_qs(parsed.query or "")
    for key in ("abstract_id", "abstractid"):
        values = query.get(key, [])
        if values:
            candidate = normalize_text(str(values[0]))
            if candidate.isdigit():
                return candidate
    return ""


def _ieee_arnumber_from_url(url: str) -> str:
    parsed = urlparse(normalize_text(url))
    if not parsed.netloc:
        return ""
    query = parse_qs(parsed.query or "")
    query_candidate = normalize_text(str((query.get("arnumber") or [""])[0]))
    if query_candidate.isdigit():
        return query_candidate

    path_match = re.search(r"/document/(\d+)", parsed.path or "")
    if path_match:
        return path_match.group(1)
    return ""


def _guess_pdf_link_for_source(*, source: str, paper_id: str, link_abs: str) -> str:
    source_norm = normalize_text(source).casefold()
    paper_id_norm = normalize_text(paper_id)
    link_abs_norm = normalize_text(link_abs)

    if source_norm == SOURCE_SSRN:
        abstract_id = _ssrn_abstract_id_from_text(paper_id_norm) or _ssrn_abstract_id_from_url(link_abs_norm)
        if abstract_id:
            return f"https://papers.ssrn.com/sol3/Delivery.cfm?abstractid={abstract_id}"
        return ""

    if source_norm == SOURCE_IEEE:
        arnumber = _ieee_arnumber_from_url(link_abs_norm)
        if not arnumber:
            trailing_digits = re.search(r"(\d+)$", paper_id_norm)
            if trailing_digits:
                arnumber = trailing_digits.group(1)
        if arnumber:
            return f"https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber={arnumber}"
        return ""

    return ""


def resolve_paper_pdf_link(paper: Paper) -> str:
    if paper.link_pdf:
        return paper.link_pdf
    guessed = _guess_pdf_link_for_source(
        source=paper.source,
        paper_id=paper.arxiv_id,
        link_abs=paper.link_abs,
    )
    if guessed:
        paper.link_pdf = guessed
    return guessed


def _keywords_to_openalex_search_query(keywords: Sequence[str]) -> str:
    clauses: List[str] = []
    seen_clauses: set[str] = set()
    for keyword in keywords:
        term_clauses: List[str] = []
        for part in str(keyword).split("+"):
            cleaned = normalize_text(part).strip('"').strip("'").strip()
            if not cleaned:
                continue
            sanitized = cleaned.replace('"', "")
            if not sanitized:
                continue
            term_clauses.append(f'"{sanitized}"')

        if not term_clauses:
            continue

        clause = term_clauses[0] if len(term_clauses) == 1 else "(" + " AND ".join(term_clauses) + ")"
        clause_key = clause.casefold()
        if clause_key in seen_clauses:
            continue
        seen_clauses.add(clause_key)
        clauses.append(clause)

    if not clauses:
        return ""
    if len(clauses) == 1:
        return clauses[0]
    return "(" + " OR ".join(clauses) + ")"


def _openalex_preprint_window_timestamp(paper: Paper) -> datetime:
    if paper.source.casefold() == SOURCE_SSRN:
        return _paper_recency_timestamp(paper, prefer_updated_for_arxiv=False)
    return paper.published


def fetch_openalex_preprint_papers(
    source: str,
    keywords: Sequence[str],
    hours_back: Optional[int] = None,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    source_norm = str(source).casefold()
    source_id = OPENALEX_SOURCE_IDS.get(source_norm, "")
    search_query = _keywords_to_openalex_search_query(keywords)
    if not source_id or not search_query:
        return [], "", 0

    cutoff = None
    if hours_back is not None and int(hours_back) > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=int(hours_back))

    rows = max(1, min(200, int(max_results)))
    cursor = "*"
    papers: List[Paper] = []
    seen_refs: set[str] = set()
    request_urls: List[str] = []
    raw_count = 0

    # Cursor pagination; keep a hard cap to avoid runaway loops.
    for _ in range(20):
        params: dict[str, Any] = {
            "filter": f"primary_location.source.id:{source_id}",
            "per-page": rows,
            "cursor": cursor,
            "sort": "publication_date:desc",
            "search": search_query,
        }

        openalex_mailto = normalize_text(os.getenv("OPENALEX_MAILTO", ""))
        if openalex_mailto:
            params["mailto"] = openalex_mailto

        response: Optional[requests.Response] = None
        for attempt in range(3):
            candidate = requests.get(
                OPENALEX_WORKS_URL,
                params=params,
                timeout=30,
                headers={"User-Agent": "telegram-arxiv-codex-bot/1.0"},
            )
            if candidate.status_code != 429:
                response = candidate
                break
            # Respect rate limiting with a small exponential backoff.
            wait_seconds = 2 * (attempt + 1)
            logger.warning("OpenAlex rate-limited request. Retrying in %ss.", wait_seconds)
            systime.sleep(wait_seconds)
            response = candidate

        if response is None:
            break
        response.raise_for_status()
        payload = response.json()
        request_urls.append(response.url)

        items = payload.get("results", [])
        if not isinstance(items, list) or not items:
            break

        raw_count += len(items)
        reached_older_records = False
        for item in items:
            if not isinstance(item, dict):
                continue
            paper = _openalex_item_to_preprint_paper(item, source=source_norm)
            if paper is None:
                continue

            recency = _openalex_preprint_window_timestamp(paper)
            if cutoff is not None and recency < cutoff:
                reached_older_records = True
                break

            ref = paper_ref_for(paper)
            if ref in seen_refs:
                continue
            seen_refs.add(ref)
            papers.append(paper)
            if len(papers) >= max_results:
                break

        if len(papers) >= max_results or reached_older_records:
            break

        meta = payload.get("meta", {})
        if not isinstance(meta, dict):
            break
        next_cursor = normalize_text(str(meta.get("next_cursor") or ""))
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor

    papers.sort(key=_openalex_preprint_window_timestamp, reverse=True)
    for idx, paper in enumerate(papers, start=1):
        paper.index = idx

    request_url = "\n".join(request_urls)
    return papers, request_url, raw_count


def _fetch_recent_openalex_preprint_papers(
    source: str,
    keywords: Sequence[str],
    hours_back: int,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    return fetch_openalex_preprint_papers(
        source=source,
        keywords=keywords,
        hours_back=hours_back,
        max_results=max_results,
    )


def fetch_recent_ssrn_papers_openalex(
    keywords: Sequence[str],
    hours_back: int,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=hours_back)

    rows = max(1, min(200, int(max_results)))
    cursor = "*"
    papers: List[Paper] = []
    seen_refs: set[str] = set()
    request_urls: List[str] = []
    raw_count = 0
    search_query = _keywords_to_openalex_search_query(keywords)

    def _ssrn_window_timestamp(paper: Paper) -> datetime:
        return _paper_recency_timestamp(paper, prefer_updated_for_arxiv=False)

    # Cursor pagination; keep a hard cap to avoid runaway loops.
    for _ in range(20):
        params: dict[str, Any] = {
            "filter": "doi_starts_with:10.2139",
            "per-page": rows,
            "cursor": cursor,
            "sort": "publication_date:desc",
        }
        if search_query:
            params["search"] = search_query

        openalex_mailto = normalize_text(os.getenv("OPENALEX_MAILTO", ""))
        if openalex_mailto:
            params["mailto"] = openalex_mailto

        response: Optional[requests.Response] = None
        for attempt in range(3):
            candidate = requests.get(
                OPENALEX_WORKS_URL,
                params=params,
                timeout=30,
                headers={"User-Agent": "telegram-arxiv-codex-bot/1.0"},
            )
            if candidate.status_code != 429:
                response = candidate
                break
            # Respect rate limiting with a small exponential backoff.
            wait_seconds = 2 * (attempt + 1)
            logger.warning("OpenAlex rate-limited request. Retrying in %ss.", wait_seconds)
            systime.sleep(wait_seconds)
            response = candidate

        if response is None:
            break
        response.raise_for_status()
        payload = response.json()
        request_urls.append(response.url)

        items = payload.get("results", [])
        if not isinstance(items, list) or not items:
            break

        raw_count += len(items)
        for item in items:
            if not isinstance(item, dict):
                continue
            paper = _openalex_item_to_preprint_paper(item, source=SOURCE_SSRN)
            if paper is None:
                continue
            if not paper.arxiv_id.casefold().startswith("10.2139/"):
                continue

            if _ssrn_window_timestamp(paper) < cutoff:
                continue
            if not _keywords_match_text(keywords, f"{paper.title}\n{paper.summary}"):
                continue
            ref = paper_ref_for(paper)
            if ref in seen_refs:
                continue
            seen_refs.add(ref)
            papers.append(paper)
            if len(papers) >= max_results:
                break

        if len(papers) >= max_results:
            break

        meta = payload.get("meta", {})
        if not isinstance(meta, dict):
            break
        next_cursor = normalize_text(str(meta.get("next_cursor") or ""))
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor

    papers.sort(key=_ssrn_window_timestamp, reverse=True)
    for idx, paper in enumerate(papers, start=1):
        paper.index = idx

    request_url = "\n".join(request_urls)
    return papers, request_url, raw_count


def fetch_recent_ssrn_papers(
    keywords: Sequence[str],
    hours_back: int,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    # SSRN is fetched via Crossref DOI prefix 10.2139.
    query_text = _keywords_to_search_query(keywords)
    return fetch_recent_crossref_preprint_papers(
        source=SOURCE_SSRN,
        doi_prefix="10.2139",
        keywords=keywords,
        hours_back=hours_back,
        query_text=query_text,
        max_results=max_results,
    )


def fetch_ssrn_papers(
    keywords: Sequence[str],
    hours_back: Optional[int] = None,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    query_text = _keywords_to_search_query(keywords)
    return fetch_crossref_preprint_papers(
        source=SOURCE_SSRN,
        doi_prefix="10.2139",
        keywords=keywords,
        hours_back=hours_back,
        query_text=query_text,
        max_results=max_results,
    )


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


def fetch_pubmed_papers(
    query: str,
    hours_back: Optional[int] = None,
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
    filtered = list(papers)
    if hours_back is not None and int(hours_back) > 0:
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(hours=int(hours_back))
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


def fetch_recent_pubmed_papers(
    query: str,
    hours_back: int,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[Paper], str, int]:
    return fetch_pubmed_papers(
        query=query,
        hours_back=hours_back,
        max_results=max_results,
    )


def fetch_arxiv_entries(
    query: str,
    max_results: int = DEFAULT_MAX_RESULTS,
    sort_by: str = "submittedDate",
) -> tuple[list[Any], str]:
    params = {
        "search_query": query,
        "start": 0,
        "max_results": max_results,
        "sortBy": sort_by,
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


def entries_to_recent_papers(
    entries: Sequence[Any],
    hours_back: int,
    *,
    use_updated: bool = False,
) -> List[Paper]:
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=hours_back)

    papers: List[Paper] = []

    for entry in entries:
        paper = entry_to_paper(entry, index=len(papers) + 1)
        if paper is None:
            continue

        recency = paper.updated if use_updated else paper.published
        if recency < cutoff:
            break

        papers.append(paper)

    return papers


def entries_to_papers(entries: Sequence[Any]) -> List[Paper]:
    """Convert arXiv API entries to papers without applying a time cutoff."""
    papers: List[Paper] = []
    for entry in entries:
        paper = entry_to_paper(entry, index=len(papers) + 1)
        if paper is None:
            continue
        papers.append(paper)
    return papers


def _paper_recency_timestamp(
    paper: Paper,
    *,
    prefer_updated_for_arxiv: bool = False,
) -> datetime:
    """Return the timestamp used to order paper results in user-facing views."""
    if prefer_updated_for_arxiv and paper.source.casefold() == SOURCE_ARXIV:
        return paper.updated
    if (
        paper.source.casefold() == SOURCE_SSRN
        and paper.updated > paper.published
        and paper.published.month == 1
        and paper.published.day == 1
    ):
        # OpenAlex often exposes SSRN publication_date as YYYY-01-01 placeholders.
        # In that case, updated_date better represents new/ingested items.
        return paper.updated
    return paper.published


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
    unsupported_refs: List[str] = []
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
        elif source == SOURCE_ARXIV:
            arxiv_ids.append(paper_id)
        else:
            unsupported_refs.append(canonical_ref)

    arxiv_papers, arxiv_missing = await fetch_papers_by_arxiv_ids(arxiv_ids)
    pubmed_papers, pubmed_missing = await fetch_papers_by_pubmed_ids(pubmed_ids)

    resolved: dict[str, Paper] = {}
    for paper in arxiv_papers:
        resolved[make_paper_ref(SOURCE_ARXIV, paper.arxiv_id)] = paper
    for paper in pubmed_papers:
        resolved[make_paper_ref(SOURCE_PUBMED, paper.arxiv_id)] = paper

    ordered: List[Paper] = []
    missing: List[str] = list(unsupported_refs)
    for idx, (canonical_ref, _source, _paper_id) in enumerate(parsed_refs, start=1):
        paper = resolved.get(canonical_ref)
        if paper is None:
            if canonical_ref not in missing:
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


def format_paper_line(
    paper: Paper,
    *,
    prefer_updated_for_arxiv: bool = False,
) -> str:
    """Format one paper entry for the `/today` list message.

    Parameters
    ----------
    paper : Paper
        Paper metadata and abstract text to show in Telegram.
    prefer_updated_for_arxiv : bool, default=False
        If True, show the arXiv entry update timestamp instead of the initial
        publication timestamp. Non-arXiv sources still display `published`.

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

    timestamp = _paper_recency_timestamp(
        paper,
        prefer_updated_for_arxiv=prefer_updated_for_arxiv,
    )
    timestamp_label = timestamp.strftime("%Y-%m-%d %H:%M UTC")
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
    meta_parts.append(html.escape(timestamp_label))

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
    pdf_link = resolve_paper_pdf_link(paper)
    if paper.link_abs:
        buttons.append(InlineKeyboardButton(text=paper.link_abs, url=paper.link_abs))
    if pdf_link and paper.arxiv_id:
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


async def _send_fetch_status_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    hours_back: int,
    scope: str,
) -> Optional[tuple[int, int]]:
    chat = update.effective_chat
    if chat is None:
        return None

    status_text = (
        f"Retrieving data from journals for {_describe_search_window(scope, hours_back)}.\n"
        "This can take a few seconds."
    )

    try:
        if update.message is not None:
            sent_message = await update.message.reply_text(status_text)
        else:
            sent_message = await context.bot.send_message(
                chat_id=chat.id,
                text=status_text,
            )
    except Exception:
        logger.exception("Could not send fetch status message")
        return None

    return int(chat.id), int(sent_message.message_id)


async def _delete_temporary_bot_message(
    context: ContextTypes.DEFAULT_TYPE,
    message_ref: Optional[tuple[int, int]],
) -> None:
    if message_ref is None:
        return

    chat_id, message_id = message_ref
    try:
        # Remove short-lived progress notices so the chat stays focused on the
        # actual search results and not on transport status.
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        logger.debug("Could not delete temporary bot message %s in chat %s", message_id, chat_id)


def _empty_raw_breakdown() -> dict[str, int]:
    return {
        SOURCE_ARXIV: 0,
        SOURCE_BIORXIV: 0,
        SOURCE_MEDRXIV: 0,
        SOURCE_CHEMRXIV: 0,
        SOURCE_SSRN: 0,
        SOURCE_IEEE: 0,
        SOURCE_PUBMED: 0,
    }


def _build_query_lines_for_sources(keywords_by_source: dict[str, Sequence[str]]) -> List[str]:
    arxiv_keywords = list(keywords_by_source.get(SOURCE_ARXIV, []))
    biorxiv_keywords = list(keywords_by_source.get(SOURCE_BIORXIV, []))
    medrxiv_keywords = list(keywords_by_source.get(SOURCE_MEDRXIV, []))
    chemrxiv_keywords = list(keywords_by_source.get(SOURCE_CHEMRXIV, []))
    ssrn_keywords = list(keywords_by_source.get(SOURCE_SSRN, []))
    ieee_keywords = list(keywords_by_source.get(SOURCE_IEEE, []))
    pubmed_keywords = list(keywords_by_source.get(SOURCE_PUBMED, []))

    arxiv_query = build_arxiv_query(keywords=arxiv_keywords)
    pubmed_query = build_pubmed_query(keywords=pubmed_keywords)

    query_lines: List[str] = []
    if arxiv_query:
        query_lines.append(f"arXiv: {arxiv_query}")
    if biorxiv_keywords:
        query_lines.append(f"bioRxiv keywords: {', '.join(biorxiv_keywords)}")
    if medrxiv_keywords:
        query_lines.append(f"medRxiv keywords: {', '.join(medrxiv_keywords)}")
    if chemrxiv_keywords:
        query_lines.append(f"ChemRxiv keywords: {', '.join(chemrxiv_keywords)}")
    if ssrn_keywords:
        query_lines.append(f"SSRN keywords: {', '.join(ssrn_keywords)}")
    if ieee_keywords:
        query_lines.append(f"IEEE keywords: {', '.join(ieee_keywords)}")
    if pubmed_query:
        query_lines.append(f"PubMed: {pubmed_query}")
    return query_lines


def _store_search_cache(
    user_data: dict[str, Any],
    *,
    papers: Sequence[Paper],
    query_text: str,
    request_text: str,
    raw_total: int,
    raw_breakdown: dict[str, int],
    hours_back: int,
    scope: str,
) -> int:
    user_data["papers"] = list(papers)
    user_data["last_query"] = query_text
    user_data["last_request_url"] = request_text
    user_data["last_refresh_utc"] = datetime.now(timezone.utc)
    user_data["last_raw_entry_count"] = raw_total
    user_data["last_raw_entry_breakdown"] = dict(raw_breakdown)
    user_data["cache_hours_back"] = int(hours_back)
    user_data["cache_scope"] = _normalize_search_scope(scope)
    user_data["results_token"] = int(user_data.get("results_token", 0) or 0) + 1
    return int(user_data["results_token"])


async def _fetch_papers_for_keywords_by_source(
    *,
    keywords_by_source: dict[str, Sequence[str]],
    hours_back: int,
    scope: str,
) -> tuple[list[Paper], str, str, int, dict[str, int]]:
    effective_scope = _normalize_search_scope(scope)
    effective_hours_back = int(hours_back)
    source_queries = {
        source: list(keywords_by_source.get(source, []))
        for source in keyword_sources()
    }
    arxiv_keywords = source_queries[SOURCE_ARXIV]
    pubmed_keywords = source_queries[SOURCE_PUBMED]
    arxiv_query = build_arxiv_query(keywords=arxiv_keywords)
    pubmed_query = build_pubmed_query(keywords=pubmed_keywords)
    has_any_query = bool(arxiv_query or pubmed_query or any(source_queries[source] for source in keyword_sources() if source != SOURCE_ARXIV and source != SOURCE_PUBMED))
    if not has_any_query:
        return [], "", "", 0, _empty_raw_breakdown()

    request_urls_by_source: dict[str, str] = {}
    raw_breakdown = _empty_raw_breakdown()
    papers_by_source: dict[str, list[Paper]] = {source: [] for source in keyword_sources()}
    fetch_hours_back = None if effective_scope == SEARCH_SCOPE_GLOBAL else effective_hours_back

    preprint_fetches: List[tuple[str, Any]] = []
    if arxiv_query:
        preprint_fetches.append(
            (
                SOURCE_ARXIV,
                asyncio.to_thread(
                    fetch_arxiv_entries,
                    arxiv_query,
                    DEFAULT_MAX_RESULTS,
                    "lastUpdatedDate" if effective_scope == SEARCH_SCOPE_TODAY else "submittedDate",
                ),
            )
        )
    if source_queries[SOURCE_BIORXIV]:
        preprint_fetches.append(
            (
                SOURCE_BIORXIV,
                asyncio.to_thread(
                    fetch_rxiv_papers,
                    SOURCE_BIORXIV,
                    source_queries[SOURCE_BIORXIV],
                    fetch_hours_back,
                    DEFAULT_MAX_RESULTS,
                ),
            )
        )
    if source_queries[SOURCE_MEDRXIV]:
        preprint_fetches.append(
            (
                SOURCE_MEDRXIV,
                asyncio.to_thread(
                    fetch_rxiv_papers,
                    SOURCE_MEDRXIV,
                    source_queries[SOURCE_MEDRXIV],
                    fetch_hours_back,
                    DEFAULT_MAX_RESULTS,
                ),
            )
        )
    if source_queries[SOURCE_CHEMRXIV]:
        preprint_fetches.append(
            (
                SOURCE_CHEMRXIV,
                asyncio.to_thread(
                    fetch_chemrxiv_papers,
                    source_queries[SOURCE_CHEMRXIV],
                    fetch_hours_back,
                    DEFAULT_MAX_RESULTS,
                ),
            )
        )
    if source_queries[SOURCE_SSRN]:
        preprint_fetches.append(
            (
                SOURCE_SSRN,
                asyncio.to_thread(
                    fetch_ssrn_papers,
                    source_queries[SOURCE_SSRN],
                    fetch_hours_back,
                    DEFAULT_MAX_RESULTS,
                ),
            )
        )
    if source_queries[SOURCE_IEEE]:
        preprint_fetches.append(
            (
                SOURCE_IEEE,
                asyncio.to_thread(
                    fetch_ieee_papers,
                    source_queries[SOURCE_IEEE],
                    fetch_hours_back,
                    DEFAULT_MAX_RESULTS,
                ),
            )
        )

    if preprint_fetches:
        preprint_results = await asyncio.gather(
            *(task for _source, task in preprint_fetches),
            return_exceptions=True,
        )
        for (source, _task), result in zip(preprint_fetches, preprint_results):
            if isinstance(result, Exception):
                logger.warning("%s fetch failed: %s", paper_source_label(source), result)
                continue
            if source == SOURCE_ARXIV:
                entries, request_url = result
                request_urls_by_source[source] = request_url
                raw_breakdown[source] = len(entries)
                if effective_scope == SEARCH_SCOPE_TODAY:
                    papers_by_source[source] = entries_to_recent_papers(
                        entries,
                        TODAY_HOURS_BACK,
                        use_updated=True,
                    )
                elif effective_scope == SEARCH_SCOPE_GLOBAL:
                    papers_by_source[source] = entries_to_papers(entries)
                else:
                    papers_by_source[source] = entries_to_recent_papers(entries, effective_hours_back)
                continue

            source_papers, request_url, raw_count = result
            papers_by_source[source] = source_papers
            request_urls_by_source[source] = request_url
            raw_breakdown[source] = int(raw_count)

    if pubmed_query:
        try:
            pubmed_papers, pubmed_request_url, pubmed_raw_count = await asyncio.to_thread(
                fetch_pubmed_papers,
                pubmed_query,
                fetch_hours_back,
                DEFAULT_MAX_RESULTS,
            )
            papers_by_source[SOURCE_PUBMED] = pubmed_papers
            request_urls_by_source[SOURCE_PUBMED] = pubmed_request_url
            raw_breakdown[SOURCE_PUBMED] = int(pubmed_raw_count)
        except Exception as exc:
            logger.warning("%s fetch failed: %s", paper_source_label(SOURCE_PUBMED), exc)

    papers = [
        paper
        for source in keyword_sources()
        for paper in papers_by_source.get(source, [])
    ]
    papers.sort(
        key=lambda paper: _paper_recency_timestamp(
            paper,
            prefer_updated_for_arxiv=effective_scope == SEARCH_SCOPE_TODAY,
        ),
        reverse=True,
    )
    for idx, paper in enumerate(papers, start=1):
        paper.index = idx

    query_lines = _build_query_lines_for_sources(source_queries)
    request_lines = [
        f"{paper_source_label(source)}: {request_urls_by_source[source]}"
        for source in keyword_sources()
        if request_urls_by_source.get(source)
    ]
    raw_total = sum(int(value) for value in raw_breakdown.values())
    return papers, "\n".join(query_lines), "\n".join(request_lines), raw_total, raw_breakdown


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
    effective_hours_back = 0 if effective_scope == SEARCH_SCOPE_GLOBAL else int(hours_back) if hours_back is not None else cached_hours_back
    keywords_by_source = get_keywords_by_source(user_data=user_data, user_id=user_id)
    query_lines = _build_query_lines_for_sources(keywords_by_source)
    if not query_lines:
        _store_search_cache(
            user_data,
            papers=[],
            query_text="",
            request_text="",
            raw_total=0,
            raw_breakdown=_empty_raw_breakdown(),
            hours_back=effective_hours_back,
            scope=effective_scope,
        )
        logger.info("No active query; waiting for keywords")
        return []

    logger.info(
        (
            "Refreshing paper cache for user %s "
            "(scope=%s, arXiv=%s, bioRxiv=%s, medRxiv=%s, ChemRxiv=%s, SSRN=%s, IEEE=%s, PubMed=%s)"
        ),
        user_id,
        effective_scope,
        bool(keywords_by_source.get(SOURCE_ARXIV)),
        bool(keywords_by_source.get(SOURCE_BIORXIV)),
        bool(keywords_by_source.get(SOURCE_MEDRXIV)),
        bool(keywords_by_source.get(SOURCE_CHEMRXIV)),
        bool(keywords_by_source.get(SOURCE_SSRN)),
        bool(keywords_by_source.get(SOURCE_IEEE)),
        bool(keywords_by_source.get(SOURCE_PUBMED)),
    )

    fetch_status_message = await _send_fetch_status_message(
        update,
        context,
        hours_back=effective_hours_back,
        scope=effective_scope,
    )
    try:
        papers, query_text, request_text, raw_total, raw_breakdown = await _fetch_papers_for_keywords_by_source(
            keywords_by_source=keywords_by_source,
            hours_back=effective_hours_back,
            scope=effective_scope,
        )
        _store_search_cache(
            user_data,
            papers=papers,
            query_text=query_text,
            request_text=request_text,
            raw_total=raw_total,
            raw_breakdown=raw_breakdown,
            hours_back=effective_hours_back,
            scope=effective_scope,
        )
        return papers
    finally:
        await _delete_temporary_bot_message(context, fetch_status_message)


async def fetch_recent_papers_for_user(
    user_id: int,
    hours_back: int,
) -> tuple[list[Paper], str, int, dict[str, int]]:
    keywords_by_source = {
        source: get_keywords_for_source(source, user_id=user_id)
        for source in keyword_sources()
    }
    papers, query_text, _request_text, raw_total, raw_breakdown = await _fetch_papers_for_keywords_by_source(
        keywords_by_source=keywords_by_source,
        hours_back=hours_back,
        scope=SEARCH_SCOPE_TODAY,
    )
    return papers, query_text, raw_total, raw_breakdown


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
            text="Daily recap failed. Could not fetch papers from one or more sources.",
            reply_markup=build_main_menu_markup(),
        )
        return

    if not query:
        await application.bot.send_message(
            chat_id=chat_id,
            text=(
                "Daily recap skipped.\n"
                "Add at least one keyword first."
            ),
            reply_markup=build_main_menu_markup(),
        )
        return

    now_label = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if not papers:
        await application.bot.send_message(
            chat_id=chat_id,
            text=(
                f"Daily recap • {now_label}\n"
                f"No matching papers in the last {DAILY_RECAP_HOURS} hours."
            ),
            reply_markup=build_main_menu_markup(),
        )
        return

    user_data = application.user_data[user_id]
    user_data["papers"] = papers
    user_data["last_query"] = query
    user_data["last_raw_entry_count"] = raw_count
    user_data["last_raw_entry_breakdown"] = raw_breakdown
    user_data["cache_hours_back"] = DAILY_RECAP_HOURS
    user_data["cache_scope"] = SEARCH_SCOPE_TODAY
    user_data["results_token"] = int(user_data.get("results_token", 0) or 0) + 1
    results_token = int(user_data["results_token"])

    await application.bot.send_message(
        chat_id=chat_id,
        text=(
            f"Daily recap • {now_label}\n"
            f"{len(papers)} matching paper(s) in the last {DAILY_RECAP_HOURS} hours."
        ),
        reply_markup=build_main_menu_markup(),
    )

    async def send_recap_text(
        text: str,
        *,
        reply_markup: Optional[Any] = None,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: Optional[bool] = None,
    ) -> None:
        await application.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )

    await send_results_page(
        send_recap_text,
        context=SimpleNamespace(user_data=user_data),
        papers=papers,
        user_id=user_id,
        hours_back=DAILY_RECAP_HOURS,
        scope=SEARCH_SCOPE_TODAY,
        results_token=results_token,
        start_index=0,
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
    expected_scope = _normalize_search_scope(scope if scope is not None else SEARCH_SCOPE_TODAY)
    expected_hours = (
        0
        if expected_scope == SEARCH_SCOPE_GLOBAL
        else int(hours_back) if hours_back is not None else TODAY_HOURS_BACK
    )
    if context.user_data.get("cache_hours_back") != expected_hours:
        return []
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
            "<b>Welcome to Scholar Stream</b>\n\n"
            "Track new papers from arXiv, bioRxiv, medRxiv, ChemRxiv, SSRN, IEEE, and PubMed using your own keywords.\n\n"
            "<b>Initial Setup (do this first)</b>\n"
            f"1. Open <b>{html.escape(MENU_BTN_ADD_KEYWORDS)}</b>, then choose a source (or All sources)\n"
            f"2. Check results with <b>{html.escape(MENU_BTN_TODAY)}</b> "
            f"(last {TODAY_HOURS_BACK}h) or <b>{html.escape(MENU_BTN_SEARCH_HOURS)}</b>\n\n"
            "<b>Set Up Recap</b>\n"
            f"3. Turn recap on with <b>{html.escape(MENU_BTN_DAILY_RECAP)}</b>\n"
            f"4. Set one or more recap times with <b>{html.escape(MENU_BTN_SET_RECAP_TIME)}</b>\n"
            f"5. Confirm recap status with <b>{html.escape(MENU_BTN_RECAP_STATUS)}</b>\n\n"
            "<b>Keyword Syntax</b>\n"
            "Use commas for separate alternatives (OR).\n"
            "Use <code>+</code> to search terms together (AND) in one keyword entry.\n"
            'Example: <code>quantum mechanics + entanglement, superconductivity</code>.'
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


async def _track_user_metrics_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    user = update.effective_user
    if user is None:
        return

    chat = update.effective_chat
    try:
        _record_user_interaction(
            int(user.id),
            username=user.username,
            full_name=user.full_name,
            chat_id=int(chat.id) if chat is not None else None,
        )
    except Exception:
        logger.exception("Could not record metrics for user %s", user.id)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            build_help_text(),
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )


def build_help_text() -> str:
    return (
        "<b>Initial Setup</b>\n"
        f"1. Add your first keywords with <b>{html.escape(MENU_BTN_ADD_KEYWORDS)}</b> "
        "(choose source or All sources)\n"
        f"2. Run your first search with <b>{html.escape(MENU_BTN_TODAY)}</b> or "
        f"<b>{html.escape(MENU_BTN_SEARCH_HOURS)}</b>\n"
        f"3. Configure automatic updates with <b>{html.escape(MENU_BTN_DAILY_RECAP)}</b> and "
        f"<b>{html.escape(MENU_BTN_SET_RECAP_TIME)}</b>\n\n"
        "<b>Search & Results</b>\n"
        f"• <b>{html.escape(MENU_BTN_TODAY)}</b>: show the last {TODAY_HOURS_BACK} hours on arXiv, bioRxiv, medRxiv, ChemRxiv, SSRN, IEEE, and PubMed\n"
        f"• <b>{html.escape(MENU_BTN_SEARCH_HOURS)}</b>: choose a custom time range\n"
        f"• <b>{html.escape(MENU_BTN_KEYWORDS)}</b>: show your current keyword lists\n"
        f"• <b>{html.escape(MENU_BTN_BOOKMARKS)}</b>: show saved papers\n\n"
        "<b>Keyword Management</b>\n"
        f"• <b>{html.escape(MENU_BTN_ADD_KEYWORDS)}</b>: add keywords for one source or all sources\n"
        f"• <b>{html.escape(MENU_BTN_REMOVE_KEYWORDS)}</b>: remove keywords for one source or all sources\n"
        f"• <b>{html.escape(MENU_BTN_CLEAR_KEYWORDS)}</b>: clear keywords for one source or all sources\n\n"
        "<b>Recap</b>\n"
        f"• <b>{html.escape(MENU_BTN_DAILY_RECAP)}</b>: turn the recap on or off\n"
        f"• <b>{html.escape(MENU_BTN_SET_RECAP_TIME)}</b>: choose time zone and set one or more local recap times (HH:MM)\n"
        f"• <b>{html.escape(MENU_BTN_RECAP_STATUS)}</b>: show recap status and times\n\n"
        "<b>Keyword Syntax</b>\n"
        "• Commas or separate lines = separate alternatives (OR)\n"
        "• <code>+</code> = terms searched together (AND) in one entry\n"
        '• Example: <code>"quantum mechanics" + entanglement, superconductivity</code>\n'
        '• List example: <code>- quantum mechanics\n- entanglement + superconductivity</code>\n\n'
        "<b>Other</b>\n"
        f"• <b>{html.escape(MENU_BTN_HELP)}</b>: show this guide\n"
        f"• <b>{html.escape(MENU_BTN_MORE)}</b>: open {html.escape(MENU_BTN_GLOBAL_SEARCH)}, feedback, and support options"
    )


async def prompt_more_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if update.message is not None:
        await update.message.reply_text(
            MORE_MENU_MESSAGE_TEXT,
            reply_markup=build_more_menu_markup(),
        )
        return
    if chat is not None:
        await context.bot.send_message(
            chat_id=chat.id,
            text=MORE_MENU_MESSAGE_TEXT,
            reply_markup=build_more_menu_markup(),
        )


def _normalize_global_search_sources(raw_sources: Sequence[Any]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    for item in raw_sources:
        source = normalize_text(str(item or "")).casefold()
        if source not in ALL_PAPER_SOURCES or source in seen:
            continue
        seen.add(source)
        normalized.append(source)
    return normalized


def _get_global_search_sources(context: ContextTypes.DEFAULT_TYPE) -> List[str]:
    cached = context.user_data.get("global_search_selected_sources", [])
    if not isinstance(cached, Sequence) or isinstance(cached, (str, bytes, bytearray)):
        return []
    return _normalize_global_search_sources(cached)


def _global_search_sources_label(sources: Sequence[str]) -> str:
    labels = [paper_source_label(source) for source in _normalize_global_search_sources(sources)]
    if not labels:
        return "no journals"
    return ", ".join(labels)


def _clear_global_search_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("awaiting_global_search_query", None)
    context.user_data.pop("global_search_selected_sources", None)


async def _show_global_search_source_picker(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    selected_sources: Optional[Sequence[str]] = None,
) -> None:
    selected = _normalize_global_search_sources(
        selected_sources if selected_sources is not None else _get_global_search_sources(context)
    )
    context.user_data["global_search_selected_sources"] = selected
    text = (
        "<b>Global Search</b>\n\n"
        "Choose one or more journals, then tap <b>Search</b>."
    )
    markup = build_global_search_sources_markup(selected)
    query = update.callback_query
    chat = update.effective_chat
    if query is not None and query.message is not None:
        await query.edit_message_text(
            text=text,
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
        )
        return
    if update.message is not None:
        await update.message.reply_text(
            text,
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
        )
        return
    if chat is not None:
        await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
        )


async def _prompt_global_search_query(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    selected_sources: Sequence[str],
) -> None:
    selected = _normalize_global_search_sources(selected_sources)
    if not selected:
        return
    _clear_pending_input_flags(context)
    context.user_data["global_search_selected_sources"] = selected
    context.user_data["awaiting_global_search_query"] = True

    query = update.callback_query
    if query is not None and query.message is not None:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            logger.exception("Could not clear Global Search source picker markup")

    chat = update.effective_chat
    text = (
        "<b>Global Search</b>\n\n"
        f"<b>Selected journals</b>: {html.escape(_global_search_sources_label(selected))}\n\n"
        "Send your query now.\n"
        "Use commas or separate lines for OR.\n"
        "Use <code>+</code> inside one entry for AND.\n\n"
        "<b>Examples</b>\n"
        "<code>graph neural networks</code>\n"
        "<code>quantum mechanics + entanglement, superconductivity</code>\n"
        "<code>- llm safety\n- retrieval + benchmark</code>"
    )
    if chat is not None:
        await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )


async def apply_global_search_query_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    raw: str,
) -> bool:
    message = update.message
    user_id = _get_user_id(update)
    if message is None or user_id is None:
        return False

    selected_sources = _get_global_search_sources(context)
    if not selected_sources:
        await message.reply_text(
            f"Choose journals first from {MENU_BTN_MORE} > {MENU_BTN_GLOBAL_SEARCH}.",
            reply_markup=build_main_menu_markup(),
        )
        _clear_global_search_state(context)
        return False

    parsed_keywords = parse_keywords_input(raw)
    if not parsed_keywords:
        await message.reply_text(
            "No valid query terms found. Send at least one keyword or phrase.",
            reply_markup=build_main_menu_markup(),
        )
        return False

    keywords_by_source = {
        source: list(parsed_keywords)
        for source in selected_sources
    }
    fetch_status_message = await _send_fetch_status_message(
        update,
        context,
        hours_back=0,
        scope=SEARCH_SCOPE_GLOBAL,
    )
    try:
        papers, query_text, request_text, raw_total, raw_breakdown = await _fetch_papers_for_keywords_by_source(
            keywords_by_source=keywords_by_source,
            hours_back=0,
            scope=SEARCH_SCOPE_GLOBAL,
        )
    except Exception as exc:
        logger.exception("Global Search failed")
        await message.reply_text(
            f"Could not run Global Search:\n{exc}",
            reply_markup=build_main_menu_markup(),
        )
        return False
    finally:
        await _delete_temporary_bot_message(context, fetch_status_message)

    results_token = _store_search_cache(
        context.user_data,
        papers=papers,
        query_text=query_text,
        request_text=request_text,
        raw_total=raw_total,
        raw_breakdown=raw_breakdown,
        hours_back=0,
        scope=SEARCH_SCOPE_GLOBAL,
    )
    _clear_global_search_state(context)

    if not papers:
        await message.reply_text(
            (
                f"No matching papers found in {_describe_search_window(SEARCH_SCOPE_GLOBAL, 0)}.\n\n"
                f"Searched journals: {_global_search_sources_label(selected_sources)}.\n"
                f"Review your query and try {MENU_BTN_GLOBAL_SEARCH} again."
            ),
            reply_markup=build_main_menu_markup(),
        )
        return True

    await message.reply_text(
        (
            f"{len(papers)} matching paper(s) found in "
            f"{_describe_search_window(SEARCH_SCOPE_GLOBAL, 0)}.\n"
            f"Searched journals: {_global_search_sources_label(selected_sources)}."
        ),
        reply_markup=build_main_menu_markup(),
    )

    async def send_text(
        text: str,
        *,
        reply_markup: Optional[Any] = None,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: Optional[bool] = None,
    ) -> None:
        await message.reply_text(
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )

    await send_results_page(
        send_text,
        context=context,
        papers=papers,
        user_id=user_id,
        hours_back=0,
        scope=SEARCH_SCOPE_GLOBAL,
        results_token=results_token,
        start_index=0,
    )
    return True


async def _send_coffee_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if chat is None:
        return

    markup = build_coffee_markup()
    if not COFFEE_EVM_ADDRESS and not COFFEE_SOLANA_ADDRESS and not COFFEE_BTC_ADDRESS:
        if markup is None:
            await context.bot.send_message(
                chat_id=chat.id,
                text="No support destination is configured yet.",
                reply_markup=build_main_menu_markup(),
            )
            return
        await context.bot.send_message(chat_id=chat.id, text=COFFEE_TEXT, reply_markup=markup, disable_web_page_preview=True)
        return

    address_lines: List[str] = []
    if COFFEE_EVM_ADDRESS:
        address_lines.append("<b>EVM (Ethereum/Base/Arbitrum/OP/Polygon/BNB/Hyperliquid)</b>")
        address_lines.append(f"<code>{html.escape(COFFEE_EVM_ADDRESS)}</code>")
    if COFFEE_SOLANA_ADDRESS:
        if address_lines:
            address_lines.append("")
        address_lines.append("<b>Solana</b>")
        address_lines.append(f"<code>{html.escape(COFFEE_SOLANA_ADDRESS)}</code>")
    if COFFEE_BTC_ADDRESS:
        if address_lines:
            address_lines.append("")
        address_lines.append("<b>Bitcoin</b>")
        address_lines.append(f"<code>{html.escape(COFFEE_BTC_ADDRESS)}</code>")

    message = (
        "<b>Support this bot</b>\n\n"
        "You can send support using one of these networks:\n\n"
        + "\n".join(address_lines)
        + "\n\n"
        "Please double-check the address before sending."
    )
    await context.bot.send_message(
        chat_id=chat.id,
        text=message,
        reply_markup=markup,
        disable_web_page_preview=True,
        parse_mode=ParseMode.HTML,
    )


async def coffee_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_coffee_message(update, context)


async def _send_report_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if chat is None:
        return

    if not get_report_forward_chat_ids():
        await context.bot.send_message(
            chat_id=chat.id,
            text="Report inbox is not configured yet on this bot.",
            reply_markup=build_main_menu_markup(),
        )
        return

    user_id = _get_user_id(update)
    if user_id is not None and _remaining_feedback_slots(user_id, context.user_data) <= 0:
        await context.bot.send_message(
            chat_id=chat.id,
            text=_feedback_limit_reached_text(),
            reply_markup=build_main_menu_markup(),
        )
        return

    _clear_pending_input_flags(context)
    context.user_data["awaiting_report_input"] = True
    await context.bot.send_message(
        chat_id=chat.id,
        text=(
            "<b>Report an issue or Request a new feature</b>\n\n"
            "Write your message and send it here.\n"
            "Your message will be forwarded to the maintainer.\n\n"
            "To cancel, press any other menu button."
        ),
        reply_markup=build_main_menu_markup(),
        parse_mode=ParseMode.HTML,
    )


async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_report_prompt(update, context)


async def more_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return

    data = query.data or ""
    if not data.startswith("moremenu:"):
        await query.answer()
        return

    action = data.removeprefix("moremenu:").strip().casefold()
    if action == "globalsearch":
        await query.answer()
        await _show_global_search_source_picker(update, context, selected_sources=[])
        return

    await query.answer()
    if query.message is not None:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            logger.exception("Could not clear More menu markup")

    if action == "report":
        await _send_report_prompt(update, context)
        return
    if action == "coffee":
        await _send_coffee_message(update, context)
        return


async def global_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return

    data = query.data or ""
    if not data.startswith("gsearch:"):
        await query.answer()
        return

    selected_sources = _get_global_search_sources(context)
    if data.startswith("gsearch:toggle:"):
        source = normalize_text(data.removeprefix("gsearch:toggle:")).casefold()
        if source not in ALL_PAPER_SOURCES:
            await query.answer("Invalid journal.", show_alert=True)
            return
        if source in selected_sources:
            selected_sources = [item for item in selected_sources if item != source]
            await query.answer(f"Removed {paper_source_label(source)}")
        else:
            selected_sources.append(source)
            await query.answer(f"Added {paper_source_label(source)}")
        await _show_global_search_source_picker(update, context, selected_sources=selected_sources)
        return

    if data == "gsearch:all":
        await query.answer("All journals selected")
        await _show_global_search_source_picker(update, context, selected_sources=keyword_sources())
        return

    if data == "gsearch:clear":
        await query.answer("Selection cleared")
        await _show_global_search_source_picker(update, context, selected_sources=[])
        return

    if data == "gsearch:start":
        if not selected_sources:
            await query.answer("Choose at least one journal first.", show_alert=True)
            return
        await query.answer("Send your query")
        await _prompt_global_search_query(
            update,
            context,
            selected_sources=selected_sources,
        )
        return

    if data == "gsearch:cancel":
        _clear_global_search_state(context)
        await query.answer("Global Search cancelled")
        if query.message is not None:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                logger.exception("Could not clear Global Search markup on cancel")
        chat = update.effective_chat
        if chat is not None:
            await context.bot.send_message(
                chat_id=chat.id,
                text="Global Search cancelled.",
                reply_markup=build_main_menu_markup(),
            )
        return

    await query.answer("Unknown Global Search action.", show_alert=True)


async def apply_report_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    raw: str,
) -> bool:
    message = update.message
    user = update.effective_user
    chat = update.effective_chat
    if message is None or user is None or chat is None:
        return False

    report_text = (raw or "").strip()
    if not report_text:
        await message.reply_text(
            "Empty message. Please write a short description.",
            reply_markup=build_main_menu_markup(),
        )
        return False

    user_id = int(user.id)
    feedback_lock = _get_feedback_submission_lock(context.application, user_id)

    # Serialize report submissions per user so the persisted daily cap remains
    # accurate even if the user sends multiple feedback messages quickly.
    async with feedback_lock:
        if _remaining_feedback_slots(user_id, context.user_data) <= 0:
            await message.reply_text(
                _feedback_limit_reached_text(),
                reply_markup=build_main_menu_markup(),
            )
            return False

        target_chat_ids = get_report_forward_chat_ids()
        if not target_chat_ids:
            await message.reply_text(
                "Report inbox is not configured yet on this bot.",
                reply_markup=build_main_menu_markup(),
            )
            return False

        username = f"@{user.username}" if user.username else "(no username)"
        forwarded_text = (
            "<b>New report/request</b>\n"
            f"<b>From</b>: {html.escape(user.full_name)} ({html.escape(username)})\n"
            f"<b>User ID</b>: <code>{user.id}</code>\n"
            f"<b>Chat ID</b>: <code>{chat.id}</code>\n\n"
            "<b>Message</b>\n"
            f"{html.escape(report_text)}"
        )

        sent_count = 0
        for target_chat_id in target_chat_ids:
            try:
                await context.bot.send_message(
                    chat_id=target_chat_id,
                    text=forwarded_text,
                    parse_mode=ParseMode.HTML,
                )
                sent_count += 1
            except Exception:
                logger.exception("Could not forward report message to %s", target_chat_id)

        if sent_count == 0:
            await message.reply_text(
                "Could not forward your message right now. Please try again later.",
                reply_markup=build_main_menu_markup(),
            )
            return False

        _record_feedback_submission(user_id, context.user_data)

    await message.reply_text(
        "Message sent. Thank you for the report.",
        reply_markup=build_main_menu_markup(),
    )
    return True


async def setreporttarget_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if update.message is None or chat is None or user is None:
        return

    if not _is_admin_user(int(user.id)):
        await update.message.reply_text(
            "This command is not available to users.\n"
            "Configure REPORT_FORWARD_CHAT_ID on the server.",
            reply_markup=build_main_menu_markup(),
        )
        return

    await update.message.reply_text(
        "Report target is configured from environment only.\n"
        "Set REPORT_FORWARD_CHAT_ID on the server.",
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_menu_markup(),
    )


async def _userstats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    user_id = _get_user_id(update)
    if message is None:
        return

    if not _is_admin_user(user_id):
        await message.reply_text(
            "This command is not available to users.\n"
            "Configure REPORT_ADMIN_USER_ID on the server.",
            reply_markup=build_main_menu_markup(),
        )
        return

    try:
        metrics = _get_user_metrics_summary()
    except Exception as exc:
        logger.exception("Could not load user metrics")
        await message.reply_text(
            f"Could not load user metrics:\n{exc}",
            reply_markup=build_main_menu_markup(),
        )
        return

    await message.reply_text(
        "<b>User Metrics</b>\n\n"
        f"Total users ever seen: <b>{metrics['total_users']}</b>\n"
        f"Daily active users (UTC): <b>{metrics['daily_active_users']}</b>\n"
        f"Weekly active users (last 7 UTC days): <b>{metrics['weekly_active_users']}</b>\n"
        f"Monthly active users (last 30 UTC days): <b>{metrics['monthly_active_users']}</b>\n"
        f"Users with recap enabled: <b>{metrics['recap_enabled_users']}</b>\n\n"
        f"As of: <code>{html.escape(str(metrics['as_of_utc']))}</code>",
        reply_markup=build_main_menu_markup(),
        parse_mode=ParseMode.HTML,
    )


async def keywords_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    keywords_by_source = get_keywords_by_source(context.user_data, user_id=user_id)
    sections: List[str] = []
    for source in keyword_sources():
        source_keywords = keywords_by_source.get(source, [])
        source_body = (
            "\n".join(f"- {html.escape(item)}" for item in source_keywords)
            if source_keywords
            else "(none)"
        )
        sections.append(
            f"<b>{html.escape(paper_source_label(source))}</b>\n"
            f"<pre>{source_body}</pre>"
        )

    if update.message:
        await update.message.reply_text(
            "<b>Active Keywords</b>\n\n"
            f"<b>{html.escape(MENU_BTN_TODAY)} Window</b>\n"
            f"All sources: last {TODAY_HOURS_BACK} hours\n\n"
            + "\n\n".join(sections)
            + "\n\n"
            f"Use <b>{html.escape(MENU_BTN_SEARCH_HOURS)}</b> for a different time range.\n"
            "Use commas or one entry per line for separate alternatives (OR). Use <code>+</code> to search terms together (AND) in one entry.\n"
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
    context.user_data.pop("awaiting_report_input", None)
    _clear_global_search_state(context)
    context.user_data.pop("pending_recap_timezone", None)


async def prompt_setkeywords_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _clear_pending_input_flags(context)
    context.user_data["awaiting_keywords_input"] = True
    if update.message:
        await update.message.reply_text(
            "<b>Set Keywords</b>\n\n"
            "Send a comma-separated list or one keyword per line to apply to all sources.\n"
            "Use <code>+</code> inside one entry to search terms together (AND), not separately.\n\n"
            "<b>Example</b>\n"
            "<code>astronomy, climate change + photosynthesis</code>\n\n"
            "<code>- astronomy\n- climate change + photosynthesis</code>\n\n"
            f"To edit one source only, use <b>{html.escape(MENU_BTN_ADD_KEYWORDS)}</b> / "
            f"<b>{html.escape(MENU_BTN_REMOVE_KEYWORDS)}</b> / "
            f"<b>{html.escape(MENU_BTN_CLEAR_KEYWORDS)}</b>.",
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
    source_label = keyword_source_label(source)
    text = (
        f"<b>Add Keywords • {html.escape(source_label)}</b>\n\n"
        "Send one or more keywords.\n"
        "Separate entries with commas or send one per line.\n\n"
        "Use <code>+</code> inside one entry to search terms together (AND), not separately.\n\n"
        "<b>Examples</b>\n"
        "<code>quantum mechanics</code>\n"
        "<code>astronomy, climate change + photosynthesis</code>\n"
        "<code>- astronomy\n- climate change + photosynthesis</code>\n"
        "<code>\"quantum mechanics\" + entanglement</code>"
    )
    if update.message:
        await update.message.reply_text(
            text,
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )
        return
    chat = update.effective_chat
    if chat is not None:
        await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )


async def prompt_keyword_scope_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> None:
    _clear_pending_input_flags(context)
    action_label = _keyword_action_label(action)
    text = (
        f"<b>{html.escape(action_label)}</b>\n\n"
        "Choose where this action should apply."
    )
    if update.message is not None:
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_keyword_scope_markup(action),
        )
        return
    chat = update.effective_chat
    if chat is not None:
        await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_keyword_scope_markup(action),
        )


async def prompt_add_keywords_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await prompt_keyword_scope_menu(update, context, "add")


async def prompt_remove_keywords_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await prompt_keyword_scope_menu(update, context, "remove")


async def prompt_clear_keywords_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await prompt_keyword_scope_menu(update, context, "clear")


async def prompt_remove_keyword_for_source(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    source: str,
) -> None:
    _clear_pending_input_flags(context)
    context.user_data["awaiting_remove_keyword_source"] = source
    source_label = keyword_source_label(source)
    user_id = _get_user_id(update)
    if source == KEYWORD_SCOPE_ALL:
        all_current = [
            f"[{paper_source_label(item_source)}] {keyword}"
            for item_source in keyword_sources()
            for keyword in get_keywords_for_source(item_source, context.user_data, user_id=user_id)
        ]
        current = all_current
    else:
        current = get_keywords_for_source(source, context.user_data, user_id=user_id)
    body = "\n".join(f"- {k}" for k in current) if current else "(none)"
    body_html = html.escape(body)
    text = (
        f"<b>Remove Keywords • {html.escape(source_label)}</b>\n\n"
        "Send one or more saved keywords to remove.\n"
        "Separate entries with commas or send one per line.\n\n"
        "<b>Current Keywords</b>\n"
        f"<pre>{body_html}</pre>\n"
        "<b>Examples</b>\n"
        "<code>astronomy</code>\n"
        "<code>- astronomy\n- photosynthesis</code>\n"
        "<code>astronomy, photosynthesis</code>"
    )
    if update.message:
        await update.message.reply_text(
            text,
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )
        return
    chat = update.effective_chat
    if chat is not None:
        await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )


async def prompt_searchhours_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _clear_pending_input_flags(context)
    context.user_data["awaiting_search_hours_input"] = True
    if update.message:
        await update.message.reply_text(
            "<b>Search by Hours Back</b>\n\n"
            "Send how many hours back you want to search.\n\n"
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
    button_text = f"Open {MENU_BTN_TODAY} results" if safe_scope == SEARCH_SCOPE_TODAY else f"Open results ({safe_hours}h)"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(text=button_text, callback_data=f"open_matches:{safe_scope}:{safe_hours}")]]
    )


def build_more_results_markup(
    *,
    scope: str,
    hours_back: int,
    offset: int,
    results_token: int,
) -> InlineKeyboardMarkup:
    """Build the inline keyboard used to request the next page of results."""
    safe_scope = _normalize_search_scope(scope)
    safe_hours = 0 if safe_scope == SEARCH_SCOPE_GLOBAL else max(1, int(hours_back))
    safe_offset = max(0, int(offset))
    safe_token = max(0, int(results_token))
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(
                text="More Results",
                callback_data=f"more_results:{safe_token}:{safe_scope}:{safe_hours}:{safe_offset}",
            )
        ]]
    )


async def send_results_page(
    send_text: Any,
    *,
    context: ContextTypes.DEFAULT_TYPE,
    papers: Sequence[Paper],
    user_id: Optional[int],
    hours_back: int,
    scope: str,
    results_token: int,
    start_index: int = 0,
) -> None:
    """Send one page of cached search results and a `More` button if needed."""
    safe_start = max(0, int(start_index))
    safe_scope = _normalize_search_scope(scope)
    end_index = min(len(papers), safe_start + RESULTS_PAGE_SIZE)
    bookmarks = set(get_bookmarks(context.user_data, user_id=user_id)) if user_id is not None else set()

    for paper in papers[safe_start:end_index]:
        await send_text(
            format_paper_line(
                paper,
                prefer_updated_for_arxiv=safe_scope == SEARCH_SCOPE_TODAY,
            ),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=build_paper_reply_markup(
                paper,
                bookmarked=paper_ref_for(paper) in bookmarks,
            ),
        )

    if end_index < len(papers):
        await send_text(
            f"Showing {safe_start + 1}-{end_index} of {len(papers)} results.",
            reply_markup=build_more_results_markup(
                scope=safe_scope,
                hours_back=hours_back,
                offset=end_index,
                results_token=results_token,
            ),
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
                f"<b>{papers_count} matching paper(s)</b> in {scope_text}.\n"
                "Tap below to open them."
            ),
            reply_markup=build_open_results_markup(hours_back, scope=scope),
            parse_mode=ParseMode.HTML,
        )
        return
    if chat is not None:
        await context.bot.send_message(
            chat_id=chat.id,
            text=(
                f"<b>{papers_count} matching paper(s)</b> in {scope_text}.\n"
                "Tap below to open them."
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
            await update.message.reply_text("Could not identify your Telegram user.")
        return False

    keywords = parse_keywords_input(raw)
    if not keywords:
        if update.message:
            await update.message.reply_text("No valid keywords found. Send at least one keyword.")
        return False

    # Legacy behavior: /setkeywords without source sets all source lists.
    updated_by_source = {
        source: set_keywords_for_source(
            user_id=user_id,
            source=source,
            keywords=keywords,
            user_data=context.user_data,
        )
        for source in keyword_sources()
    }

    try:
        papers = await refresh_cache(
            update,
            context,
            hours_back=TODAY_HOURS_BACK,
            scope=SEARCH_SCOPE_TODAY,
        )
    except Exception as exc:
        logger.exception("Failed to refresh after setting keywords")
        if update.message:
            await update.message.reply_text(f"Keywords were saved, but the results could not be refreshed:\n{exc}")
        return False

    if update.message:
        sections = []
        for source in keyword_sources():
            items = updated_by_source.get(source, [])
            body = "\n".join(f"• {html.escape(item)}" for item in items) if items else "(none)"
            sections.append(f"<b>{html.escape(paper_source_label(source))}</b>\n{body}")
        await update.message.reply_text(
            "<b>Keywords Updated</b>\n\n"
            + "\n\n".join(sections),
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )
    await send_open_results_prompt(
        update=update,
        context=context,
        papers_count=len(papers),
        hours_back=TODAY_HOURS_BACK,
        scope=SEARCH_SCOPE_TODAY,
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
            await update.message.reply_text("Could not identify your Telegram user.")
        return False

    source_norm = str(source or "").strip().casefold()
    if source_norm != KEYWORD_SCOPE_ALL and source_norm not in ALL_PAPER_SOURCES:
        source_norm = SOURCE_ARXIV
    target_sources = keyword_sources() if source_norm == KEYWORD_SCOPE_ALL else [source_norm]
    source_label = keyword_source_label(source_norm)

    changed = False
    per_source_results: dict[str, dict[str, List[str]]] = {}
    parsed_keywords = parse_keywords_input(raw)
    if not parsed_keywords:
        if update.message:
            await update.message.reply_text("No valid keywords found. Send at least one keyword.")
        return False

    def _dedupe_fold(values: Sequence[str]) -> List[str]:
        unique: List[str] = []
        seen: set[str] = set()
        for item in values:
            key = item.casefold()
            if key in seen:
                continue
            seen.add(key)
            unique.append(item)
        return unique

    if mode == "add":
        for target_source in target_sources:
            current = get_keywords_for_source(target_source, context.user_data, user_id=user_id)
            by_fold = {item.casefold(): item for item in current}
            added: List[str] = []
            skipped: List[str] = []

            for keyword in parsed_keywords:
                key = keyword.casefold()
                if key in by_fold:
                    skipped.append(by_fold[key])
                    continue
                current.append(keyword)
                by_fold[key] = keyword
                added.append(keyword)

            source_changed = bool(added)
            if source_changed:
                changed = True
                current = set_keywords_for_source(
                    user_id=user_id,
                    source=target_source,
                    keywords=current,
                    user_data=context.user_data,
                )

            per_source_results[target_source] = {
                "added": _dedupe_fold(added),
                "skipped": _dedupe_fold(skipped),
                "removed": [],
                "missing": [],
                "current": list(current),
            }
    elif mode == "remove":
        remove_keys = {keyword.casefold() for keyword in parsed_keywords}
        for target_source in target_sources:
            current = get_keywords_for_source(target_source, context.user_data, user_id=user_id)
            by_fold = {item.casefold(): item for item in current}
            missing = [keyword for keyword in parsed_keywords if keyword.casefold() not in by_fold]
            removed: List[str] = []
            updated_current: List[str] = []
            for item in current:
                if item.casefold() in remove_keys:
                    removed.append(item)
                    continue
                updated_current.append(item)

            source_changed = bool(removed)
            if source_changed:
                changed = True
                updated_current = set_keywords_for_source(
                    user_id=user_id,
                    source=target_source,
                    keywords=updated_current,
                    user_data=context.user_data,
                )

            per_source_results[target_source] = {
                "added": [],
                "skipped": [],
                "removed": _dedupe_fold(removed),
                "missing": _dedupe_fold(missing),
                "current": list(updated_current),
            }
    else:
        raise RuntimeError(f"Unsupported keyword mode: {mode}")

    def _html_bullets(values: Sequence[str]) -> str:
        if not values:
            return "(none)"
        return "\n".join(f"• {html.escape(v)}" for v in values)

    if not changed:
        if update.message:
            if mode == "add":
                skipped_all = _dedupe_fold(
                    item
                    for target_source in target_sources
                    for item in per_source_results.get(target_source, {}).get("skipped", [])
                )
                if skipped_all:
                    await update.message.reply_text(
                        f"<b>No new keywords added in {html.escape(source_label)}</b>\n\n"
                        "<b>Already present</b>\n"
                        f"{_html_bullets(skipped_all)}",
                        reply_markup=build_main_menu_markup(),
                        parse_mode=ParseMode.HTML,
                    )
                else:
                    await update.message.reply_text(
                        f"No new keywords to add in {source_label}.",
                        reply_markup=build_main_menu_markup(),
                    )
            else:
                missing_all = _dedupe_fold(
                    item
                    for target_source in target_sources
                    for item in per_source_results.get(target_source, {}).get("missing", [])
                )
                details = f"\n\n<b>Not found</b>\n{_html_bullets(missing_all)}" if missing_all else ""
                await update.message.reply_text(
                    f"No saved keywords matched in {source_label}.{details}",
                    reply_markup=build_main_menu_markup(),
                    parse_mode=ParseMode.HTML,
                )
        return False

    if mode == "remove":
        if update.message:
            lines: List[str] = [f"<b>{html.escape(source_label)} Keywords Updated</b>"]
            for target_source in target_sources:
                result = per_source_results[target_source]
                if not result["removed"] and source_norm == KEYWORD_SCOPE_ALL:
                    continue
                lines.extend(
                    [
                        "",
                        f"<b>{html.escape(paper_source_label(target_source))}</b>",
                        f"<b>Removed</b> ({len(result['removed'])})",
                        _html_bullets(result["removed"]),
                    ]
                )
                if result["missing"]:
                    lines.extend(
                        [
                            "<b>Not found</b>",
                            _html_bullets(result["missing"]),
                        ]
                    )
                lines.extend(
                    [
                        f"<b>Current keywords</b>",
                        _html_bullets(result["current"]),
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
        papers = await refresh_cache(
            update,
            context,
            hours_back=TODAY_HOURS_BACK,
            scope=SEARCH_SCOPE_TODAY,
        )
    except Exception as exc:
        logger.exception("Failed to refresh after keyword change")
        if update.message:
            await update.message.reply_text(
                f"{source_label} keywords were saved, but the results could not be refreshed:\n{exc}",
                reply_markup=build_main_menu_markup(),
            )
        return False

    if update.message:
        hours_back = TODAY_HOURS_BACK
        scope = SEARCH_SCOPE_TODAY
        lines: List[str] = [f"<b>{html.escape(source_label)} Keywords Updated</b>"]
        for target_source in target_sources:
            result = per_source_results[target_source]
            if not result["added"] and source_norm == KEYWORD_SCOPE_ALL:
                continue
            lines.extend(
                [
                    "",
                    f"<b>{html.escape(paper_source_label(target_source))}</b>",
                    f"<b>Added</b> ({len(result['added'])})",
                    _html_bullets(result["added"]),
                ]
            )
            if result["skipped"]:
                lines.extend(
                    [
                        "<b>Skipped</b> (already present)",
                        _html_bullets(result["skipped"]),
                    ]
                )
            lines.extend(
                [
                    f"<b>Current keywords</b>",
                    _html_bullets(result["current"]),
                ]
            )
        lines.extend(
            [
                "",
                (
                    f"<b>{len(papers)} matching paper(s)</b> in the last "
                    f"<b>{hours_back} hours</b>.\n"
                    "Tap below to open them."
                ),
            ]
        )
        await update.message.reply_text(
            "\n".join(lines),
            reply_markup=build_open_results_markup(hours_back, scope=scope),
            parse_mode=ParseMode.HTML,
        )
        return True

    await send_open_results_prompt(
        update=update,
        context=context,
        papers_count=len(papers),
        hours_back=TODAY_HOURS_BACK,
        scope=SEARCH_SCOPE_TODAY,
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
            await update.message.reply_text("Could not identify your Telegram user.")
        return False

    keywords = parse_keywords_input(raw)
    if not keywords:
        if update.message:
            await update.message.reply_text("No valid keywords found. Send at least one keyword.")
        return False

    source_norm = str(source or "").strip().casefold()
    if source_norm != KEYWORD_SCOPE_ALL and source_norm not in ALL_PAPER_SOURCES:
        source_norm = SOURCE_ARXIV
    target_sources = keyword_sources() if source_norm == KEYWORD_SCOPE_ALL else [source_norm]
    source_label = keyword_source_label(source_norm)
    updated_by_source = {
        target_source: set_keywords_for_source(
            user_id=user_id,
            source=target_source,
            keywords=keywords,
            user_data=context.user_data,
        )
        for target_source in target_sources
    }

    try:
        papers = await refresh_cache(
            update,
            context,
            hours_back=TODAY_HOURS_BACK,
            scope=SEARCH_SCOPE_TODAY,
        )
    except Exception as exc:
        logger.exception("Failed to refresh after setting source keywords")
        if update.message:
            await update.message.reply_text(
                f"{source_label} keywords were saved, but the results could not be refreshed:\n{exc}",
                reply_markup=build_main_menu_markup(),
            )
        return False

    if update.message:
        updated_sections = []
        for target_source in target_sources:
            updated = updated_by_source.get(target_source, [])
            updated_body = "\n".join(f"• {html.escape(item)}" for item in updated) if updated else "(none)"
            updated_sections.append(
                f"<b>{html.escape(paper_source_label(target_source))}</b>\n{updated_body}"
            )
        await update.message.reply_text(
            f"<b>{html.escape(source_label)} Keywords Set</b>\n\n" + "\n\n".join(updated_sections),
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
        )
    await send_open_results_prompt(
        update=update,
        context=context,
        papers_count=len(papers),
        hours_back=TODAY_HOURS_BACK,
        scope=SEARCH_SCOPE_TODAY,
    )
    return True


async def apply_search_hours_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    hours: int,
) -> bool:
    if hours <= 0:
        if update.message:
            await update.message.reply_text("Hours must be greater than 0.")
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
                    "Use this format:\n"
                    "arxiv kw1, kw2 + kw3\n"
                    "biorxiv kw1, kw2 + kw3\n"
                    "medrxiv kw1, kw2 + kw3\n"
                    "chemrxiv kw1, kw2 + kw3\n"
                    "ssrn kw1, kw2 + kw3\n"
                    "ieee kw1, kw2 + kw3\n"
                    "pubmed kw1, kw2 + kw3\n\n"
                    "all kw1, kw2 + kw3\n\n"
                    "Use commas for separate alternatives (OR). Use + to search terms together (AND) in one entry.\n\n"
                    f"You can also use {MENU_BTN_ADD_KEYWORDS}.",
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
            await update.message.reply_text("Could not identify your Telegram user.")
        return False

    _clear_pending_input_flags(context)
    source_norm = str(source or "").strip().casefold()
    if source_norm != KEYWORD_SCOPE_ALL and source_norm not in ALL_PAPER_SOURCES:
        source_norm = SOURCE_ARXIV
    target_sources = keyword_sources() if source_norm == KEYWORD_SCOPE_ALL else [source_norm]
    source_label = keyword_source_label(source_norm)
    for target_source in target_sources:
        set_keywords_for_source(
            user_id=user_id,
            source=target_source,
            keywords=[],
            user_data=context.user_data,
        )

    text = (
        "Cleared keywords for all sources."
        if source_norm == KEYWORD_SCOPE_ALL
        else f"Cleared all {source_label} keywords."
    )
    if update.message:
        await update.message.reply_text(
            text,
            reply_markup=build_main_menu_markup(),
        )
    else:
        chat = update.effective_chat
        if chat is not None:
            await context.bot.send_message(
                chat_id=chat.id,
                text=text,
                reply_markup=build_main_menu_markup(),
            )
    return True


async def clearkeywords_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not identify your Telegram user.")
        return

    _clear_pending_input_flags(context)
    source = parse_keyword_source(context.args[0]) if context.args else None
    if context.args and source is None:
        if update.message:
            await update.message.reply_text(
                "Choose one source:\n"
                "- arxiv\n"
                "- biorxiv\n"
                "- medrxiv\n"
                "- chemrxiv\n"
                "- ssrn\n"
                "- ieee\n"
                "- pubmed\n\n"
                "Or use: all\n\n"
                "Or use the clear buttons in the menu.",
                reply_markup=build_main_menu_markup(),
            )
        return

    if source is not None:
        await clear_keywords_for_source(update, context, source)
        return
    else:
        for target_source in keyword_sources():
            set_keywords_for_source(
                user_id=user_id,
                source=target_source,
                keywords=[],
                user_data=context.user_data,
            )
        _save_user_setting(user_id, "custom_keywords", None)

    if update.message:
        await update.message.reply_text(
            "Cleared keywords for all sources.",
            reply_markup=build_main_menu_markup(),
        )


async def addkeyword_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        if update.message:
            await update.message.reply_text(
                "Use this format:\n"
                "arxiv quantum mechanics\n"
                "arxiv astronomy, climate change + photosynthesis\n"
                "biorxiv microbiome\n"
                "medrxiv sepsis\n"
                "chemrxiv catalyst\n"
                "ssrn market microstructure\n"
                "ieee federated learning\n"
                "pubmed genetics\n\n"
                "all llm, diffusion model\n\n"
                "Use commas for separate alternatives (OR). Use + to search terms together (AND) in one entry.\n\n"
                f"Or use {MENU_BTN_ADD_KEYWORDS}.",
                reply_markup=build_main_menu_markup(),
            )
        return
    source = parse_keyword_source(context.args[0])
    if source is None:
        if update.message:
            await update.message.reply_text(
                "Source must be one of: arxiv, biorxiv, medrxiv, chemrxiv, ssrn, ieee, pubmed, all.",
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
                "Use this format:\n"
                "arxiv quantum mechanics\n"
                "arxiv astronomy, climate change + photosynthesis\n"
                "biorxiv microbiome\n"
                "medrxiv sepsis\n"
                "chemrxiv catalyst\n"
                "ssrn market microstructure\n"
                "ieee federated learning\n"
                "pubmed genetics\n\n"
                "all llm, diffusion model\n\n"
                "When removing keywords, use the saved clause exactly.\n\n"
                f"Or use {MENU_BTN_REMOVE_KEYWORDS}.",
                reply_markup=build_main_menu_markup(),
            )
        return
    source = parse_keyword_source(context.args[0])
    if source is None:
        if update.message:
            await update.message.reply_text(
                "Source must be one of: arxiv, biorxiv, medrxiv, chemrxiv, ssrn, ieee, pubmed, all.",
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
            "Choose the time zone for your recap schedule.\n\n"
            f"<b>Current time zone</b>: <code>{html.escape(current_timezone)}</code>\n"
            f"<b>Current local time</b>: {html.escape(current_local_time)}\n"
            f"<b>Browsing</b>: {html.escape(group)} ({safe_page + 1}/{total_pages})\n\n"
            "Tap a time zone below or send an exact name such as "
            "<code>Europe/Rome</code>."
        )
        reply_markup = build_recap_timezone_choices_markup(group, safe_page)
    else:
        text = (
            "<b>Set Recap Time</b>\n\n"
            "1. Choose your time zone.\n"
            "2. Send one or more local times as HH:MM.\n\n"
            f"<b>Current time zone</b>: <code>{html.escape(current_timezone)}</code>\n"
            f"<b>Current local time</b>: {html.escape(current_local_time)}\n\n"
            "Browse time zones by region below, or send an exact "
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
        "Send one or more local times as HH:MM.\n"
        "Use commas or spaces to separate them.\n\n"
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
                "Choose a time zone from the list or send an exact name such as <code>Europe/Rome</code>.",
                reply_markup=build_main_menu_markup(),
                parse_mode=ParseMode.HTML,
            )
        return False

    parts = cleaned.split(maxsplit=1)
    tz_name = resolve_recap_timezone_name(parts[0])
    if tz_name is None:
        if update.message:
            await update.message.reply_text(
                "Time zone not recognized.\n"
                "Send an exact name such as <code>Europe/Rome</code>, <code>US/Eastern</code>, or <code>UTC</code>.",
                reply_markup=build_main_menu_markup(),
                parse_mode=ParseMode.HTML,
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
            await update.message.reply_text("Could not identify your Telegram user.")
        return

    enabled, recap_times, chat_id = get_daily_recap_config(user_id)
    recap_timezone = get_daily_recap_timezone(user_id)
    status = "enabled" if enabled else "disabled"
    chat_label = str(chat_id) if chat_id is not None else "(not set)"
    times_label = ", ".join(recap_times)

    if update.message:
        await update.message.reply_text(
            "<b>Daily Recap</b>\n\n"
            f"<b>Status</b>: {html.escape(status)}\n"
            f"<b>Time zone</b>: <code>{html.escape(recap_timezone)}</code>\n"
            f"<b>Times</b>: {html.escape(times_label)}\n"
            f"<b>Local time now</b>: {html.escape(format_recap_timezone_clock(recap_timezone))}\n"
            f"<b>Chat ID</b>: <code>{html.escape(chat_label)}</code>\n\n"
            f"Use <b>{html.escape(MENU_BTN_DAILY_RECAP)}</b> and <b>{html.escape(MENU_BTN_SET_RECAP_TIME)}</b> to change these settings.",
            reply_markup=build_main_menu_markup(),
            parse_mode=ParseMode.HTML,
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
            await update.message.reply_text("Could not identify this user or chat for recap settings.")
        return False

    recap_timezone = _coerce_daily_recap_timezone(
        context.user_data.get("pending_recap_timezone") or get_daily_recap_timezone(user_id)
    )
    recap_times = parse_daily_recap_times(raw)
    if recap_times is None:
        if update.message:
            await update.message.reply_text(
                "Time format not recognized.\n"
                f"Use one or more HH:MM times in {recap_timezone}.\n"
                "Examples: 09:30 or 09:30, 21:00."
            )
        return False
    if not recap_times:
        if update.message:
            await update.message.reply_text(
                "Send at least one time.\n"
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
                await update.message.reply_text(f"Times were saved, but scheduling failed:\n{exc}")
            return False

    if update.message:
        times_label = ", ".join(recap_times)
        await update.message.reply_text(
            f"Recap times saved: {times_label} ({recap_timezone})."
            + (" Recap is active." if enabled else " Recap is currently off."),
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
            await update.message.reply_text("Could not identify this user or chat for recap settings.")
        return False

    _, recap_times, _ = get_daily_recap_config(user_id)
    recap_timezone = get_daily_recap_timezone(user_id)
    _save_user_setting(user_id, "daily_recap_enabled", bool(enabled))
    _save_user_setting(user_id, "daily_recap_times", recap_times)
    _save_user_setting(user_id, "daily_recap_time", recap_times[0])
    _save_user_setting(user_id, "daily_recap_timezone", recap_timezone)
    _save_user_setting(user_id, "daily_recap_chat_id", int(chat.id))
    _set_metrics_recap_enabled(user_id, enabled, chat_id=int(chat.id))

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
                await update.message.reply_text(f"Could not enable the daily recap:\n{exc}")
            return False
        times_label = ", ".join(recap_times)
        message = (
            f"Daily recap is on.\n"
            f"You will receive updates every day at {times_label} in {recap_timezone}.\n"
            f"Each recap covers the last {DAILY_RECAP_HOURS} hours."
        )
    else:
        remove_daily_recap_job(context.application, user_id)
        message = "Daily recap is off."

    if update.message:
        await update.message.reply_text(message, reply_markup=build_main_menu_markup())
    return True


async def toggledailyrecap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    if user_id is None:
        if update.message:
            await update.message.reply_text("Could not identify your Telegram user.")
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
            f"Use {MENU_BTN_DAILY_RECAP} to turn the recap on or off, and {MENU_BTN_RECAP_STATUS} to view the current settings.",
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
            await query.answer("This time zone page is invalid.", show_alert=True)
            return
        await query.answer()
        await show_recap_timezone_picker(update, context, group=group, page=page)
        return

    if data.startswith("rtzpick:"):
        raw_index = data.removeprefix("rtzpick:").strip()
        try:
            zone_name = AVAILABLE_RECAP_TIMEZONES[int(raw_index)]
        except (ValueError, IndexError):
            await query.answer("Time zone not recognized.", show_alert=True)
            return
        await query.answer(f"Selected {zone_name}")
        await prompt_recap_local_time_input(update, context, zone_name)
        return

    await query.answer()


async def keyword_scope_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return

    data = query.data or ""
    if not data.startswith("kwmenu:"):
        await query.answer()
        return

    parts = data.split(":", 2)
    if len(parts) != 3:
        await query.answer("Invalid keyword action.", show_alert=True)
        return
    _, action, source = parts
    action_norm = normalize_text(action).casefold()
    source_norm = normalize_text(source).casefold()
    if action_norm not in {"add", "remove", "clear"}:
        await query.answer("Invalid keyword action.", show_alert=True)
        return
    if source_norm != KEYWORD_SCOPE_ALL and source_norm not in ALL_PAPER_SOURCES:
        await query.answer("Invalid source.", show_alert=True)
        return

    await query.answer()
    if action_norm == "add":
        await prompt_add_keyword_for_source(update, context, source_norm)
        return
    if action_norm == "remove":
        await prompt_remove_keyword_for_source(update, context, source_norm)
        return
    await clear_keywords_for_source(update, context, source_norm)


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
        await query.answer("Invalid time range.", show_alert=True)
        return

    if hours_back <= 0:
        await query.answer("Invalid time range.", show_alert=True)
        return

    await query.answer("Loading results...")
    await run_search_for_hours(
        update=update,
        context=context,
        hours_back=hours_back,
        force_refresh=True,
        scope=scope,
    )


async def more_results_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat = update.effective_chat
    if query is None:
        return

    data = query.data or ""
    if not data.startswith("more_results:"):
        await query.answer()
        return

    parts = data.split(":", 4)
    if len(parts) != 5:
        await query.answer("This results page is invalid.", show_alert=True)
        return

    _, raw_token, raw_scope, raw_hours, raw_offset = parts
    scope = _normalize_search_scope(raw_scope)
    try:
        results_token = int(raw_token)
        hours_back = int(raw_hours)
        offset = int(raw_offset)
    except ValueError:
        await query.answer("This results page is invalid.", show_alert=True)
        return

    if offset < 0:
        await query.answer("This results page is invalid.", show_alert=True)
        return
    if scope == SEARCH_SCOPE_GLOBAL:
        if hours_back != 0:
            await query.answer("This results page is invalid.", show_alert=True)
            return
    elif hours_back <= 0:
        await query.answer("This results page is invalid.", show_alert=True)
        return

    cached_token = int(context.user_data.get("results_token", 0) or 0)
    papers = get_cached_papers(context, hours_back=hours_back, scope=scope)
    if cached_token != results_token or not papers:
        await query.answer(
            f"These results are no longer current. Open {MENU_BTN_TODAY} again.",
            show_alert=True,
        )
        return

    if offset >= len(papers):
        await query.answer("No more results.")
        if query.message is not None:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
        return

    await query.answer("Loading more results...")
    if query.message is not None:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            logger.exception("Could not clear More button markup")

    async def send_text(
        text: str,
        *,
        reply_markup: Optional[Any] = None,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: Optional[bool] = None,
    ) -> None:
        if chat is None:
            raise RuntimeError("Could not determine the Telegram chat for paginated results.")
        await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )

    await send_results_page(
        send_text,
        context=context,
        papers=papers,
        user_id=_get_user_id(update),
        hours_back=hours_back,
        scope=scope,
        results_token=results_token,
        start_index=offset,
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
        MENU_BTN_ADD_KEYWORDS.casefold(): prompt_add_keywords_menu,
        MENU_BTN_REMOVE_KEYWORDS.casefold(): prompt_remove_keywords_menu,
        MENU_BTN_CLEAR_KEYWORDS.casefold(): prompt_clear_keywords_menu,
        MENU_BTN_SEARCH_HOURS.casefold(): prompt_searchhours_input,
        MENU_BTN_DAILY_RECAP.casefold(): toggledailyrecap_cmd,
        MENU_BTN_SET_RECAP_TIME.casefold(): prompt_setrecaptime_input,
        MENU_BTN_RECAP_STATUS.casefold(): dailyrecap_status_cmd,
        MENU_BTN_BOOKMARKS.casefold(): bookmarks_cmd,
        MENU_BTN_HELP.casefold(): help_cmd,
        MENU_BTN_MORE.casefold(): prompt_more_menu,
    }

    pending_flags = [
        "awaiting_keywords_input",
        "awaiting_add_keyword_source",
        "awaiting_remove_keyword_source",
        "awaiting_search_hours_input",
        "awaiting_hours_input",
        "awaiting_recap_timezone_input",
        "awaiting_recap_time_input",
        "awaiting_report_input",
        "awaiting_global_search_query",
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
            await message.reply_text("Hours must be an integer. Action cancelled.")
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

    if context.user_data.get("awaiting_report_input", False):
        await apply_report_input(update, context, text)
        context.user_data.pop("awaiting_report_input", None)
        return

    if context.user_data.get("awaiting_global_search_query", False):
        success = await apply_global_search_query_input(update, context, text)
        if success:
            context.user_data.pop("awaiting_global_search_query", None)
        return

    await message.reply_text(
        "Use the buttons below.",
        reply_markup=build_main_menu_markup(),
    )


async def debugquery_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = _get_user_id(update)
    query = context.user_data.get("last_query", "")
    raw_count = context.user_data.get("last_raw_entry_count", 0)
    raw_breakdown = context.user_data.get("last_raw_entry_breakdown", {})
    request_url = context.user_data.get("last_request_url", "")
    keywords_by_source = get_keywords_by_source(context.user_data, user_id=user_id)
    arxiv_keywords = keywords_by_source.get(SOURCE_ARXIV, [])
    biorxiv_keywords = keywords_by_source.get(SOURCE_BIORXIV, [])
    medrxiv_keywords = keywords_by_source.get(SOURCE_MEDRXIV, [])
    chemrxiv_keywords = keywords_by_source.get(SOURCE_CHEMRXIV, [])
    ssrn_keywords = keywords_by_source.get(SOURCE_SSRN, [])
    ieee_keywords = keywords_by_source.get(SOURCE_IEEE, [])
    pubmed_keywords = keywords_by_source.get(SOURCE_PUBMED, [])
    hours_back = int(context.user_data.get("cache_hours_back", TODAY_HOURS_BACK))
    scope = _normalize_search_scope(context.user_data.get("cache_scope", SEARCH_SCOPE_TODAY))

    if not query:
        arxiv_query = build_arxiv_query(keywords=arxiv_keywords)
        pubmed_query = build_pubmed_query(keywords=pubmed_keywords)
        query_lines: List[str] = []
        if arxiv_query:
            query_lines.append(f"arXiv: {arxiv_query}")
        if biorxiv_keywords:
            query_lines.append(f"bioRxiv keywords: {', '.join(biorxiv_keywords)}")
        if medrxiv_keywords:
            query_lines.append(f"medRxiv keywords: {', '.join(medrxiv_keywords)}")
        if chemrxiv_keywords:
            query_lines.append(f"ChemRxiv keywords: {', '.join(chemrxiv_keywords)}")
        if ssrn_keywords:
            query_lines.append(f"SSRN keywords: {', '.join(ssrn_keywords)}")
        if ieee_keywords:
            query_lines.append(f"IEEE keywords: {', '.join(ieee_keywords)}")
        if pubmed_query:
            query_lines.append(f"PubMed: {pubmed_query}")
        query = "\n".join(query_lines) if query_lines else "(no active query)"

    arxiv_raw = 0
    biorxiv_raw = 0
    medrxiv_raw = 0
    chemrxiv_raw = 0
    ssrn_raw = 0
    ieee_raw = 0
    pubmed_raw = 0
    if isinstance(raw_breakdown, dict):
        arxiv_raw = int(raw_breakdown.get("arxiv", 0) or 0)
        biorxiv_raw = int(raw_breakdown.get("biorxiv", 0) or 0)
        medrxiv_raw = int(raw_breakdown.get("medrxiv", 0) or 0)
        chemrxiv_raw = int(raw_breakdown.get("chemrxiv", 0) or 0)
        ssrn_raw = int(raw_breakdown.get("ssrn", 0) or 0)
        ieee_raw = int(raw_breakdown.get("ieee", 0) or 0)
        pubmed_raw = int(raw_breakdown.get("pubmed", 0) or 0)

    msg = (
        f"Search scope: {_describe_search_window(scope, hours_back)}\n"
        f"Hours back: {hours_back}\n"
        f"arXiv keywords: {arxiv_keywords if arxiv_keywords else '(none)'}\n"
        f"bioRxiv keywords: {biorxiv_keywords if biorxiv_keywords else '(none)'}\n"
        f"medRxiv keywords: {medrxiv_keywords if medrxiv_keywords else '(none)'}\n"
        f"ChemRxiv keywords: {chemrxiv_keywords if chemrxiv_keywords else '(none)'}\n"
        f"SSRN keywords: {ssrn_keywords if ssrn_keywords else '(none)'}\n"
        f"IEEE keywords: {ieee_keywords if ieee_keywords else '(none)'}\n"
        f"PubMed keywords: {pubmed_keywords if pubmed_keywords else '(none)'}\n\n"
        f"Current queries:\n{query}\n\n"
        f"Last raw entry count: {raw_count} "
        "("
        f"arXiv: {arxiv_raw}, "
        f"bioRxiv: {biorxiv_raw}, "
        f"medRxiv: {medrxiv_raw}, "
        f"ChemRxiv: {chemrxiv_raw}, "
        f"SSRN: {ssrn_raw}, "
        f"IEEE: {ieee_raw}, "
        f"PubMed: {pubmed_raw}"
        ")"
    )

    if request_url:
        msg += f"\n\nLast request URL:\n{request_url}"

    if update.message:
        await update.message.reply_text(msg)


async def refresh_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Backward-compatible alias: refresh now behaves exactly like Last 24h.
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
            await send_text(f"Could not load papers:\n{exc}")
            return False

    if not papers:
        query = context.user_data.get("last_query", "")

        if not query:
            text = (
                "No keywords set yet.\n\n"
                f"Use {MENU_BTN_ADD_KEYWORDS} and choose a source "
                "to add your keywords."
            )
        else:
            text = (
                f"No matching papers found in {_describe_search_window(effective_scope, hours_back)}.\n"
                "\n"
                f"Review your keywords in {MENU_BTN_KEYWORDS} and try again."
            )

        await send_text(text, reply_markup=build_main_menu_markup())
        return True

    await send_text(
        f"{len(papers)} matching paper(s) found in {_describe_search_window(effective_scope, hours_back)}.",
        reply_markup=build_main_menu_markup(),
    )

    await send_results_page(
        send_text,
        context=context,
        papers=papers,
        user_id=user_id,
        hours_back=hours_back,
        scope=effective_scope,
        results_token=int(context.user_data.get("results_token", 0) or 0),
        start_index=0,
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
        await query.answer("Paper identifier is missing.", show_alert=True)
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
                    text=f"Could not load papers:\n{exc}",
                )
            return
        paper = find_cached_paper_by_ref(context, source, paper_id)

    if paper is None:
        await query.answer(
            f"This paper is no longer in the current list. Open {MENU_BTN_TODAY} again.",
            show_alert=True,
        )
        return

    pdf_link = resolve_paper_pdf_link(paper)
    if not pdf_link:
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
            document=pdf_link,
            caption=caption,
        )
    except Exception:
        logger.exception("Sending PDF through Telegram callback failed")
        if chat is not None:
            await context.bot.send_message(
                chat_id=chat.id,
                text=(
                    "Could not send the PDF through Telegram.\n\n"
                    f"Direct PDF link:\n{pdf_link}"
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
        await query.answer("Paper identifier is missing.", show_alert=True)
        return
    source, paper_id = parsed_ref
    paper_ref = make_paper_ref(source, paper_id)

    user_id = _get_user_id(update)
    if user_id is None:
        await query.answer("Could not identify your Telegram user.", show_alert=True)
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
            await update.message.reply_text("Could not identify your Telegram user.")
        return

    bookmark_ids = get_bookmarks(context.user_data, user_id=user_id)
    if not bookmark_ids:
        if update.message:
            await update.message.reply_text(
                f"No bookmarks yet.\nUse ⭐ on a paper from {MENU_BTN_TODAY}.",
                reply_markup=build_main_menu_markup(),
            )
        return

    cached_by_ref = {
        paper_ref_for(paper): paper
        for paper in context.user_data.get("papers", [])
        if isinstance(paper, Paper)
    }
    resolved_from_cache: List[Paper] = []
    unresolved_refs: List[str] = []
    seen_refs: set[str] = set()
    for ref in bookmark_ids:
        if ref in seen_refs:
            continue
        seen_refs.add(ref)
        cached = cached_by_ref.get(ref)
        if cached is not None:
            resolved_from_cache.append(cached)
        else:
            unresolved_refs.append(ref)

    fetched: List[Paper] = []
    missing: List[str] = []
    try:
        if unresolved_refs:
            fetched, missing = await fetch_papers_by_refs(unresolved_refs)
    except Exception as exc:
        logger.exception("Failed to fetch bookmarked papers")
        if update.message:
            await update.message.reply_text(
                f"Could not load bookmarks:\n{exc}",
                reply_markup=build_main_menu_markup(),
            )
        return

    papers = resolved_from_cache + fetched
    for idx, paper in enumerate(papers, start=1):
        paper.index = idx

    if not papers:
        if update.message:
            await update.message.reply_text(
                "No bookmarked papers could be loaded right now.",
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

        _initialize_metrics_db()
        _sync_metrics_users_from_settings()
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

    app.add_handler(TypeHandler(Update, _track_user_metrics_callback), group=-1)
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
    app.add_handler(CommandHandler("report", report_cmd))
    app.add_handler(CommandHandler("setreporttarget", setreporttarget_cmd))
    app.add_handler(CommandHandler("coffee", coffee_cmd))
    app.add_handler(CommandHandler("dailyrecap", dailyrecap_cmd))
    app.add_handler(CommandHandler("setrecaptime", setrecaptime_cmd))
    app.add_handler(CommandHandler("userstats", _userstats_cmd))
    app.add_handler(CommandHandler("debugquery", debugquery_cmd))
    app.add_handler(CommandHandler("refresh", refresh_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_text_router))
    app.add_handler(CallbackQueryHandler(more_menu_callback, pattern=r"^moremenu:"))
    app.add_handler(CallbackQueryHandler(global_search_callback, pattern=r"^gsearch:"))
    app.add_handler(CallbackQueryHandler(recap_timezone_callback, pattern=r"^rtz"))
    app.add_handler(CallbackQueryHandler(keyword_scope_callback, pattern=r"^kwmenu:"))
    app.add_handler(CallbackQueryHandler(open_matches_callback, pattern=r"^open_matches:"))
    app.add_handler(CallbackQueryHandler(more_results_callback, pattern=r"^more_results:"))
    app.add_handler(CallbackQueryHandler(pdf_callback, pattern=r"^pdf:"))
    app.add_handler(CallbackQueryHandler(bookmark_callback, pattern=r"^bm:"))

    logger.info("Bot starting")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
