# tay_weather_bot.py
#
# Tay Township Weather Bot (Final "F" style)
#
# PURPOSE
#   - Reads Environment Canada ATOM alert feed (source of truth)
#   - Writes an RSS file (tay-weather.xml) for your site/consumers
#   - Posts to X (Twitter) and Facebook when enabled
#   - Uses Telegram as an approval gate / preview channel
#   - Chooses images in this order:
#       (1) Google Drive folder (your curated photos)  ✅ FIRST
#       (2) Ontario 511 highway cameras (CR-29)        ✅ FALLBACK
#       BUT:
#         - If the alert is a WIND WARNING, do NOT use highway camera images
#           (so a "wind warning" won’t show winter-road cameras)
#
# CARE STATEMENTS (Facebook only)
#   - X should be clean/short and NOT include the care statement.
#   - Facebook SHOULD append a care statement pulled from the Google Sheet
#     tab: "CareStatements"
#
# IMPORTANT:
#   - Severity emoji logic must NOT be changed. (It is preserved as-is.)

import base64
import datetime as dt
import email.utils
import hashlib
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build
from PIL import Image
from requests_oauthlib import OAuth1

import facebook_poster as fb
from telegram_gate import (
    ingest_telegram_actions,
    maybe_send_reminders,
    ensure_preview_sent,
    update_preview,
    decision_for,
    is_pending,
    is_expired,
    mark_denied,
    remix_count_for,
    custom_text_for,
    clear_custom_text,
    wait_for_decision,
    tg_send_message,
)

# =============================================================================
# Feature toggles (controlled by GitHub Actions env)
# =============================================================================
ENABLE_X_POSTING = os.getenv("ENABLE_X_POSTING", "false").lower() == "true"
ENABLE_FB_POSTING = os.getenv("ENABLE_FB_POSTING", "false").lower() == "true"
TELEGRAM_ENABLE_GATE = os.getenv("TELEGRAM_ENABLE_GATE", "true").lower() == "true"

# Legacy compatibility: TEST_TWEET implies X test
TEST_TWEET = os.getenv("TEST_TWEET", "false").lower() == "true"

# Per-platform test mode (workflow_dispatch inputs set these)
TEST_X = os.getenv("TEST_X", "false").lower() == "true" or TEST_TWEET
TEST_FACEBOOK = os.getenv("TEST_FACEBOOK", "false").lower() == "true"

# =============================================================================
# RUN MODES (hard safety gate)
# =============================================================================
RUN_MODE = (os.getenv("RUN_MODE", "live") or "live").strip().lower()
_VALID_RUN_MODES = {"live", "test_telegram_buttons_no_post", "test_sample_alert_no_post"}
if RUN_MODE not in _VALID_RUN_MODES:
    print(f"⚠️ Invalid RUN_MODE='{RUN_MODE}'. Falling back to 'live'.")
    RUN_MODE = "live"

# Single source of truth for “physically cannot post”
NO_POST_MODE = RUN_MODE != "live"

# IMPORTANT:
#   These are the ONLY flags you should use when deciding to post.
EFFECTIVE_ENABLE_X_POSTING = ENABLE_X_POSTING and (not NO_POST_MODE)
EFFECTIVE_ENABLE_FB_POSTING = ENABLE_FB_POSTING and (not NO_POST_MODE)

# =============================================================================
# Paths (repo workspace)
# =============================================================================
STATE_PATH = "state.json"
RSS_PATH = "tay-weather.xml"
ROTATED_X_REFRESH_TOKEN_PATH = "x_refresh_token_rotated.txt"

USER_AGENT = "tay-weather-rss-bot/1.1"

# =============================================================================
# URLs and feed settings
# =============================================================================
ALERT_FEED_URL = os.getenv("ALERT_FEED_URL", "https://weather.gc.ca/rss/battleboard/onrm94_e.xml").strip()
DISPLAY_AREA_NAME = "Tay Township area"

# Public URLs:
TAY_ALERTS_URL = (os.getenv("TAY_ALERTS_URL") or "").strip()
TAY_COORDS_URL = (os.getenv("TAY_COORDS_URL") or "https://tayweather.short.gy/alert").strip()

# =============================================================================
# Ontario 511 camera resolver settings
# =============================================================================
ON511_CAMERAS_API = "https://511on.ca/api/v2/get/cameras"
ON511_CAMERA_KEYWORD = os.getenv("ON511_CAMERA_KEYWORD", "CR-29").strip() or "CR-29"

# =============================================================================
# Cooldown policy
# =============================================================================
COOLDOWN_MINUTES = {
    "warning": 60,
    "watch": 120,
    "advisory": 180,
    "statement": 240,
    "other": 180,
    "default": 180,
}
GLOBAL_COOLDOWN_MINUTES = 5

# =============================================================================
# Telegram helper: final "test succeeded" confirmation
# =============================================================================
def tg_send_test_success(note: str = "") -> None:
    """
    Sends a final confirmation message to Telegram for MANUAL TEST RUNS.
    """
    if not TELEGRAM_ENABLE_GATE:
        return
    try:
        msg = "✅ TEST COMPLETE — workflow finished and exited cleanly."
        if note:
            msg += f"\n{note.strip()}"
        tg_send_message(msg)
    except Exception as e:
        print(f"⚠️ Telegram test success message failed: {e}")


def warning_delay_elapsed(state: Dict[str, Any], token: str) -> bool:
    """
    WARNING policy:
      - Wait TELEGRAM_PREVIEW_DELAY_MIN minutes after preview was created,
        unless denied.

    NOTE:
      - pending_approvals[token]["created_at"] is ISO time (e.g. 2026-01-09T01:23:45Z)
    """
    delay_min = int(os.getenv("TELEGRAM_PREVIEW_DELAY_MIN", "15"))
    pending = (state.get("pending_approvals") or {}).get(token) or {}
    created_at = (pending.get("created_at") or "").strip()

    if not created_at:
        return True  # fail-open: don't block a warning indefinitely

    try:
        created_dt = dt.datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=dt.timezone.utc)
        now = dt.datetime.now(dt.timezone.utc)
        return (now - created_dt).total_seconds() >= (delay_min * 60)
    except Exception:
        return True


def _no_post_guard(platform: str) -> bool:
    """
    Hard safety gate.
    Returns True if posting should be blocked.
    """
    if NO_POST_MODE:
        print(f"🧯 NO-POST MODE ACTIVE ({RUN_MODE}) — blocked posting to {platform}.")
        return True
    return False


# =============================================================================
# Helper: normalize text for stable comparisons
# =============================================================================
def normalize(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def safe_int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default


def text_hash(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

# ---------------------------------------------------------------------
# ENDED / ALL-CLEAR detection (keeps core logic intact)
# ---------------------------------------------------------------------
_ENDED_PHRASES = (
    " no longer in effect",
    " is no longer in effect",
    " has ended",
    " ended",
)

def is_alert_ended(title: str, summary: str) -> bool:
    """
    Returns True when Environment Canada issues an 'ended' / 'no longer in effect' entry.
    This does NOT change warning/watch/advisory wording. It's just a state detector.
    """
    t = f"{title or ''} {summary or ''}".lower()
    return any(p in t for p in _ENDED_PHRASES)

# =============================================================================
# Severity emoji (match Environment Canada alert colours)
# IMPORTANT: DO NOT CHANGE THIS LOGIC (per your instruction)
# =============================================================================
def severity_emoji(title: str) -> str:
    """
    Environment Canada colour-coded alerts:
      Yellow = 🟡
      Orange = 🟠
      Red    = 🔴
    If not colour-coded, return "" (no emoji).
    """
    t = (title or "").strip().lower()

    # Prefer explicit colour words first (EC banner titles)
    if t.startswith("red "):
        return "🔴"
    if t.startswith("orange "):
        return "🟠"
    if t.startswith("yellow "):
        return "🟡"

    return ""


def classify_alert_kind(title: str) -> str:
    """
    Used for cooldown bucket AND Telegram gate policy.
    """
    t = (title or "").lower()
    if "warning" in t:
        return "warning"
    if "watch" in t:
        return "watch"
    if "advisory" in t:
        return "advisory"
    if "statement" in t:
        return "statement"
    return "other"


# =============================================================================
# State file (dedupe/cooldowns/telegram gate memory)
# =============================================================================
def load_state() -> dict:
    default = {
        "seen_ids": [],
        "posted_guids": [],
        "posted_text_hashes": [],
        "cooldowns": {},
        "global_last_post_ts": 0,

        # Telegram gate state
        "pending_approvals": {},
        "approval_decisions": {},
        "telegram_last_update_id": 0,
        "telegram_last_signal": None,
        "test_gate_token": "",

        # for remix/custom refresh bookkeeping
        "telegram_last_remix_seen": {},
    }

    if not os.path.exists(STATE_PATH):
        return default

    try:
        raw = open(STATE_PATH, "r", encoding="utf-8").read().strip()
        if not raw:
            return default
        data = json.loads(raw)
        if not isinstance(data, dict):
            return default
    except Exception:
        return default

    for k, v in default.items():
        data.setdefault(k, v)
    return data


def save_state(state: dict) -> None:
    state["seen_ids"] = state.get("seen_ids", [])[-5000:]
    state["posted_guids"] = state.get("posted_guids", [])[-5000:]
    state["posted_text_hashes"] = state.get("posted_text_hashes", [])[-5000:]

    cds = state.get("cooldowns", {})
    if isinstance(cds, dict) and len(cds) > 5000:
        items = sorted(cds.items(), key=lambda kv: kv[1], reverse=True)[:4000]
        state["cooldowns"] = dict(items)

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


# =============================================================================
# Cooldown logic
# =============================================================================
def group_key_for_cooldown(area_name: str, kind: str) -> str:
    raw = f"{normalize(area_name)}|{normalize(kind)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def cooldown_allows_post(state: Dict[str, Any], area_name: str, kind: str) -> Tuple[bool, str]:
    now_ts = int(time.time())

    last_global = safe_int(state.get("global_last_post_ts", 0), 0)
    if last_global and (now_ts - last_global) < (GLOBAL_COOLDOWN_MINUTES * 60):
        return False, f"Global cooldown active ({GLOBAL_COOLDOWN_MINUTES}m)."

    key = group_key_for_cooldown(area_name, kind)
    cooldowns = state.get("cooldowns", {}) if isinstance(state.get("cooldowns"), dict) else {}
    last_ts = safe_int(cooldowns.get(key, 0), 0)

    mins = COOLDOWN_MINUTES.get(kind, COOLDOWN_MINUTES["default"])
    if last_ts and (now_ts - last_ts) < (mins * 60):
        return False, f"Cooldown active for group ({mins}m)."

    return True, "OK"


def mark_posted(state: Dict[str, Any], area_name: str, kind: str) -> None:
    now_ts = int(time.time())
    key = group_key_for_cooldown(area_name, kind)
    state.setdefault("cooldowns", {})
    state["cooldowns"][key] = now_ts
    state["global_last_post_ts"] = now_ts


# =============================================================================
# "More:" URL resolver
# =============================================================================
def _url_looks_ok(url: str) -> bool:
    url = (url or "").strip()
    if not url:
        return False
    try:
        r = requests.head(url, allow_redirects=True, headers={"User-Agent": USER_AGENT}, timeout=(5, 15))
        if r.status_code < 400:
            return True
    except Exception:
        pass
    try:
        r = requests.get(url, allow_redirects=True, headers={"User-Agent": USER_AGENT}, timeout=(5, 15))
        return r.status_code < 400
    except Exception:
        return False


def resolve_more_info_url() -> str:
    if TAY_ALERTS_URL:
        return TAY_ALERTS_URL
    return TAY_COORDS_URL


# =============================================================================
# ATOM feed parsing
# =============================================================================
ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}


def _parse_atom_dt(s: str) -> dt.datetime:
    if not s:
        return dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def fetch_feed_entries(feed_url: str, retries: int = 3, timeout: Tuple[int, int] = (5, 20)) -> List[Dict[str, Any]]:
    last_err: Optional[Exception] = None

    def _parse_rss_dt(s: str) -> dt.datetime:
        s = (s or "").strip()
        if not s:
            return dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        try:
            parsed = email.utils.parsedate_to_datetime(s)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.astimezone(dt.timezone.utc)
        except Exception:
            return dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)

    for attempt in range(retries):
        try:
            r = requests.get(feed_url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
            r.raise_for_status()
            root = ET.fromstring(r.content)

            entries: List[Dict[str, Any]] = []

            # --- Atom ---
            if "Atom" in (root.tag or "") or root.tag.endswith("feed"):
                for e in root.findall("a:entry", ATOM_NS):
                    title = (e.findtext("a:title", default="", namespaces=ATOM_NS) or "").strip()

                    link = ""
                    link_el = e.find("a:link[@type='text/html']", ATOM_NS)
                    if link_el is None:
                        link_el = e.find("a:link", ATOM_NS)
                    if link_el is not None:
                        link = (link_el.get("href") or "").strip()

                    updated = (e.findtext("a:updated", default="", namespaces=ATOM_NS) or "").strip()
                    published = (e.findtext("a:published", default="", namespaces=ATOM_NS) or "").strip()
                    entry_id = (e.findtext("a:id", default="", namespaces=ATOM_NS) or "").strip()
                    summary = (e.findtext("a:summary", default="", namespaces=ATOM_NS) or "").strip()

                    entries.append(
                        {
                            "id": entry_id,
                            "title": title,
                            "link": link,
                            "summary": summary,
                            "updated_dt": _parse_atom_dt(updated or published),
                        }
                    )

            # --- RSS 2.0 ---
            else:
                ch = root.find("channel") if root.tag == "rss" else root.find(".//channel")
                if ch is not None:
                    for it in ch.findall("item"):
                        title = (it.findtext("title", default="") or "").strip()
                        link = (it.findtext("link", default="") or "").strip()
                        guid = (it.findtext("guid", default="") or "").strip()
                        pub = (it.findtext("pubDate", default="") or "").strip()
                        desc = (it.findtext("description", default="") or "").strip()
                        entries.append(
                            {
                                "id": guid or link or title,
                                "title": title,
                                "link": link,
                                "summary": desc,
                                "updated_dt": _parse_rss_dt(pub),
                            }
                        )

            entries = [e for e in entries if (e.get("id") or "").strip()]
            entries.sort(
                key=lambda x: x.get("updated_dt") or dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc),
                reverse=True,
            )
            return entries

        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
                continue
            raise

    raise last_err if last_err else RuntimeError("Failed to fetch feed")


def atom_title_for_tay(title: str) -> str:
    if not title:
        return title
    t = title.replace(", Midland - Coldwater - Orr Lake", f" ({DISPLAY_AREA_NAME})")
    t = t.replace("Midland - Coldwater - Orr Lake", DISPLAY_AREA_NAME)
    return t


def normalize_alert_title(title: str) -> str:
    t = (title or "").strip()
    t = t.replace("–", "-").replace("—", "-")
    t = re.sub(r"\s+", " ", t).strip()
    return t

# =============================================================================
# Environment Canada detail extraction
# =============================================================================
def _extract_details_lines_from_ec(official_url: str) -> List[str]:
    """
    Extracts short 'What' and 'When' lines from the official Environment Canada alert page.
    Falls back to a short weather-related sentence if structured fields aren't found.
    """
    official_url = (official_url or "").strip()
    if not official_url:
        return []

    r = requests.get(official_url, headers={"User-Agent": USER_AGENT}, timeout=(10, 30))
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    raw = soup.get_text("\n")
    lines = [ln.strip() for ln in raw.splitlines()]
    lines = [ln for ln in lines if ln]
    text = " ".join(lines)

    def _clean_details(s: str) -> str:
        s = re.sub(r"\s+", " ", (s or "")).strip()
        if not s:
            return ""
        if re.match(r"^\s*issued\b", s, flags=re.IGNORECASE):
            return ""
        if "bookmarking your customized list" in s.lower():
            return ""
        if "continue to monitor" in s.lower():
            return ""
        if "share this page" in s.lower():
            return ""
        return s

    m_what = re.search(r"What:\s*(.+?)(?=\s+(When:|Where:|Additional information:))", text, re.IGNORECASE)
    m_when = re.search(r"When:\s*(.+?)(?=\s+(Where:|Additional information:)|$)", text, re.IGNORECASE)

    out: List[str] = []
    if m_what:
        what = _clean_details(m_what.group(1))
        if what:
            out.append(what.rstrip(".") + ".")
    if m_when:
        when = _clean_details(m_when.group(1))
        if when:
            out.append(when.rstrip(".") + ".")

    if out:
        return out[:2]

    # Fallback: pick the first decent weather-related sentence
    weather_keywords = (
        "snow", "snowfall", "squall", "rain", "freezing", "ice", "wind", "fog",
        "visibility", "blowing", "drifting", "thunder", "heat", "cold",
    )
    sentences = re.split(r"(?<=[.!?])\s+", text)
    for s in sentences[:140]:
        s = s.strip()
        if 25 <= len(s) <= 240 and any(k in s.lower() for k in weather_keywords):
            sl = s.lower()
            if "environment canada" in sl or "continue to monitor" in sl:
                continue
            candidate = _clean_details(s)
            if candidate:
                return [candidate.rstrip(".") + "."]
    return []


def _extract_recommended_action_from_ec(official_url: str) -> str:
    """
    Pulls the 'Recommended action:' section from the official Environment Canada alert page, when present.
    Returns a single string (empty if not found).
    """
    official_url = (official_url or "").strip()
    if not official_url:
        return ""

    r = requests.get(official_url, headers={"User-Agent": USER_AGENT}, timeout=(10, 30))
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    raw = soup.get_text("\n")
    lines = [ln.strip() for ln in raw.splitlines()]
    lines = [ln for ln in lines if ln]
    text = " ".join(lines)

    def _clean_action(s: str) -> str:
        s = re.sub(r"\s+", " ", (s or "")).strip()
        if not s:
            return ""
        if "bookmarking your customized list" in s.lower():
            return ""
        if "share this page" in s.lower():
            return ""
        return s

    # Common label on EC pages: "Recommended action:"
    m = re.search(
        r"(Recommended action[s]?:)\s*(.+?)(?=\s+(What:|When:|Where:|Additional information:|$))",
        text,
        flags=re.IGNORECASE,
    )
    if not m:
        return ""

    action = _clean_action(m.group(2))
    action = action.strip("•-–— \t")
    return action


# =============================================================================
# Google Sheet + Google Drive integration
# =============================================================================
def _google_services() -> Tuple[Optional[Any], Optional[Any], str, str]:
    sheet_id = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
    sa_json = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    drive_folder_id = (os.getenv("GOOGLE_DRIVE_FOLDER_ID") or "").strip()

    if not sheet_id or not sa_json:
        return None, None, "", drive_folder_id

    info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    )

    sheets_svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    drive_svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    return sheets_svc, drive_svc, sheet_id, drive_folder_id


def load_care_statements_rows() -> List[dict]:
    sheets_svc, _, sheet_id, _ = _google_services()
    if not sheets_svc or not sheet_id:
        return []

    rng = "CareStatements!A:Z"
    resp = sheets_svc.spreadsheets().values().get(spreadsheetId=sheet_id, range=rng).execute()
    values = resp.get("values", [])
    if not values or len(values) < 2:
        return []

    headers = [h.strip().lower() for h in values[0]]
    rows: List[dict] = []
    for v in values[1:]:
        row = {}
        for i, h in enumerate(headers):
            row[h] = v[i].strip() if i < len(v) and isinstance(v[i], str) else (v[i] if i < len(v) else "")
        rows.append(row)
    return rows


# -----------------------------
# CareStatements selector
# Supports your sheet schema:
#   enabled | hazard | severity | platform | weight | variant text
# -----------------------------
def pick_care_statement(care_rows: List[dict], colour: str, alert_type: str, platform: str = "FB") -> str:
    """Return a single care statement text (or "")."""

    def _get_first(r: dict, keys: List[str]) -> str:
        for k in keys:
            if k in r and str(r.get(k, "")).strip() != "":
                return str(r.get(k, "")).strip()
        return ""

    def enabled(r: dict) -> bool:
        raw = _get_first(r, ["enabled", "active", "use"])
        if raw == "":
            return True
        return raw.lower() in ("true", "yes", "1", "y", "on")

    def norm_colour(c: str) -> str:
        c = (c or "").strip().lower()
        if c in ("🔴", "red"):
            return "🔴"
        if c in ("🟠", "orange"):
            return "🟠"
        if c in ("🟡", "yellow"):
            return "🟡"
        if c in ("⚪", "white"):
            return "⚪"
        if c in ("🟢", "green"):
            return "🟢"
        return ""

    def norm_platform(p: str) -> str:
        p = (p or "").strip().lower()
        if not p or p in ("any", "all", "*"):
            return ""
        if p in ("fb", "facebook", "meta"):
            return "fb"
        if p in ("x", "twitter"):
            return "x"
        return p

    want_type = (alert_type or "").strip().lower()
    want_colour = norm_colour((colour or "").strip())
    want_platform = norm_platform(platform)

    def row_colour(r: dict) -> str:
        return norm_colour(_get_first(r, ["colour", "color", "severity", "emoji"]))

    def row_type(r: dict) -> str:
        # Your sheet uses "hazard" (often "any") to mean bucket/type matching
        return _get_first(r, ["type", "alert_type", "kind", "bucket", "hazard"]).strip().lower()

    def row_platform(r: dict) -> str:
        return norm_platform(_get_first(r, ["platform", "channel"]))

    def row_text(r: dict) -> str:
        # Your sheet uses "variant text"
        return _get_first(
            r,
            [
                "variant text",
                "variant_text",
                "text",
                "care",
                "care_text",
                "carestatement",
                "care_statement",
                "statement",
                "message",
                "variant",
            ],
        )

    def matches(r: dict, c_req: str, t_req: str, p_req: str) -> bool:
        rc = row_colour(r)
        rt = row_type(r)
        rp = row_platform(r)

        # colour filter
        if c_req and rc != c_req:
            return False

        # type/hazard filter:
        # treat blank/"any" in row as wildcard
        if t_req:
            if rt and rt not in ("any", "*") and rt != t_req:
                return False

        # platform filter:
        # treat blank/"any" in row as wildcard
        if p_req:
            if rp and rp != p_req:
                return False

        return True

    # Priority buckets (most specific -> least specific)
    buckets = [
        (want_colour, want_type, want_platform),
        ("", want_type, want_platform),
        (want_colour, "", want_platform),
        ("", "", want_platform),
        (want_colour, want_type, ""),  # allow platform-wildcard rows
        ("", want_type, ""),
        (want_colour, "", ""),
        ("", "", ""),
    ]

    for bc, bt, bp in buckets:
        for r in (care_rows or []):
            if not enabled(r):
                continue
            if matches(r, bc, bt, bp):
                txt = row_text(r).strip()
                if txt:
                    return txt
    return ""


def list_matching_care_texts(care_rows: List[dict], colour: str, alert_type: str, platform: str = "FB") -> List[str]:
    """Return all unique matching care texts (used for Remix)."""

    def _get_first(r: dict, keys: List[str]) -> str:
        for k in keys:
            if k in r and str(r.get(k, "")).strip() != "":
                return str(r.get(k, "")).strip()
        return ""

    def enabled(r: dict) -> bool:
        raw = _get_first(r, ["enabled", "active", "use"])
        if raw == "":
            return True
        return raw.lower() in ("true", "yes", "1", "y", "on")

    def norm_colour(c: str) -> str:
        c = (c or "").strip().lower()
        if c in ("🔴", "red"):
            return "🔴"
        if c in ("🟠", "orange"):
            return "🟠"
        if c in ("🟡", "yellow"):
            return "🟡"
        if c in ("⚪", "white"):
            return "⚪"
        if c in ("🟢", "green"):
            return "🟢"
        return ""

    def norm_platform(p: str) -> str:
        p = (p or "").strip().lower()
        if not p or p in ("any", "all", "*"):
            return ""
        if p in ("fb", "facebook", "meta"):
            return "fb"
        if p in ("x", "twitter"):
            return "x"
        return p

    want_type = (alert_type or "").strip().lower()
    want_colour = norm_colour((colour or "").strip())
    want_platform = norm_platform(platform)

    def row_colour(r: dict) -> str:
        return norm_colour(_get_first(r, ["colour", "color", "severity", "emoji"]))

    def row_type(r: dict) -> str:
        return _get_first(r, ["type", "alert_type", "kind", "bucket", "hazard"]).strip().lower()

    def row_platform(r: dict) -> str:
        return norm_platform(_get_first(r, ["platform", "channel"]))

    def row_text(r: dict) -> str:
        return _get_first(
            r,
            [
                "variant text",
                "variant_text",
                "text",
                "care",
                "care_text",
                "carestatement",
                "care_statement",
                "statement",
                "message",
                "variant",
            ],
        )

    def matches(r: dict, c_req: str, t_req: str, p_req: str) -> bool:
        rc = row_colour(r)
        rt = row_type(r)
        rp = row_platform(r)

        if c_req and rc != c_req:
            return False

        if t_req:
            if rt and rt not in ("any", "*") and rt != t_req:
                return False

        if p_req:
            if rp and rp != p_req:
                return False

        return True

    buckets = [
        (want_colour, want_type, want_platform),
        ("", want_type, want_platform),
        (want_colour, "", want_platform),
        ("", "", want_platform),
        (want_colour, want_type, ""),
        ("", want_type, ""),
        (want_colour, "", ""),
        ("", "", ""),
    ]

    out: List[str] = []
    seen = set()
    for bc, bt, bp in buckets:
        for r in (care_rows or []):
            if not enabled(r):
                continue
            if not matches(r, bc, bt, bp):
                continue
            txt = row_text(r).strip()
            if txt and txt not in seen:
                out.append(txt)
                seen.add(txt)
    return out


def pick_remixed_care_text(
    care_rows: List[dict],
    colour: str,
    alert_type: str,
    current_care_text: str,
    remix_count: int,
    platform: str = "FB",
) -> str:
    candidates = list_matching_care_texts(care_rows, colour, alert_type, platform=platform)
    cur = (current_care_text or "").strip()

    if cur:
        filtered = [c for c in candidates if c.strip() != cur]
        if filtered:
            candidates = filtered

    if not candidates:
        return cur

    idx = abs(int(remix_count)) % len(candidates)
    return candidates[idx].strip()


# =============================================================================
# Google Drive curated photos (FIRST choice for images)
# =============================================================================
def pick_drive_images(
    drive_svc: Any,
    folder_id: str,
    alert_type_label: str,
    severity: str,
    max_images: int = 2,
) -> List[str]:
    if not drive_svc or not folder_id:
        return []

    words = [w for w in re.split(r"[^a-z0-9]+", (alert_type_label or "").lower()) if w]
    words = [w for w in words if len(w) >= 3]

    colour_words = {
        "🔴": ["red", "warning"],
        "🟠": ["orange", "watch"],
        "🟡": ["yellow", "advisory"],
        "⚪": ["white", "statement", "other"],
    }.get(severity, [])

    q = f"'{folder_id}' in parents and trashed = false and (mimeType contains 'image/')"
    resp = drive_svc.files().list(
        q=q,
        pageSize=50,
        fields="files(id,name,mimeType,modifiedTime)",
        orderBy="modifiedTime desc",
    ).execute()

    files = resp.get("files", []) or []

    def score(name: str) -> int:
        n = (name or "").lower()
        sc = 0
        for w in words:
            if w in n:
                sc += 3
        for w in colour_words:
            if w in n:
                sc += 2
        return sc

    scored = sorted(
        files,
        key=lambda f: (score(f.get("name", "")), f.get("modifiedTime", "")),
        reverse=True,
    )

    picked: List[str] = []
    for f in scored:
        if len(picked) >= max_images:
            break
        fid = (f.get("id") or "").strip()
        if not fid:
            continue
        picked.append(f"drive://{fid}")

    return picked[:max_images]


def download_drive_image_bytes(drive_svc: Any, drive_ref: str) -> Tuple[bytes, str]:
    m = re.match(r"^drive://(.+)$", (drive_ref or "").strip())
    if not m:
        raise RuntimeError("Invalid drive ref")

    file_id = m.group(1).strip()
    if not file_id:
        raise RuntimeError("Empty drive file id")

    meta = drive_svc.files().get(fileId=file_id, fields="mimeType,name").execute()
    mime = (meta.get("mimeType") or "application/octet-stream").lower()
    data = drive_svc.files().get_media(fileId=file_id).execute()
    if not isinstance(data, (bytes, bytearray)):
        data = bytes(data)

    if mime == "image/jpg":
        mime = "image/jpeg"

    return bytes(data), mime


# =============================================================================
# Ontario 511 camera resolver (SECOND choice for images)
# =============================================================================
_ON511_CAMERAS_CACHE: Optional[List[Dict[str, Any]]] = None


def is_image_url(url: str) -> bool:
    url = (url or "").strip()
    if not url:
        return False

    try:
        r = requests.head(url, allow_redirects=True, headers={"User-Agent": USER_AGENT}, timeout=(5, 15))
        ct = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if r.status_code < 400 and ct.startswith("image/"):
            return True
    except Exception:
        pass

    try:
        r = requests.get(url, allow_redirects=True, headers={"User-Agent": USER_AGENT}, timeout=(5, 20))
        ct = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        return r.status_code < 400 and ct.startswith("image/")
    except Exception:
        return False


def fetch_on511_cameras() -> List[Dict[str, Any]]:
    global _ON511_CAMERAS_CACHE
    if _ON511_CAMERAS_CACHE is not None:
        return _ON511_CAMERAS_CACHE

    r = requests.get(ON511_CAMERAS_API, headers={"User-Agent": USER_AGENT}, timeout=(10, 30))
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError("Unexpected 511 cameras payload (expected list).")

    _ON511_CAMERAS_CACHE = data
    return data


def resolve_on511_views_by_keyword(keyword: str) -> List[Dict[str, Any]]:
    kw = normalize(keyword)
    cams = fetch_on511_cameras()
    out: List[Dict[str, Any]] = []

    for cam in cams:
        name = normalize(str(cam.get("Name") or ""))
        desc = normalize(str(cam.get("Description") or ""))
        if kw and (kw in name or kw in desc):
            views = cam.get("Views") or []
            if isinstance(views, list):
                for v in views:
                    if isinstance(v, dict):
                        out.append(v)
    return out


def pick_north_south_view_urls(views: List[Dict[str, Any]]) -> Tuple[str, str]:
    north = ""
    south = ""

    def normalize_url(u: str) -> str:
        u = (u or "").strip()
        if not u:
            return ""
        if not u.lower().startswith("http"):
            u = "https://511on.ca" + (u if u.startswith("/") else "/" + u)
        return u

    for v in views:
        d = normalize(str(v.get("Description") or ""))
        u = normalize_url(v.get("Url") or "")
        if not u:
            continue
        if ("north" in d or "nb" in d) and not north:
            north = u
        if ("south" in d or "sb" in d) and not south:
            south = u

    if not north or not south:
        urls: List[str] = []
        for v in views:
            u = normalize_url(v.get("Url") or "")
            if u:
                urls.append(u)
        if not north and len(urls) >= 1:
            north = urls[0]
        if not south and len(urls) >= 2:
            south = urls[1]

    return north, south


def resolve_cr29_image_urls() -> List[str]:
    north_env = (os.getenv("CR29_NORTH_IMAGE_URL") or "").strip()
    south_env = (os.getenv("CR29_SOUTH_IMAGE_URL") or "").strip()

    urls: List[str] = []

    for u in [north_env, south_env]:
        if u and is_image_url(u) and u not in urls:
            urls.append(u)

    if len(urls) >= 2:
        return urls[:2]

    try:
        views = resolve_on511_views_by_keyword(ON511_CAMERA_KEYWORD)
        north_api, south_api = pick_north_south_view_urls(views)
        for u in [north_api, south_api]:
            if u and is_image_url(u) and u not in urls:
                urls.append(u)
    except Exception as e:
        print(f"⚠️ 511 camera API resolver skipped: {e}")

    return urls[:2]


# =============================================================================
# On511 camera "bug" overlay for Ontario 511 images (stamp lower-right)
# =============================================================================
def apply_on511_bug(image_bytes: bytes, mime_type: str) -> Tuple[bytes, str]:
    BUG_RELATIVE_WIDTH = 0.07
    BUG_MIN_WIDTH_PX = 35
    BUG_OPACITY_ALPHA = 80
    BUG_PAD_RELATIVE = 0.015

    try:
        im = Image.open(BytesIO(image_bytes)).convert("RGBA")
        asset_path = Path(__file__).resolve().parent / "assets" / "On511_logo.png"
        logo = Image.open(asset_path).convert("RGBA")

        target_w = max(BUG_MIN_WIDTH_PX, int(im.width * BUG_RELATIVE_WIDTH))
        scale = target_w / float(logo.width)
        target_h = max(1, int(logo.height * scale))
        logo = logo.resize((target_w, target_h), resample=Image.LANCZOS)

        if BUG_OPACITY_ALPHA < 255:
            alpha = logo.getchannel("A")
            a_scale = BUG_OPACITY_ALPHA / 255.0
            alpha = alpha.point(lambda p: int(p * a_scale))
            logo.putalpha(alpha)

        pad = max(8, int(im.width * BUG_PAD_RELATIVE))
        x = max(0, im.width - logo.width - pad)
        y = max(0, im.height - logo.height - pad)
        im.alpha_composite(logo, (x, y))

        out = BytesIO()
        im.save(out, format="PNG", optimize=True)
        return out.getvalue(), "image/png"

    except Exception as e:
        print("⚠️ On511 bug overlay failed; using original image:", e)
        return image_bytes, mime_type


def download_image_bytes(image_url: str) -> Tuple[bytes, str]:
    image_url = (image_url or "").strip()
    if not image_url:
        raise RuntimeError("No image_url provided")

    r = requests.get(image_url, headers={"User-Agent": USER_AGENT}, timeout=(10, 30), allow_redirects=True)
    r.raise_for_status()

    content_type = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if not content_type.startswith("image/"):
        raise RuntimeError(f"URL did not return an image. Content-Type={content_type}")

    data = r.content
    u = image_url.lower()
    if "511on.ca" in u and "/cctv/" in u:
        data, content_type = apply_on511_bug(data, content_type)

    return data, content_type


# =============================================================================
# Facebook image loader wiring
# =============================================================================
_DRIVE_SVC_FOR_MEDIA: Optional[Any] = None


def fb_load_image_bytes(ref: str) -> Tuple[bytes, str]:
    ref = (ref or "").strip()
    if not ref:
        raise RuntimeError("No image ref provided")

    if os.path.exists(ref):
        with open(ref, "rb") as f:
            data = f.read()
        ext = os.path.splitext(ref)[1].lower()
        if ext == ".png":
            return data, "image/png"
        if ext in (".jpg", ".jpeg"):
            return data, "image/jpeg"
        return data, "application/octet-stream"

    if ref.startswith("drive://"):
        if not _DRIVE_SVC_FOR_MEDIA:
            raise RuntimeError("Drive ref provided but Drive service is not configured")
        return download_drive_image_bytes(_DRIVE_SVC_FOR_MEDIA, ref)

    return download_image_bytes(ref)


fb.load_image_bytes = fb_load_image_bytes


def materialize_images_for_facebook(image_refs: List[str]) -> List[str]:
    out_paths: List[str] = []
    for i, ref in enumerate([x for x in (image_refs or []) if (x or "").strip()][:10]):
        b, mt = fb_load_image_bytes(ref)
        ext = "png" if mt == "image/png" else "jpg"
        p = f"/tmp/tay_media_{i}.{ext}"
        with open(p, "wb") as f:
            f.write(b)
        out_paths.append(p)
    return out_paths


def cleanup_tmp_media_files(paths: List[str]) -> None:
    for p in paths or []:
        try:
            if isinstance(p, str) and p.startswith("/tmp/tay_media_"):
                os.remove(p)
        except Exception:
            pass


# =============================================================================
# X OAuth2 (posting) + OAuth1 (media upload)
# =============================================================================
def write_rotated_refresh_token(new_refresh: str) -> None:
    new_refresh = (new_refresh or "").strip()
    if not new_refresh:
        return
    with open(ROTATED_X_REFRESH_TOKEN_PATH, "w", encoding="utf-8") as f:
        f.write(new_refresh)


def get_oauth2_access_token() -> str:
    client_id = os.getenv("X_CLIENT_ID", "").strip()
    client_secret = os.getenv("X_CLIENT_SECRET", "").strip()
    refresh_token = os.getenv("X_REFRESH_TOKEN", "").strip()

    missing = [k for k, v in [
        ("X_CLIENT_ID", client_id),
        ("X_CLIENT_SECRET", client_secret),
        ("X_REFRESH_TOKEN", refresh_token),
    ] if not v]
    if missing:
        raise RuntimeError(f"Missing required X env vars: {', '.join(missing)}")

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    headers = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": USER_AGENT,
    }

    r = requests.post(
        "https://api.x.com/2/oauth2/token",
        headers=headers,
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        timeout=30,
    )
    print("X token refresh status:", r.status_code)
    r.raise_for_status()

    payload = r.json()
    access = payload.get("access_token")
    if not access:
        raise RuntimeError("No access_token returned during refresh.")

    new_refresh = payload.get("refresh_token")
    if new_refresh and new_refresh != refresh_token:
        print("⚠️ X refresh token rotated. Workflow will update the repo secret.")
        write_rotated_refresh_token(new_refresh)

    return access


def x_upload_media(image_ref: str) -> str:
    api_key = os.getenv("X_API_KEY", "").strip()
    api_secret = os.getenv("X_API_SECRET", "").strip()
    access_token = os.getenv("X_ACCESS_TOKEN", "").strip()
    access_secret = os.getenv("X_ACCESS_TOKEN_SECRET", "").strip()

    missing = [k for k, v in [
        ("X_API_KEY", api_key),
        ("X_API_SECRET", api_secret),
        ("X_ACCESS_TOKEN", access_token),
        ("X_ACCESS_TOKEN_SECRET", access_secret),
    ] if not v]
    if missing:
        raise RuntimeError(f"Missing required X OAuth1 env vars: {', '.join(missing)}")

    if image_ref.startswith("drive://"):
        if not _DRIVE_SVC_FOR_MEDIA:
            raise RuntimeError("Drive ref provided but Drive service is not configured")
        img_bytes, mime_type = download_drive_image_bytes(_DRIVE_SVC_FOR_MEDIA, image_ref)
    else:
        img_bytes, mime_type = download_image_bytes(image_ref)

    auth = OAuth1(api_key, api_secret, access_token, access_secret)
    upload_url = "https://upload.twitter.com/1.1/media/upload.json"
    files = {"media": ("image", img_bytes, mime_type)}
    r = requests.post(upload_url, auth=auth, files=files, timeout=60)

    print("X media upload status:", r.status_code)
    if r.status_code >= 400:
        raise RuntimeError(f"X media upload failed {r.status_code}")

    j = r.json()
    media_id = j.get("media_id_string") or (str(j.get("media_id")) if j.get("media_id") else "")
    if not media_id:
        raise RuntimeError("X media upload succeeded but no media_id returned")

    return media_id


def post_to_x(text: str, image_refs: Optional[List[str]] = None) -> Dict[str, Any]:
    url = "https://api.x.com/2/tweets"
    access_token = get_oauth2_access_token()

    payload: Dict[str, Any] = {"text": text}

    image_refs = [u for u in (image_refs or []) if (u or "").strip()]
    if image_refs:
        media_ids: List[str] = []
        for u in image_refs[:4]:
            try:
                media_ids.append(x_upload_media(u))
            except Exception as e:
                print(f"⚠️ X media skipped for one image: {e}")
        if media_ids:
            payload["media"] = {"media_ids": media_ids}

    r = requests.post(
        url,
        json=payload,
        headers={
            "Authorization": f"Bearer {access_token}",
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
        },
        timeout=20,
    )

    print("X POST /2/tweets status:", r.status_code)

    if r.status_code >= 400:
        detail = ""
        try:
            j = r.json()
            detail = (j.get("detail") or "").lower()
        except Exception:
            pass

        if r.status_code == 403 and "duplicate" in detail:
            raise RuntimeError("X_DUPLICATE_TWEET")

        raise RuntimeError(f"X post failed {r.status_code}")

    return r.json()


def safe_post_to_x(text: str, image_refs: Optional[List[str]] = None) -> Dict[str, Any]:
    if _no_post_guard("X"):
        return {"dry_run": True, "blocked_by_run_mode": RUN_MODE}
    return post_to_x(text, image_refs=image_refs)


def safe_post_to_facebook(state: Dict[str, Any], caption: str, image_urls: List[str]) -> Dict[str, Any]:
    if _no_post_guard("Facebook"):
        return {"dry_run": True, "blocked_by_run_mode": RUN_MODE}
    return fb.safe_post_facebook(
        state,
        caption=caption,
        image_urls=image_urls,
        has_new_social_event=True,
        state_path=STATE_PATH,
    )


# =============================================================================
# RSS building
# =============================================================================
MAX_RSS_ITEMS = 25


def ensure_rss_exists() -> None:
    if os.path.exists(RSS_PATH):
        return

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = "Tay Township Weather Statements"
    ET.SubElement(channel, "link").text = "https://commsconnect.github.io/tay-weather-rss/"
    ET.SubElement(channel, "description").text = "Automated weather statements and alerts for Tay Township area."
    ET.SubElement(channel, "language").text = "en-ca"

    ET.ElementTree(rss).write(RSS_PATH, encoding="utf-8", xml_declaration=True)


def load_rss_tree() -> Tuple[ET.ElementTree, ET.Element]:
    ensure_rss_exists()
    tree = ET.parse(RSS_PATH)
    root = tree.getroot()
    channel = root.find("channel")
    if channel is None:
        raise RuntimeError("RSS file missing <channel>")
    return tree, channel


def rss_item_exists(channel: ET.Element, guid_text: str) -> bool:
    for item in channel.findall("item"):
        guid = item.find("guid")
        if guid is not None and (guid.text or "").strip() == guid_text:
            return True
    return False


def add_rss_item(channel: ET.Element, title: str, link: str, guid: str, pub_date: str, description: str) -> None:
    item = ET.Element("item")
    ET.SubElement(item, "title").text = title
    ET.SubElement(item, "link").text = link
    g = ET.SubElement(item, "guid")
    g.text = guid
    g.set("isPermaLink", "false")
    ET.SubElement(item, "pubDate").text = pub_date
    ET.SubElement(item, "description").text = description

    insert_index = 0
    for i, child in enumerate(list(channel)):
        if child.tag in {"title", "link", "description", "language", "lastBuildDate"}:
            insert_index = i + 1
    channel.insert(insert_index, item)


def trim_rss_items(channel: ET.Element, max_items: int) -> None:
    items = channel.findall("item")
    if len(items) <= max_items:
        return
    for item in items[max_items:]:
        channel.remove(item)


def build_rss_description_from_atom(entry: Dict[str, Any], more_url: str) -> str:
    title = atom_title_for_tay((entry.get("title") or "").strip())
    issued = (entry.get("summary") or "").strip()
    official = (entry.get("link") or "").strip()

    bits = [title]
    if issued:
        bits.append(issued)
    bits.append(f"More info (Tay Township): {more_url}")
    if official:
        bits.append(f"Official alert details: {official}")
    return "\n".join(bits)


# =============================================================================
# Social text formatting
# =============================================================================

# Remove the specific EC area parenthetical from anywhere in a string
_TAY_AREA_PAREN_RE = re.compile(r"\s*\(\s*Tay Township area[^)]*\)\s*", flags=re.IGNORECASE)

def strip_tay_area_paren(s: str) -> str:
    s = (s or "")
    s = _TAY_AREA_PAREN_RE.sub(" ", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s.strip()

def _pretty_title_for_social(title: str) -> str:
    """
    Converts EC banner title like:
      "Yellow Warning - Snowfall"
    into:
      "Snowfall warning in Tay Township"
    """
    t = (title or "").strip()

    m = re.match(r"^(yellow|orange|red)\s+(warning|watch|advisory)\s*-\s*(.+)$", t, flags=re.IGNORECASE)
    if m:
        alert_type = m.group(2).lower().strip()
        hazard = m.group(3).strip()
        hazard = hazard[:1].upper() + hazard[1:] if hazard else hazard
        return f"{hazard} {alert_type} in Tay Township"

    t = re.sub(r"\s*\(.*?\)\s*$", "", t).strip()
    return f"{t} in Tay Township"


def build_x_post_text(entry: Dict[str, Any], more_url: str, care: str = "", custom_x: str = "") -> str:
    """
    X post builder with character limit handling.
    NOTE: In this bot, X should be clean/short and should NOT include FB care statements.
          We keep the 'care' param for compatibility, but main() passes care="".
    """
    title_raw = strip_tay_area_paren(atom_title_for_tay((entry.get("title") or "").strip()))
    official = (entry.get("link") or "").strip()
    sev = "🟢" if is_alert_ended(title_raw, entry.get("summary") or "") else severity_emoji(title_raw)

    title_line = f"{sev} {_pretty_title_for_social(title_raw)}" if sev else _pretty_title_for_social(title_raw)

    details_lines: List[str] = []
    try:
        details_lines = _extract_details_lines_from_ec(official)
    except Exception as e:
        print(f"⚠️ EC details parse failed (X): {e}")

    def try_append_custom(base_text: str) -> str:
        if custom_x.strip():
            cand = base_text + "\n\n" + custom_x.strip()
            if len(cand) <= 280:
                return cand
        return base_text

    # --- VERSION 1: MAX (2 details lines) ---
    parts_max = [title_line, "", *details_lines[:2], "", "Environment Canada", f"More: {more_url}", "#TayTownship #ONStorm"]
    text_max = "\n".join(parts_max).strip()
    if len(text_max) <= 280:
        return try_append_custom(text_max)

    # --- VERSION 2: MEDIUM (1 details line) ---
    if details_lines:
        parts_med = [title_line, "", details_lines[0], "", "Environment Canada", f"More: {more_url}", "#TayTownship #ONStorm"]
        text_med = "\n".join(parts_med).strip()
        if len(text_med) <= 280:
            return try_append_custom(text_med)

    # --- VERSION 3: MINIMAL (No details) ---
    parts_min = [title_line, "", "Environment Canada", f"More: {more_url}", "#TayTownship #ONStorm"]
    text_min = "\n".join(parts_min).strip()
    if len(text_min) <= 280:
        return try_append_custom(text_min)

    return text_min[:277].rstrip() + "..."


def build_facebook_post_text(entry: Dict[str, Any], care: str, more_url: str, custom_fb: str = "") -> str:
    """
    Facebook SHOULD include:
      - Details lines (What/When)
      - Recommended action (when present on EC page)
      - OR Care statement (from Google Sheets) IF no recommended action is found
    """
    title_raw = strip_tay_area_paren(atom_title_for_tay((entry.get("title") or "").strip()))
    official = (entry.get("link") or "").strip()

    sev = "🟢" if is_alert_ended(title_raw, entry.get("summary") or "") else severity_emoji(title_raw)

    title_line = f"{sev} {_pretty_title_for_social(title_raw)}" if sev else _pretty_title_for_social(title_raw)

    details_lines: List[str] = []
    try:
        details_lines = _extract_details_lines_from_ec(official)
    except Exception as e:
        print(f"⚠️ EC details parse failed (FB): {e}")

    recommended_action = ""
    try:
        recommended_action = _extract_recommended_action_from_ec(official)
    except Exception as e:
        print(f"⚠️ EC recommended action parse failed (FB): {e}")

    parts: List[str] = []
    parts.append(title_line)
    parts.append("")
    parts.extend(details_lines[:3])

    if recommended_action:
        parts.append("")
        parts.append("Recommended action:")
        ra = recommended_action.strip().rstrip(".")
        parts.append(f"• {ra}.")
    elif care:
        parts.append("")
        parts.append(care.strip())

    if custom_fb.strip():
        parts.append("")
        parts.append(custom_fb.strip())

    parts.append("")
    parts.append("Environment Canada")
    parts.append(f"More: {more_url}")
    parts.append("#TayTownship #ONStorm")

    print(
        f"FB build: action={'yes' if bool(recommended_action) else 'no'}, "
        f"care_applied={'yes' if (not recommended_action and care) else 'no'}"
    )

    return "\n".join(parts).strip()


# =============================================================================
# Image selection policy
# =============================================================================
def choose_images_for_alert(
    drive_svc: Optional[Any],
    drive_folder_id: str,
    alert_kind: str,
    alert_type_label: str,
    severity: str,
) -> List[str]:
    drive_refs: List[str] = []
    if drive_svc and drive_folder_id:
        try:
            drive_refs = pick_drive_images(
                drive_svc=drive_svc,
                folder_id=drive_folder_id,
                alert_type_label=alert_type_label,
                severity=severity,
                max_images=2,
            )
        except Exception as e:
            print(f"⚠️ Drive image selection failed: {e}")
            drive_refs = []

    if drive_refs:
        return drive_refs[:2]

    # Policy: If WIND WARNING, do not use highway camera images
    if alert_kind == "warning" and "wind" in (alert_type_label or "").lower():
        return []

    try:
        return resolve_cr29_image_urls()
    except Exception as e:
        print(f"⚠️ Camera fallback failed: {e}")
        return []


# =============================================================================
# Telegram decision helper (robust)
# =============================================================================
def wait_for_decision_safe(state: Dict[str, Any], token: str) -> str:
    """
    For non-warning alerts:
      - wait up to TELEGRAM_WAIT_SECONDS for explicit Approve/Deny
      - timeout => denied (safe default)

    Implementation details:
      - state_path ensures we see decisions saved to disk.
      - ingest_each_poll ensures we process Telegram button clicks while waiting.
    """
    wait_seconds = int(os.getenv("TELEGRAM_WAIT_SECONDS", "600"))
    ttl_min = int(os.getenv("TELEGRAM_APPROVAL_TTL_MIN", "60"))

    try:
        d = wait_for_decision(
            st=state,
            token=token,
            save_state_fn=save_state,
            ttl_min=ttl_min,
            poll_interval_seconds=4,
            max_wait_seconds=wait_seconds,
            state_path=STATE_PATH,
            ingest_each_poll=True,
        )
    except Exception as e:
        print(f"⚠️ wait_for_decision failed ({e}); treating as denied.")
        d = "denied"

    return d if d in ("approved", "denied") else "denied"


# =============================================================================
# Main routine
# =============================================================================
def main() -> None:
    more_url = resolve_more_info_url()

    # cleanup rotated token file
    if os.path.exists(ROTATED_X_REFRESH_TOKEN_PATH):
        try:
            os.remove(ROTATED_X_REFRESH_TOKEN_PATH)
        except Exception:
            pass

    sheets_svc, drive_svc, sheet_id, drive_folder_id = _google_services()

    global _DRIVE_SVC_FOR_MEDIA
    _DRIVE_SVC_FOR_MEDIA = drive_svc

    # -------------------------------------------------------------------------
    # RUN MODE: TEST (Telegram-first, NO POST)
    # -------------------------------------------------------------------------
    if RUN_MODE in ("test_telegram_buttons_no_post", "test_sample_alert_no_post"):
        if not TELEGRAM_ENABLE_GATE:
            print("RUN_MODE is a Telegram test mode, but TELEGRAM_ENABLE_GATE=false. Exiting cleanly.")
            return

        test_images = resolve_cr29_image_urls()
        st = load_state()

        ingest_telegram_actions(st, save_state)
        maybe_send_reminders(st, save_state)

        token = (st.get("test_gate_token") or "").strip()
        created_at = None
        if token:
            created_at = (st.get("pending_approvals") or {}).get(token, {}).get("created_at")

        if (
            (not token)
            or (created_at and is_expired(st, token))
            or (token and decision_for(st, token) in ("approved", "denied"))
        ):
            token = hashlib.sha1(f"test:{RUN_MODE}:{dt.datetime.utcnow().isoformat()}".encode("utf-8")).hexdigest()[:10]
            st["test_gate_token"] = token
            save_state(st)

        if RUN_MODE == "test_telegram_buttons_no_post":
            preview_text = (
                "🧪 TEST MODE: Telegram buttons (NO POST)\n\n"
                "Tap: ✅ Approve / 🛑 Deny / 🔁 Remix / ✏️ Custom\n\n"
                f"RUN_MODE: {RUN_MODE}\n"
                f"TOKEN: {token}"
            )
            kind_for_buttons = "other"
        else:
            fake_entry = {
                "id": f"test-sample-{token}",
                "title": "Yellow Advisory - Snowfall",
                "link": TAY_COORDS_URL,
                "summary": "Test sample alert (no post).",
                "updated_dt": dt.datetime.now(dt.timezone.utc),
            }
            x_text = build_x_post_text(fake_entry, more_url=more_url, care="")
            fb_text = build_facebook_post_text(fake_entry, care="", more_url=more_url)
            preview_text = (
                f"🧪 TEST MODE: Sample alert (NO POST)\n\n"
                f"----- X (NO POST) -----\n{x_text}\n\n"
                f"----- Facebook (NO POST) -----\n{fb_text}\n\n"
                f"RUN_MODE: {RUN_MODE}\n"
                f"More:\n{more_url}\n\n"
                f"TOKEN: {token}"
            )
            kind_for_buttons = classify_alert_kind(fake_entry["title"])

        ensure_preview_sent(
            st,
            save_state,
            token,
            preview_text,
            kind=kind_for_buttons,
            image_urls=test_images,
        )

        ingest_telegram_actions(st, save_state)
        d = decision_for(st, token)
        if d not in ("approved", "denied"):
            d = wait_for_decision_safe(st, token)

        if d == "approved":
            try:
                tg_send_message("✅ Test approved — confirmed: NO POST mode blocked all platform posting.")
            except Exception:
                pass
        elif d == "denied":
            try:
                tg_send_message("🛑 Test denied — confirmed: nothing was posted.")
            except Exception:
                pass
        else:
            try:
                tg_send_message("⏳ Test still pending — nothing posted this run.")
            except Exception:
                pass

        tg_send_test_success("Telegram preview + buttons path completed (NO POST mode).")
        return

    # -------------------------------------------------------------------------
    # LIVE MODE: Optional platform test (existing behaviour preserved)
    # -------------------------------------------------------------------------
    if (TEST_X or TEST_FACEBOOK) and (not NO_POST_MODE):
        base = "Testing the validity of the post — please ignore ✅"
        test_images = resolve_cr29_image_urls()

        if TELEGRAM_ENABLE_GATE:
            st = load_state()
            ingest_telegram_actions(st, save_state)
            maybe_send_reminders(st, save_state)

            token = (st.get("test_gate_token") or "").strip()
            created_at = None
            if token:
                created_at = (st.get("pending_approvals") or {}).get(token, {}).get("created_at")

            if (
                (not token)
                or (created_at and is_expired(st, token))
                or (token and decision_for(st, token) in ("approved", "denied"))
            ):
                token = hashlib.sha1(f"test:{dt.datetime.utcnow().isoformat()}".encode("utf-8")).hexdigest()[:10]
                st["test_gate_token"] = token
                save_state(st)

            platforms = []
            if ENABLE_X_POSTING and TEST_X:
                platforms.append("X")
            if ENABLE_FB_POSTING and TEST_FACEBOOK:
                platforms.append("Facebook")
            platforms_str = " + ".join(platforms) if platforms else "None"

            preview_text = (
                f"{base}\n\n"
                f"Platforms (if approved): {platforms_str}\n\n"
                "Tap: ✅ Approve / 🛑 Deny / 🔁 Remix / ✏️ Custom\n\n"
                f"TOKEN: {token}"
            )

            ensure_preview_sent(st, save_state, token, preview_text, kind="other", image_urls=test_images)

            ingest_telegram_actions(st, save_state)
            d = decision_for(st, token)
            if d not in ("approved", "denied"):
                d = wait_for_decision_safe(st, token)

            if d == "denied":
                print("Telegram: denied (test). Not posting.")
                try:
                    tg_send_message("🛑 Test denied — confirmed: nothing was posted. Workflow exiting cleanly.")
                except Exception:
                    pass
                return

            if d != "approved":
                print("Telegram: still pending (test). Not posting this run.")
                try:
                    tg_send_message("⏳ Test still pending approval — nothing posted this run. Workflow exiting cleanly.")
                except Exception:
                    pass
                return

        if EFFECTIVE_ENABLE_X_POSTING and TEST_X:
            safe_post_to_x(f"{base}\n\n(X)", image_refs=test_images)

        if EFFECTIVE_ENABLE_FB_POSTING and TEST_FACEBOOK:
            st2 = load_state()
            st2.pop("fb_cooldown_until", None)
            st2.pop("fb_last_posted_at", None)

            fb_local = materialize_images_for_facebook(test_images)
            try:
                fb_result = safe_post_to_facebook(st2, caption=f"{base}\n\n(Facebook)", image_urls=fb_local)
                print("FB result:", fb_result)
            finally:
                cleanup_tmp_media_files(fb_local)

        tg_send_test_success("Remix + Custom buttons are ready — try them on the preview above.")
        return

    # -------------------------------------------------------------------------
    # NORMAL MODE (real alerts)
    # -------------------------------------------------------------------------
    state = load_state()

    # Load CareStatements once per run
    care_rows: List[dict] = []
    try:
        care_rows = load_care_statements_rows()
        print(f"CareStatements: loaded {len(care_rows)} rows")
        if care_rows:
            print("CareStatements: sample keys:", sorted(care_rows[0].keys()))
    except Exception as e:
        print(f"⚠️ CareStatements failed to load (will post without care text): {e}")
        care_rows = []

    posted = set(state.get("posted_guids", []))
    posted_text_hashes = set(state.get("posted_text_hashes", []))

    tree, channel = load_rss_tree()

    try:
        feed_entries = fetch_feed_entries(ALERT_FEED_URL)
    except Exception as e:
        print(f"⚠️ Feed unavailable: {e}")
        print("Exiting cleanly; will retry on next scheduled run.")
        return

    def entry_guid(entry: dict) -> str:
        return (entry.get("id") or entry.get("link") or "").strip()

    for entry in feed_entries:
        guid = entry_guid(entry)
        if not guid:
            continue

        title = normalize_alert_title(atom_title_for_tay((entry.get("title") or "Weather alert").strip()))
        title_l = (title or "").lower()
        summary_l = ((entry.get("summary") or "")).lower()

        ended = is_alert_ended(title, entry.get("summary") or "")

        general_non_alert_markers = (
            "no alerts in effect",
            "no watches or warnings in effect",
        )

        if any(m in title_l for m in general_non_alert_markers) or any(m in summary_l for m in general_non_alert_markers):
            print(f"Info: general bulletin — skipping social post: {title}")
            posted.add(guid)
            continue

        pub_dt = entry.get("updated_dt") or dt.datetime.now(dt.timezone.utc)
        pub_date = email.utils.format_datetime(pub_dt)
        link = more_url
        description = build_rss_description_from_atom(entry, more_url=more_url)

        if not rss_item_exists(channel, guid):
            add_rss_item(channel, title=title, link=link, guid=guid, pub_date=pub_date, description=description)

        if guid in posted:
            continue

        alert_kind = classify_alert_kind(title)

        allowed, reason = cooldown_allows_post(state, DISPLAY_AREA_NAME, kind=alert_kind)
        if not allowed:
            print("Social skipped:", reason)
            continue

        title_raw = atom_title_for_tay((entry.get("title") or "Weather alert").strip())
        type_label = classify_alert_kind(title_raw)
        sev = severity_emoji(title_raw)

        care = ""
        if care_rows:
            try:
                care_severity = "🟢" if ended else sev
                # IMPORTANT: care statements are FB-only in this bot
                care = pick_care_statement(care_rows, care_severity, type_label, platform="FB")

                if care:
                    print(f"CareStatements: matched ({care_severity} / {type_label} / FB)")
                else:
                    print(f"CareStatements: no match for ({care_severity} / {type_label} / FB)")
                print("CareStatements: chosen text:", (care[:120] + "…") if care and len(care) > 120 else care)
            except Exception as e:
                print(f"⚠️ Care statement match failed: {e}")
                care = ""

        image_refs = choose_images_for_alert(
            drive_svc=drive_svc,
            drive_folder_id=drive_folder_id,
            alert_kind=alert_kind,
            alert_type_label=title_raw,
            severity=sev,
        )

        # X stays clean: do NOT pass FB care
        x_text = build_x_post_text(entry, more_url=more_url, care="")
        fb_text = build_facebook_post_text(entry, care=care, more_url=more_url)

        h = text_hash(x_text)
        if h in posted_text_hashes:
            print("Social skipped: duplicate text hash already posted")
            posted.add(guid)
            continue

        # ---------------------------------------------------------
        # Telegram gate preview / policy
        # ---------------------------------------------------------
        if TELEGRAM_ENABLE_GATE:
            token = hashlib.sha1(guid.encode("utf-8")).hexdigest()[:10]

            ingest_telegram_actions(state, save_state)
            maybe_send_reminders(state, save_state)

            preview_text = (
                f"{'🟢' if ended else '🚨'} {title}\n\n"
                f"----- X (will post) -----\n{x_text}\n\n"
                f"----- Facebook (will post) -----\n{fb_text}\n\n"
                f"Alert type: {alert_kind.upper()}\n\n"
                f"More:\n{more_url}"
            )

            ensure_preview_sent(state, save_state, token, preview_text, kind=alert_kind, image_urls=image_refs)

            # --- Handle Remix / Custom requests (rebuild preview when requested) ---
            custom = custom_text_for(state, token) or {}
            current_remix = remix_count_for(state, token)
            last_seen = int((state.get("telegram_last_remix_seen") or {}).get(token, 0))

            x_extra = (custom.get("x") or "").strip()
            fb_extra = (custom.get("fb") or "").strip()

            needs_refresh = (current_remix != last_seen) or bool(x_extra) or bool(fb_extra)

            if needs_refresh:
                care2 = care
                care_colour_for_sheet = "🟢" if ended else sev

                if care_rows and (current_remix != last_seen):
                    try:
                        care2 = pick_remixed_care_text(
                            care_rows=care_rows,
                            colour=care_colour_for_sheet,
                            alert_type=type_label,
                            current_care_text=care,
                            remix_count=current_remix,
                            platform="FB",
                        )
                    except Exception as e:
                        print(f"⚠️ Care remix failed: {e}")
                        care2 = care

                x_text2 = build_x_post_text(entry, more_url=more_url, care="", custom_x=x_extra)
                fb_text2 = build_facebook_post_text(entry, care=care2, more_url=more_url, custom_fb=fb_extra)

                care = care2
                x_text = x_text2
                fb_text = fb_text2

                refreshed_preview = (
                    f"{'🟢' if ended else '🚨'} {title}\n\n"
                    f"----- X (will post) -----\n{x_text}\n\n"
                    f"----- Facebook (will post) -----\n{fb_text}\n\n"
                    f"Alert type: {alert_kind.upper()}\n\n"
                    f"More:\n{more_url}"
                )

                try:
                    update_preview(state, save_state, token, refreshed_preview, image_urls=image_refs)
                except Exception:
                    try:
                        tg_send_message(refreshed_preview)
                    except Exception:
                        pass

                try:
                    clear_custom_text(state, token)
                except Exception:
                    pass

                state.setdefault("telegram_last_remix_seen", {})
                state["telegram_last_remix_seen"][token] = int(current_remix)
                save_state(state)

                ingest_telegram_actions(state, save_state)

            ingest_telegram_actions(state, save_state)
            d = decision_for(state, token)

            if alert_kind == "warning":
                if d == "denied":
                    print(f"🛑 Telegram denied for WARNING token={token}. Skipping.")
                    try:
                        tg_send_message(f"🛑 DENIED — will NOT post.\nTOKEN: {token}")
                    except Exception:
                        pass
                    continue

                if d == "approved":
                    print(f"✅ Telegram approved for WARNING token={token}. Proceeding to post.")
                    try:
                        tg_send_message(f"✅ APPROVED — proceeding to post now.\nTOKEN: {token}")
                    except Exception:
                        pass
                else:
                    if not warning_delay_elapsed(state, token):
                        print(f"⏳ WARNING waiting for delay window (token={token}). Not posting this run.")
                        continue

                    ingest_telegram_actions(state, save_state)
                    d2 = decision_for(state, token)
                    if d2 == "denied":
                        print(f"🛑 Telegram denied during delay window (token={token}). Skipping.")
                        try:
                            tg_send_message(f"🛑 DENIED — will NOT post.\nTOKEN: {token}")
                        except Exception:
                            pass
                        continue

                    print(f"✅ WARNING delay elapsed and not denied (token={token}). Proceeding to post.")
                    try:
                        tg_send_message(f"✅ Proceeding to post (WARNING delay elapsed; not denied).\nTOKEN: {token}")
                    except Exception:
                        pass

            else:
                if d not in ("approved", "denied"):
                    d = wait_for_decision_safe(state, token)

                if d != "approved":
                    print(f"🛑 Telegram not approved for token={token} (d={d}). Skipping.")
                    try:
                        tg_send_message(f"🛑 NOT APPROVED — will NOT post.\nDecision: {d}\nTOKEN: {token}")
                    except Exception:
                        pass
                    continue

                print(f"✅ Telegram approved for token={token}. Proceeding to post.")
                try:
                    tg_send_message(f"✅ APPROVED — proceeding to post now.\nTOKEN: {token}")
                except Exception:
                    pass
        else:
            print("Telegram gate disabled — skipping social post (safe default).")
            continue

        # ✅ FINAL DENY GUARD
        if TELEGRAM_ENABLE_GATE:
            ingest_telegram_actions(state, save_state)
            if decision_for(state, token) == "denied":
                print(f"🛑 Final guard: token={token} was denied. Skipping post.")
                try:
                    tg_send_message(f"🛑 DENIED — will NOT post.\nTOKEN: {token}")
                except Exception:
                    pass
                continue

        # ---------------------------------------------------------
        # Post now (capture outcomes + confirm to Telegram)
        # ---------------------------------------------------------
        posted_this = False
        x_ok = False
        fb_ok = False
        x_err = ""
        fb_err = ""

        # --- X ---
        if EFFECTIVE_ENABLE_X_POSTING:
            try:
                safe_post_to_x(x_text, image_refs=image_refs)
                x_ok = True
                posted_this = True
            except Exception as e:
                msg = str(e)
                if "X_DUPLICATE_TWEET" in msg:
                    x_ok = True
                    posted_this = True
                    print("ℹ️ X duplicate detected — treating as already posted.")
                else:
                    x_err = msg
                    print(f"⚠️ X post failed: {e}")
        else:
            print("X posting skipped (disabled or run mode).")

        # --- Facebook ---
        if EFFECTIVE_ENABLE_FB_POSTING:
            fb_images = materialize_images_for_facebook(image_refs)
            try:
                try:
                    safe_post_to_facebook(state, caption=fb_text, image_urls=fb_images)
                    fb_ok = True
                    posted_this = True
                except Exception as e:
                    fb_err = str(e)
                    print(f"⚠️ Facebook post failed: {e}")
            finally:
                cleanup_tmp_media_files(fb_images)
        else:
            print("Facebook posting skipped (disabled or run mode).")

        # --- Telegram: confirm outcome ---
        if TELEGRAM_ENABLE_GATE:
            try:
                lines = []
                lines.append("✅ APPROVED — posting complete")
                lines.append(f"🟢 ENDED: {'yes' if ended else 'no'}")
                lines.append(f"TOKEN: {token}")

                if EFFECTIVE_ENABLE_X_POSTING:
                    lines.append(f"X: {'✅ posted' if x_ok else '❌ failed'}")
                    if x_err and not x_ok:
                        lines.append(f"X error: {x_err[:140]}")
                else:
                    lines.append("X: ⏭️ skipped")

                if EFFECTIVE_ENABLE_FB_POSTING:
                    lines.append(f"Facebook: {'✅ posted' if fb_ok else '❌ failed'}")
                    if fb_err and not fb_ok:
                        lines.append(f"FB error: {fb_err[:140]}")
                else:
                    lines.append("Facebook: ⏭️ skipped")

                if posted_this:
                    lines.append("✅ Success.")
                else:
                    lines.append("⚠️ Nothing posted (all platforms skipped or failed).")

                tg_send_message("\n".join(lines))
            except Exception as e:
                print(f"⚠️ Telegram post-confirmation failed: {e}")

        # ---------------------------------------------------------
        # Update state
        # ---------------------------------------------------------
        if posted_this:
            posted.add(guid)
            posted_text_hashes.add(h)

            state["posted_guids"] = list(posted)
            state["posted_text_hashes"] = list(posted_text_hashes)

            mark_posted(state, DISPLAY_AREA_NAME, kind=alert_kind)
            save_state(state)

    # --- Write RSS file at end
    try:
        trim_rss_items(channel, MAX_RSS_ITEMS)
        tree.write(RSS_PATH, encoding="utf-8", xml_declaration=True)
    except Exception as e:
        print(f"⚠️ Failed writing RSS to {RSS_PATH}: {e}")

    state["posted_guids"] = list(posted)
    state["posted_text_hashes"] = list(posted_text_hashes)
    save_state(state)


if __name__ == "__main__":
    main()
