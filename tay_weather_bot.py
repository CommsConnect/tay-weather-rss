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
# MORE LINK RULE
#   - Always prefer TAY_ALERTS_URL first
#   - If TAY_ALERTS_URL is missing or unreachable, fall back to TAY_COORDS_URL
#
# REQUIRED GitHub Secrets (X OAuth 2.0 posting):
#   X_CLIENT_ID
#   X_CLIENT_SECRET
#   X_REFRESH_TOKEN
#
# REQUIRED GitHub Secrets (X media upload via OAuth 1.0a user context):
#   X_API_KEY
#   X_API_SECRET
#   X_ACCESS_TOKEN
#   X_ACCESS_TOKEN_SECRET
#
# REQUIRED GitHub Secrets (Facebook Page posting):
#   FB_PAGE_ID
#   FB_PAGE_ACCESS_TOKEN
#
# OPTIONAL GitHub Secrets (Google Sheet + Drive for care statements + curated photos):
#   GOOGLE_SHEET_ID
#   GOOGLE_SERVICE_ACCOUNT_JSON
#   GOOGLE_DRIVE_FOLDER_ID
#
# OPTIONAL workflow env vars:
#   ENABLE_X_POSTING=true|false
#   ENABLE_FB_POSTING=true|false
#   TEST_X=true|false
#   TEST_FACEBOOK=true|false
#   ALERT_FEED_URL=<ATOM feed url>
#   TAY_ALERTS_URL=<preferred public "More" URL>
#   TAY_COORDS_URL=<fallback coords link>
#   CR29_NORTH_IMAGE_URL=<direct image url OR https://511on.ca/map/Cctv/<id>>
#   CR29_SOUTH_IMAGE_URL=<direct image url OR https://511on.ca/map/Cctv/<id>>
#   ON511_CAMERA_KEYWORD=<default: CR-29>
#   TELEGRAM_ENABLE_GATE=true|false
#   TELEGRAM_PREVIEW_DELAY_MIN=<minutes before WARNING auto-posts unless denied>
#   TELEGRAM_WAIT_SECONDS=<max seconds to wait for approval on WATCH/OTHER>
#
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
    decision_for,
    is_expired,
    warning_delay_elapsed,
    wait_for_decision,
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
# - TAY_ALERTS_URL is your preferred "More" page (your GitHub pages /tay/).
# - TAY_COORDS_URL is the fallback official coords URL.
TAY_ALERTS_URL = (os.getenv("TAY_ALERTS_URL") or "").strip()
TAY_COORDS_URL = (os.getenv("TAY_COORDS_URL") or "https://weather.gc.ca/en/location/index.html?coords=44.751,-79.768").strip()

# =============================================================================
# Ontario 511 camera resolver settings
# =============================================================================
ON511_CAMERAS_API = "https://511on.ca/api/v2/get/cameras"
ON511_CAMERA_KEYWORD = os.getenv("ON511_CAMERA_KEYWORD", "CR-29").strip() or "CR-29"

# =============================================================================
# Cooldown policy
# =============================================================================
# NOTE: This is your "anti-spam" policy.
# - We still dedupe by GUID and by text hash.
# - This cooldown prevents re-posting too frequently even if EC changes small text.
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


# =============================================================================
# Severity emoji
# =============================================================================
def severity_emoji(title: str) -> str:
    """
    Advisory = 🟡
    Watch    = 🟠
    Warning  = 🔴
    Other    = ⚪
    """
    t = (title or "").lower()
    if "warning" in t:
        return "🔴"
    if "watch" in t:
        return "🟠"
    if "advisory" in t:
        return "🟡"
    return "⚪"


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
    """
    This is the only file that makes the bot remember what it has done.
    If you reset this file, the bot behaves like it's the first run again.
    """
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
    """
    Save state and cap growth so it doesn't grow forever.
    """
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
    """
    Returns (allowed, reason)
    """
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
#   - Prefer TAY_ALERTS_URL
#   - Fall back to TAY_COORDS_URL if missing or unreachable
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
    """
    This ensures your posts always use your desired URL when it works.
    """
    if TAY_ALERTS_URL and _url_looks_ok(TAY_ALERTS_URL):
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


def fetch_feed_entries(
    feed_url: str,
    retries: int = 3,
    timeout: Tuple[int, int] = (5, 20),
) -> List[Dict[str, Any]]:
    """
    Fetch EC feed. Supports Atom (<entry>) and RSS 2.0 (<item>).
    Returns entries newest-first with a common shape:
      {id,title,link,summary,updated_dt}
    """
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

                        # Description can be HTML; keep raw text here
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
                key=lambda x: x.get("updated_dt")
                or dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc),
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
    """
    Convert EC region naming to your local naming.
    """
    if not title:
        return title
    t = title.replace(", Midland - Coldwater - Orr Lake", f" ({DISPLAY_AREA_NAME})")
    t = t.replace("Midland - Coldwater - Orr Lake", DISPLAY_AREA_NAME)
    return t


def atom_entry_guid(entry: Dict[str, Any]) -> str:
    return (entry.get("id") or entry.get("link") or entry.get("title") or "").strip()


# =============================================================================
# Environment Canada detail extraction
# =============================================================================
def _extract_details_lines_from_ec(official_url: str) -> List[str]:
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

    m_what = re.search(r"What:\s*(.+?)(?=\s+(When:|Where:|Additional information:))", text, re.IGNORECASE)
    m_when = re.search(r"When:\s*(.+?)(?=\s+(Where:|Additional information:)|$)", text, re.IGNORECASE)

    out: List[str] = []
    if m_what:
        what = m_what.group(1).strip()
        if what:
            out.append(what.rstrip(".") + ".")
    if m_when:
        when = m_when.group(1).strip()
        if when:
            out.append(when.rstrip(".") + ".")

    if out:
        return out[:2]

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
            return [s.rstrip(".") + "."]
    return []


# =============================================================================
# Google Sheet + Google Drive integration
# =============================================================================
def _google_services() -> Tuple[Optional[Any], Optional[Any], str, str]:
    """
    Returns (sheets_service, drive_service, sheet_id, drive_folder_id).
    If missing creds, returns (None, None, "", "").
    """
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


def pick_care_statement(care_rows: List[dict], colour: str, alert_type: str) -> str:
    def enabled(r: dict) -> bool:
        return str(r.get("enabled", "")).strip().lower() in ("true", "yes", "1", "y")

    def norm_colour(c: str) -> str:
        c = (c or "").strip().lower()
        if c in ("🔴", "red", "warning"):
            return "🔴"
        if c in ("🟠", "orange", "watch"):
            return "🟠"
        if c in ("🟡", "yellow", "advisory"):
            return "🟡"
        if c in ("⚪", "white", "other", "statement"):
            return "⚪"
        return (c or "").strip()

    want_type = (alert_type or "").strip().lower()
    want_colour = norm_colour((colour or "").strip())

    def matches(r: dict, c_req: str, t_req: str) -> bool:
        rc = norm_colour((r.get("colour") or "").strip())
        rt = (r.get("type") or "").strip().lower()
        if c_req and rc != c_req:
            return False
        if t_req and rt != t_req:
            return False
        return True

    buckets = [
        (want_colour, want_type),
        ("", want_type),
        (want_colour, ""),
        ("", ""),
    ]

    for bc, bt in buckets:
        for r in care_rows:
            if not enabled(r):
                continue
            if matches(r, bc, bt):
                txt = (r.get("text") or "").strip()
                if txt:
                    return txt
    return ""


def load_media_rules_rows() -> List[dict]:
    sheets_svc, _, sheet_id, _ = _google_services()
    if not sheets_svc or not sheet_id:
        return []

    try:
        rng = "MediaRules!A:Z"
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
    except Exception:
        return []


# =============================================================================
# Google Drive curated photos (FIRST choice for images)
# =============================================================================
def _drive_direct_download_url(file_id: str) -> str:
    return f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"


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

    q = (
        f"'{folder_id}' in parents and trashed = false and "
        "("
        "mimeType contains 'image/'"
        ")"
    )

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

    r = requests.get(
        image_url,
        headers={"User-Agent": USER_AGENT},
        timeout=(10, 30),
        allow_redirects=True,
    )
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
    ET.SubElement(channel, "link").text = "https://weatherpresenter.github.io/tay-weather-rss/"
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
def _pretty_title_for_social(title: str) -> str:
    t = (title or "").strip()
    t = re.sub(r"\s*\(.*?\)\s*$", "", t).strip()
    return f"{t} in Tay Township"


def _issued_short(issued: str) -> str:
    s = (issued or "").strip()
    s2 = re.sub(r"^Issued:\s*", "", s, flags=re.IGNORECASE).strip()

    m = re.search(
        r"(?P<h>\d{1,2}):(?P<min>\d{2})\s*(?P<ampm>AM|PM)\b.*?\b(?P<day>\d{1,2})\s+(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)\s+(?P<year>\d{4})",
        s2,
        flags=re.IGNORECASE,
    )
    if not m:
        return s

    h = int(m.group("h"))
    minute = m.group("min")
    ampm = m.group("ampm").lower()
    day = int(m.group("day"))
    month = m.group("month").strip().capitalize()[:3]

    suffix = "a" if ampm.startswith("a") else "p"
    return f"Issued {month} {day} {h}:{minute}{suffix}"


def build_x_post_text(entry: Dict[str, Any], more_url: str) -> str:
    title_raw = atom_title_for_tay((entry.get("title") or "").strip())
    issued_raw = (entry.get("summary") or "").strip()
    official = (entry.get("link") or "").strip()

    sev = severity_emoji(title_raw)
    title_line = f"{sev} - {_pretty_title_for_social(title_raw)}"

    details_lines: List[str] = []
    try:
        details_lines = _extract_details_lines_from_ec(official)
    except Exception as e:
        print(f"⚠️ EC details parse failed (X): {e}")

    issued_short = _issued_short(issued_raw)

    parts: List[str] = []
    parts.append(title_line)
    parts.append("")
    parts.extend(details_lines[:2])
    parts.append("")
    parts.append(f"More: {more_url}")
    parts.append(f"{issued_short} #TayTownship #ONStorm")

    text = "\n".join([p for p in parts if p is not None])

    if len(text) <= 280:
        return text

    if len(details_lines) > 1:
        parts2 = [
            title_line,
            "",
            details_lines[0],
            "",
            f"More: {more_url}",
            f"{issued_short} #TayTownship #ONStorm",
        ]
        text2 = "\n".join([p for p in parts2 if p is not None])
        if len(text2) <= 280:
            return text2

    parts3 = [
        title_line,
        "",
        f"More: {more_url}",
        f"{issued_short} #TayTownship #ONStorm",
    ]
    text3 = "\n".join([p for p in parts3 if p is not None])
    if len(text3) <= 280:
        return text3

    return text[:277].rstrip() + "..."


def build_facebook_post_text(entry: Dict[str, Any], care: str, more_url: str) -> str:
    title_raw = atom_title_for_tay((entry.get("title") or "").strip())
    issued_raw = (entry.get("summary") or "").strip()
    official = (entry.get("link") or "").strip()

    sev = severity_emoji(title_raw)
    title_line = f"{sev} - {_pretty_title_for_social(title_raw)}"
    issued_short = _issued_short(issued_raw)

    details_lines: List[str] = []
    try:
        details_lines = _extract_details_lines_from_ec(official)
    except Exception as e:
        print(f"⚠️ EC details parse failed (FB): {e}")

    parts: List[str] = []
    parts.append(title_line)
    parts.append("")
    parts.extend(details_lines[:3])

    if care:
        parts.append("")
        parts.append(care.strip())

    parts.append("")
    parts.append(f"More: {more_url}")
    parts.append(f"{issued_short} #TayTownship #ONStorm")

    return "\n".join([p for p in parts if p is not None])


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

    if alert_kind == "warning" and "wind" in (alert_type_label or "").lower():
        return []

    try:
        return resolve_cr29_image_urls()
    except Exception as e:
        print(f"⚠️ Camera fallback failed: {e}")
        return []


# =============================================================================
# Main routine
# =============================================================================
def main() -> None:
    more_url = resolve_more_info_url()

    if os.path.exists(ROTATED_X_REFRESH_TOKEN_PATH):
        try:
            os.remove(ROTATED_X_REFRESH_TOKEN_PATH)
        except Exception:
            pass

    sheets_svc, drive_svc, sheet_id, drive_folder_id = _google_services()

    global _DRIVE_SVC_FOR_MEDIA
    _DRIVE_SVC_FOR_MEDIA = drive_svc

    care_rows = []
    try:
        care_rows = load_care_statements_rows()
    except Exception as e:
        print(f"⚠️ CareStatements load failed: {e}")
        care_rows = []

    media_rules = []
    try:
        media_rules = load_media_rules_rows()
        if media_rules:
            print(f"MediaRules loaded: {len(media_rules)} rows")
        else:
            print("MediaRules: none (empty or missing). Using default Drive-first behaviour.")
    except Exception as e:
        print(f"⚠️ MediaRules load failed (ignored): {e}")

    # -------------------------------------------------------------------------
    # TEST MODE (manual)
    # -------------------------------------------------------------------------
    if TEST_X or TEST_FACEBOOK:
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
                f"Platforms: {platforms_str}\n\n"
                f"Tap ✅ Approve / 🛑 Deny below.\n\n"
                f"TOKEN: {token}"
            )

            ensure_preview_sent(
                st,
                save_state,
                token,
                preview_text,
                kind="other",
                image_urls=test_images,
            )

            d = decision_for(st, token)
            if d not in ("approved", "denied"):
                wait_seconds = int(os.getenv("TELEGRAM_WAIT_SECONDS", "600"))
                d = wait_for_decision(
                    st,
                    save_state,
                    token,
                    max_wait_seconds=wait_seconds,
                    poll_interval_seconds=4,
                )

            if d == "denied":
                print("Telegram: denied (test). Not posting.")
                return
            if d != "approved":
                print("Telegram: still pending (test). Not posting this run.")
                return

        if ENABLE_X_POSTING and TEST_X:
            post_to_x(f"{base}\n\n(X)", image_refs=test_images)

        if ENABLE_FB_POSTING and TEST_FACEBOOK:
            st2 = load_state()
            st2.pop("fb_cooldown_until", None)
            st2.pop("fb_last_posted_at", None)

            fb_local = materialize_images_for_facebook(test_images)
            try:
                fb_result = fb.safe_post_facebook(
                    st2,
                    caption=f"{base}\n\n(Facebook)",
                    image_urls=fb_local,
                    has_new_social_event=True,
                    state_path=STATE_PATH,
                )
                print("FB result:", fb_result)
            finally:
                cleanup_tmp_media_files(fb_local)

        return

    # -------------------------------------------------------------------------
    # NORMAL MODE (real alerts)
    # -------------------------------------------------------------------------
    state = load_state()

    # ----------------------------
    # Load CareStatements once per run (Google Sheet)
    # ----------------------------
    care_rows: List[dict] = []
    try:
        care_rows = load_care_statements_rows()
        print(f"CareStatements: loaded {len(care_rows)} rows")
    except Exception as e:
        print(f"⚠️ CareStatements failed to load (will post without care text): {e}")
        care_rows = []

    posted = set(state.get("posted_guids", []))
    posted_text_hashes = set(state.get("posted_text_hashes", []))

    tree, channel = load_rss_tree()

    try:
        atom_entries = fetch_atom_entries(ALERT_FEED_URL)
    except Exception as e:
        print(f"⚠️ ATOM feed unavailable: {e}")
        print("Exiting cleanly; will retry on next scheduled run.")
        return

    # Process newest-first
    for entry in atom_entries:
        guid = atom_entry_guid(entry)
        if not guid:
            continue

        title = atom_title_for_tay((entry.get("title") or "Weather alert").strip())
        title_l = (title or "").lower()
        summary_l = ((entry.get("summary") or "")).lower()

        # ---------------------------------------------------------
        # Skip "ended/cancelled/no alert" entries (ALWAYS do this)
        # ---------------------------------------------------------
        inactive_markers = (
            "ended",
            "has ended",
            "cancelled",
            "no longer in effect",
            "is no longer in effect",
            "terminated",
            "rescinded",
            "no alerts in effect",
            "no watches or warnings in effect",
        )
        if any(m in title_l for m in inactive_markers) or any(m in summary_l for m in inactive_markers):
            print(f"Info: non-active item — skipping social post: {title}")
            posted.add(guid)
            continue

        # ---------------------------------------------------------
        # RSS write (ALWAYS do this, independent of care statements)
        # ---------------------------------------------------------
        pub_dt = entry.get("updated_dt") or dt.datetime.now(dt.timezone.utc)
        pub_date = email.utils.format_datetime(pub_dt)
        link = more_url
        description = build_rss_description_from_atom(entry, more_url=more_url)

        if not rss_item_exists(channel, guid):
            add_rss_item(
                channel,
                title=title,
                link=link,
                guid=guid,
                pub_date=pub_date,
                description=description,
            )

        # ---------------------------------------------------------
        # Already posted this alert GUID? skip social
        # ---------------------------------------------------------
        if guid in posted:
            continue

        # ---------------------------------------------------------
        # Determine kind + cooldown bucket
        # ---------------------------------------------------------
        alert_kind = classify_alert_kind(title)

        allowed, reason = cooldown_allows_post(state, DISPLAY_AREA_NAME, kind=alert_kind)
        if not allowed:
            print("Social skipped:", reason)
            continue

        # ---------------------------------------------------------
        # Build alert type label + severity
        # ---------------------------------------------------------
        type_label = re.sub(r"\s*\(.*?\)\s*$", "", title).strip().lower()
        sev = severity_emoji(title)

        # ---------------------------------------------------------
        # CARE STATEMENT (Facebook only)
        # ---------------------------------------------------------
        care = ""
        if care_rows:
            try:
                care = pick_care_statement(care_rows, sev, type_label)
                if care:
                    print(f"CareStatements: matched ({sev} / {type_label})")
                else:
                    print(f"CareStatements: no match for ({sev} / {type_label})")
            except Exception as e:
                print(f"⚠️ Care statement match failed: {e}")
                care = ""

        # ---------------------------------------------------------
        # Choose images (Drive first, then cameras)
        # ---------------------------------------------------------
        image_refs = choose_images_for_alert(
            drive_svc=drive_svc,
            drive_folder_id=drive_folder_id,
            alert_kind=alert_kind,
            alert_type_label=type_label,
            severity=sev,
        )

        # ---------------------------------------------------------
        # Build platform-specific text
        # ---------------------------------------------------------
        x_text = build_x_post_text(entry, more_url=more_url)
        fb_text = build_facebook_post_text(entry, care=care, more_url=more_url)

        # ---------------------------------------------------------
        # Dedupe by X text hash
        # ---------------------------------------------------------
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
                f"🚨 {title}\n\n"
                f"----- X (will post) -----\n{x_text}\n\n"
                f"----- Facebook (will post) -----\n{fb_text}\n\n"
                f"Alert type: {alert_kind.upper()}\n\n"
                f"More:\n{more_url}"
            )

            ensure_preview_sent(
                state,
                save_state,
                token,
                preview_text,
                kind=alert_kind,
                image_urls=image_refs,
            )

            d = decision_for(state, token)

            if alert_kind in ("watch", "advisory", "statement", "other"):
                if d == "denied":
                    continue
                if d != "approved":
                    if is_expired(state, token):
                        continue
                    d = wait_for_decision(
                        state,
                        save_state,
                        token,
                        max_wait_seconds=int(os.getenv("TELEGRAM_WAIT_SECONDS", "600")),
                        poll_interval_seconds=4,
                    )
                    if d != "approved":
                        continue

            elif alert_kind == "warning":
                if d == "denied":
                    continue
                if not warning_delay_elapsed(state, token):
                    continue

        # ---------------------------------------------------------
        # Post now
        # ---------------------------------------------------------
        posted_this = False

        if ENABLE_X_POSTING:
            post_to_x(x_text, image_refs=image_refs)
            posted_this = True

        if ENABLE_FB_POSTING:
            fb_images = materialize_images_for_facebook(image_refs)
            try:
                fb.safe_post_facebook(
                    state,
                    caption=fb_text,
                    image_urls=fb_images,
                    has_new_social_event=True,
                    state_path=STATE_PATH,
                )
                posted_this = True
            finally:
                cleanup_tmp_media_files(fb_images)

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
