# tay_weather_bot.py
#
# Tay Township Weather Bot
# - Pulls Environment Canada alerts from regional ATOM feed (source of truth)
# - Writes RSS feed: tay-weather.xml
# - Posts to X automatically (OAuth 2.0 refresh token)
# - Uploads media to X via OAuth 1.0a user context (required for media upload)
# - Posts to Facebook Page automatically (Page access token), supports photo carousels
# - Supports cooldowns + dedupe
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
# OPTIONAL workflow env vars:
#   ENABLE_X_POSTING=true|false
#   ENABLE_FB_POSTING=true|false
#   TEST_TWEET=true
#   ALERT_FEED_URL=<ATOM feed url>
#   TAY_COORDS_URL=<coords link>
#   TAY_ALERTS_URL=<preferred tay alerts link>   <-- (you already set this in workflow)
#   CR29_NORTH_IMAGE_URL=<direct image url OR https://511on.ca/map/Cctv/<id>>
#   CR29_SOUTH_IMAGE_URL=<direct image url OR https://511on.ca/map/Cctv/<id>>
#   ON511_CAMERA_KEYWORD=<default: CR-29>
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
from typing import Any, Dict, List, Optional, Tuple

import requests
from PIL import Image
from io import BytesIO
import facebook_poster as fb
from requests_oauthlib import OAuth1
from pathlib import Path
from telegram_gate import (
    ingest_telegram_actions,
    maybe_send_reminders,
    ensure_preview_sent,
    decision_for,
    is_expired,
    warning_delay_elapsed,
    wait_for_decision,
)

from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ----------------------------
# Feature toggles
# ----------------------------
ENABLE_X_POSTING = os.getenv("ENABLE_X_POSTING", "false").lower() == "true"
ENABLE_FB_POSTING = os.getenv("ENABLE_FB_POSTING", "false").lower() == "true"
TELEGRAM_ENABLE_GATE = os.getenv("TELEGRAM_ENABLE_GATE", "true").lower() == "true"

# Legacy single-platform test flag (optional)
TEST_TWEET = os.getenv("TEST_TWEET", "false").lower() == "true"

# ----------------------------
# Test mode flags (from GitHub Actions)
# - TEST_TWEET maps to TEST_X for backward compatibility
# ----------------------------
TEST_X = os.getenv("TEST_X", "false").lower() == "true" or TEST_TWEET
TEST_FACEBOOK = os.getenv("TEST_FACEBOOK", "false").lower() == "true"

# ----------------------------
# Paths
# ----------------------------
STATE_PATH = "state.json"
RSS_PATH = "tay-weather.xml"
ROTATED_X_REFRESH_TOKEN_PATH = "x_refresh_token_rotated.txt"

USER_AGENT = "tay-weather-rss-bot/1.1"

# Public URLs (preferred: TAY_ALERTS_URL, fallback: TAY_COORDS_URL)
TAY_COORDS_URL = os.getenv(
    "TAY_COORDS_URL",
    "https://weather.gc.ca/en/location/index.html?coords=44.751,-79.768",
).strip()
TAY_ALERTS_URL = (os.getenv("TAY_ALERTS_URL") or "").strip()

ALERT_FEED_URL = os.getenv("ALERT_FEED_URL", "https://weather.gc.ca/rss/battleboard/onrm94_e.xml").strip()
DISPLAY_AREA_NAME = "Tay Township area"

# Ontario 511 cameras API
ON511_CAMERAS_API = "https://511on.ca/api/v2/get/cameras"
ON511_CAMERA_KEYWORD = os.getenv("ON511_CAMERA_KEYWORD", "CR-29").strip() or "CR-29"


# ----------------------------
# Severity emoji
# ----------------------------
def severity_emoji(title: str) -> str:
    """Advisory=🟡, Watch=🟠, Warning=🔴, other=⚪"""
    t = (title or "").lower()
    if "warning" in t:
        return "🔴"
    if "watch" in t:
        return "🟠"
    if "advisory" in t:
        return "🟡"
    return "⚪"


# ----------------------------
# Cooldown policy
# ----------------------------
COOLDOWN_MINUTES = {
    "warning": 60,
    "watch": 120,
    "advisory": 180,
    "statement": 240,
    "alert": 180,
    "allclear": 60,
    "default": 180,
}
GLOBAL_COOLDOWN_MINUTES = 5


# ----------------------------
# Generic helpers
# ----------------------------
def normalize(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def safe_int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default


def text_hash(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()


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

    # Ensure required keys always exist
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


def _url_looks_ok(url: str) -> bool:
    """
    Lightweight check:
    - must be http(s)
    - HEAD/GET returns < 400
    """
    url = (url or "").strip()
    if not url.lower().startswith(("http://", "https://")):
        return False
    try:
        r = requests.head(url, allow_redirects=True, headers={"User-Agent": USER_AGENT}, timeout=(5, 12))
        if r.status_code and r.status_code < 400:
            return True
    except Exception:
        pass
    try:
        r = requests.get(url, allow_redirects=True, headers={"User-Agent": USER_AGENT}, timeout=(5, 12))
        return r.status_code < 400
    except Exception:
        return False


def get_more_info_url() -> str:
    """
    Your rule:
      - Choose TAY_ALERTS_URL first
      - Fall back to TAY_COORDS_URL if something is wrong
    """
    if TAY_ALERTS_URL and _url_looks_ok(TAY_ALERTS_URL):
        return TAY_ALERTS_URL
    return TAY_COORDS_URL


def materialize_images_for_facebook(image_urls: List[str]) -> List[str]:
    """
    Facebook path: force our downloader/bug overlay to run by converting remote URLs
    into local temp files (non-URL refs). Your fb module only uses fb.load_image_bytes
    for non-URL inputs.
    Returns list of local file paths.
    """
    out_paths: List[str] = []
    for i, u in enumerate([x for x in (image_urls or []) if (x or "").strip()][:10]):
        b, mt = download_image_bytes(u)  # <-- applies bug overlay for 511 URLs
        ext = "png" if mt == "image/png" else "jpg"
        p = f"/tmp/tay_cam_{i}.{ext}"
        with open(p, "wb") as f:
            f.write(b)
        out_paths.append(p)
    return out_paths


def cleanup_tmp_cam_files(paths: List[str]) -> None:
    for p in paths or []:
        try:
            if isinstance(p, str) and p.startswith("/tmp/tay_cam_"):
                os.remove(p)
        except Exception:
            pass


# ----------------------------
# ATOM helpers
# ----------------------------
ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}


def _parse_atom_dt(s: str) -> dt.datetime:
    if not s:
        return dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def fetch_atom_entries(
    feed_url: str,
    retries: int = 3,
    timeout: Tuple[int, int] = (5, 20),
) -> List[Dict[str, Any]]:
    """Fetch and parse an ATOM feed. Returns entries newest-first."""
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = requests.get(feed_url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
            r.raise_for_status()
            root = ET.fromstring(r.content)

            entries: List[Dict[str, Any]] = []
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
                        "updated": updated,
                        "published": published,
                        "summary": summary,
                        "updated_dt": _parse_atom_dt(updated or published),
                    }
                )

            entries.sort(key=lambda x: x["updated_dt"], reverse=True)
            return entries
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
                continue
            raise
    raise last_err if last_err else RuntimeError("Failed to fetch ATOM feed")


def atom_title_for_tay(title: str) -> str:
    if not title:
        return title
    t = title.replace(", Midland - Coldwater - Orr Lake", f" ({DISPLAY_AREA_NAME})")
    t = t.replace("Midland - Coldwater - Orr Lake", DISPLAY_AREA_NAME)
    return t


def atom_entry_guid(entry: Dict[str, Any]) -> str:
    return (entry.get("id") or entry.get("link") or entry.get("title") or "").strip()


def _sheet_service():
    sheet_id = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
    sa_json = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not sheet_id or not sa_json:
        return None, None

    info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return svc, sheet_id


def load_care_statements_rows() -> list[dict]:
    """
    Reads CareStatements tab. Expected columns (header row):
      enabled, colour, type, text
    'colour' should match the emoji colour bucket: 🔴 🟠 🟡 ⚪ (or can be blank for any)
    'type' should match the alert type text, e.g. 'Special weather statement' (or blank for any)
    """
    svc, sheet_id = _sheet_service()
    if not svc:
        return []

    rng = "CareStatements!A:Z"
    resp = svc.spreadsheets().values().get(spreadsheetId=sheet_id, range=rng).execute()
    values = resp.get("values", [])
    if not values or len(values) < 2:
        return []

    headers = [h.strip().lower() for h in values[0]]
    rows = []
    for v in values[1:]:
        row = {
            headers[i]: (
                v[i].strip()
                if i < len(v) and isinstance(v[i], str)
                else (v[i] if i < len(v) else "")
            )
            for i in range(len(headers))
        }
        rows.append(row)
    return rows


def pick_care_statement(care_rows: list[dict], colour: str, alert_type: str) -> str:
    """
    Precedence (best -> fallback):
      enabled + exact colour + exact type
      enabled + any colour + exact type
      enabled + exact colour + any type
      enabled + any colour + any type
    """
    def enabled(r):
        return str(r.get("enabled", "")).strip().lower() in ("true", "yes", "1", "y")

    alert_type_l = (alert_type or "").strip().lower()
    colour_l = (colour or "").strip()

    def matches(r, want_colour, want_type):
        rc = (r.get("colour") or "").strip()
        rt = (r.get("type") or "").strip().lower()
        if want_colour and rc != want_colour:
            return False
        if want_type and rt != want_type:
            return False
        return True

    def norm_colour(c: str) -> str:
        c = (c or "").strip().lower()
        if c in ("🔴", "red", "warning"): return "🔴"
        if c in ("🟠", "orange", "watch"): return "🟠"
        if c in ("🟡", "yellow", "advisory"): return "🟡"
        if c in ("⚪", "white", "other", "statement"): return "⚪"
        return (c or "").strip()

    colour_n = norm_colour(colour_l)

    buckets = [
        (colour_n, alert_type_l),
        ("", alert_type_l),
        (colour_n, ""),
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


def _pretty_title_for_social(title: str) -> str:
    """
    Convert 'Snow Squall Warning (Tay Township area)' -> 'Snow Squall Warning in Tay Township'
    """
    t = (title or "").strip()
    t = re.sub(r"\s*\(.*?\)\s*$", "", t).strip()
    return f"{t} in Tay Township"


def _issued_short(issued: str) -> str:
    """
    Convert 'Issued: 5:07 PM EST Sunday 4 January 2026'
    -> 'Issued Jan 4 5:07p'
    Fallback: return original issued if parsing fails.
    """
    s = (issued or "").strip()
    s2 = re.sub(r"^Issued:\s*", "", s, flags=re.IGNORECASE).strip()

    m = re.search(
        r"(?P<h>\d{1,2}):(?P<min>\d{2})\s*(?P<ampm>AM|PM)\b.*?\b(?P<day>\d{1,2})\s+(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)\s+(?P<year>\d{4})",
        s2,
        flags=re.IGNORECASE,
    )
    if not m:
        return s  # fallback

    h = int(m.group("h"))
    minute = m.group("min")
    ampm = m.group("ampm").lower()
    day = int(m.group("day"))
    month = m.group("month").strip().capitalize()[:3]

    suffix = "a" if ampm.startswith("a") else "p"
    return f"Issued {month} {day} {h}:{minute}{suffix}"


def _extract_details_lines_from_ec(official_url: str) -> List[str]:
    """
    Prefer What/When blocks, but return clean detail lines with no labels.
    Output lines like:
      ['Additional local snowfall amounts up to 30 cm.',
       'Continuing tonight. Weakening on Saturday morning.']
    Falls back to 1 meaningful weather sentence if no What/When found.
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
        "visibility", "blowing", "drifting", "thunder", "heat", "cold"
    )
    sentences = re.split(r"(?<=[.!?])\s+", text)
    for s in sentences[:120]:
        s = s.strip()
        if 25 <= len(s) <= 220 and any(k in s.lower() for k in weather_keywords):
            return [s.rstrip(".") + "."]
    return []


def build_x_post_text(entry: Dict[str, Any], care: str = "") -> str:
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
        details_lines = []

    issued_short = _issued_short(issued_raw)
    more_url = get_more_info_url()

    parts: List[str] = []
    parts.append(title_line)
    parts.append("")  # blank line
    parts.extend(details_lines[:2])  # 1–2 lines
    parts.append("")  # blank line
    parts.append(f"More: {more_url}")
    parts.append(f"{issued_short} #TayTownship #ONStorm")

    text = "\n".join([p for p in parts if p is not None])

    # Hard 280-char safety: trim details first
    if len(text) <= 280:
        return text

    # Drop second detail line
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

    # Drop details entirely
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


def build_facebook_post_text(entry: Dict[str, Any], care: str = "") -> str:
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
        details_lines = []

    more_url = get_more_info_url()

    parts: List[str] = []
    parts.append(title_line)
    parts.append("")
    parts.extend(details_lines[:3])  # FB can take a bit more
    if care:
        parts.append("")
        parts.append(care.strip())
    parts.append("")
    parts.append(f"More: {more_url}")
    parts.append(f"{issued_short} #TayTownship #ONStorm")

    return "\n".join([p for p in parts if p is not None])


def build_rss_description_from_atom(entry: Dict[str, Any]) -> str:
    title = atom_title_for_tay((entry.get("title") or "").strip())
    issued = (entry.get("summary") or "").strip()
    official = (entry.get("link") or "").strip()
    bits = [title]
    if issued:
        bits.append(issued)
    bits.append(f"More info (Tay Township): {get_more_info_url()}")
    if official:
        bits.append(f"Official alert details: {official}")
    return "\n".join(bits)


# ----------------------------
# RSS helpers
# ----------------------------
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


MAX_RSS_ITEMS = 25


# ----------------------------
# Cooldown logic
# ----------------------------
def group_key_for_cooldown(area_name: str, kind: str) -> str:
    raw = f"{normalize(area_name)}|{normalize(kind)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def cooldown_allows_post(state: Dict[str, Any], area_name: str, kind: str = "alert") -> Tuple[bool, str]:
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


def mark_posted(state: Dict[str, Any], area_name: str, kind: str = "alert") -> None:
    now_ts = int(time.time())
    key = group_key_for_cooldown(area_name, kind)
    state.setdefault("cooldowns", {})
    state["cooldowns"][key] = now_ts
    state["global_last_post_ts"] = now_ts


# ----------------------------
# Ontario 511 camera resolver
# ----------------------------
_ON511_CAMERAS_CACHE: Optional[List[Dict[str, Any]]] = None


def is_image_url(url: str) -> bool:
    """Does URL respond with Content-Type image/*?"""
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
    """Resolve up to two image URLs (north, south) with fallbacks."""
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


# ----------------------------
# X OAuth2 (posting) helpers
# ----------------------------
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


# ----------------------------
# X media upload helpers (OAuth 1.0a)
# ----------------------------

# ----------------------------
# On511 camera "bug" overlay (stamp in lower-right corner)
# - Applies only to Ontario 511 camera images (CR29 URLs)
# - If anything fails, returns the original image bytes so posting still works
# ----------------------------
def apply_on511_bug(image_bytes: bytes, mime_type: str) -> Tuple[bytes, str]:
    """
    Adds the On511/Tay 'bug' in the lower-right corner for Ontario 511 camera images.
    """
    BUG_RELATIVE_WIDTH = 0.07
    BUG_MIN_WIDTH_PX   = 35
    BUG_OPACITY_ALPHA  = 80
    BUG_PAD_RELATIVE   = 0.015

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
            scale = BUG_OPACITY_ALPHA / 255.0
            alpha = alpha.point(lambda p: int(p * scale))
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
    """
    Downloads an image URL and returns (bytes, mime_type).
    Applies the On511 bug overlay for Ontario 511 camera captures.
    """
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

    return download_image_bytes(ref)


fb.load_image_bytes = fb_load_image_bytes


def x_upload_media(image_url: str) -> str:
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

    img_bytes, mime_type = download_image_bytes(image_url)

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


def post_to_x(text: str, image_urls: Optional[List[str]] = None) -> Dict[str, Any]:
    url = "https://api.x.com/2/tweets"
    access_token = get_oauth2_access_token()

    payload: Dict[str, Any] = {"text": text}

    image_urls = [u for u in (image_urls or []) if (u or "").strip()]
    if image_urls:
        media_ids: List[str] = []
        for u in image_urls[:4]:
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


# ----------------------------
# Facebook Page posting helpers
# ----------------------------
def post_to_facebook_page(message: str) -> Dict[str, Any]:
    page_id = os.getenv("FB_PAGE_ID", "").strip()
    page_token = os.getenv("FB_PAGE_ACCESS_TOKEN", "").strip()
    if not page_id or not page_token:
        raise RuntimeError("Missing FB_PAGE_ID or FB_PAGE_ACCESS_TOKEN")

    url = f"https://graph.facebook.com/v24.0/{page_id}/feed"
    r = requests.post(url, data={"message": message, "access_token": page_token}, timeout=30)
    print("FB POST /feed status:", r.status_code)
    if r.status_code >= 400:
        raise RuntimeError(f"Facebook feed post failed {r.status_code}")
    return r.json()


def post_photo_to_facebook_page(caption: str, image_url: str) -> Dict[str, Any]:
    page_id = os.getenv("FB_PAGE_ID", "").strip()
    page_token = os.getenv("FB_PAGE_ACCESS_TOKEN", "").strip()
    if not page_id or not page_token:
        raise RuntimeError("Missing FB_PAGE_ID or FB_PAGE_ACCESS_TOKEN")
    if not image_url:
        raise RuntimeError("Missing image_url for FB photo post")

    url = f"https://graph.facebook.com/v24.0/{page_id}/photos"
    r = requests.post(
        url,
        data={
            "url": image_url,
            "caption": caption,
            "access_token": page_token,
        },
        timeout=30,
    )
    print("FB POST /photos status:", r.status_code)
    if r.status_code >= 400:
        raise RuntimeError(f"Facebook photo post failed {r.status_code}")
    return r.json()


def post_carousel_to_facebook_page(caption: str, image_urls: List[str]) -> Dict[str, Any]:
    """Posts up to 10 images as a single carousel post."""
    image_urls = [u for u in (image_urls or []) if (u or "").strip()]

    if not image_urls:
        return post_to_facebook_page(caption)

    if len(image_urls) == 1:
        return post_photo_to_facebook_page(caption, image_urls[0])

    page_id = os.getenv("FB_PAGE_ID", "").strip()
    page_token = os.getenv("FB_PAGE_ACCESS_TOKEN", "").strip()
    if not page_id or not page_token:
        raise RuntimeError("Missing FB_PAGE_ID or FB_PAGE_ACCESS_TOKEN")

    media_fbids: List[str] = []

    for u in image_urls[:10]:
        try:
            url = f"https://graph.facebook.com/v24.0/{page_id}/photos"
            r = requests.post(
                url,
                data={
                    "url": u,
                    "published": "false",
                    "access_token": page_token,
                },
                timeout=30,
            )
            if r.status_code >= 400:
                print(f"⚠️ FB carousel upload failed for one image: {r.status_code}")
                continue
            j = r.json()
            fbid = j.get("id")
            if fbid:
                media_fbids.append(str(fbid))
        except Exception as e:
            print(f"⚠️ FB carousel upload skipped for one image: {e}")

    if not media_fbids:
        return post_to_facebook_page(caption)

    if len(media_fbids) == 1:
        return post_photo_to_facebook_page(caption, image_urls[0])

    data: Dict[str, Any] = {"message": caption, "access_token": page_token}
    for i, fbid in enumerate(media_fbids):
        data[f"attached_media[{i}]"] = json.dumps({"media_fbid": fbid})

    feed_url = f"https://graph.facebook.com/v24.0/{page_id}/feed"
    r = requests.post(feed_url, data=data, timeout=30)
    print("FB POST /feed (carousel) status:", r.status_code)
    if r.status_code >= 400:
        raise RuntimeError(f"Facebook carousel post failed {r.status_code}")

    return r.json()


# ----------------------------
# Main
# ----------------------------
def classify_alert_kind(title: str) -> str:
    t = (title or "").lower()
    if "warning" in t:
        return "warning"
    if "watch" in t:
        return "watch"
    return "other"


def main() -> None:
    # Clean up any previous rotated token file
    if os.path.exists(ROTATED_X_REFRESH_TOKEN_PATH):
        try:
            os.remove(ROTATED_X_REFRESH_TOKEN_PATH)
        except Exception:
            pass

    camera_image_urls = resolve_cr29_image_urls()

    # =========================================================
    # Manual test mode (bypasses alerts + cooldown/dedupe)
    # =========================================================
    if TEST_X or TEST_FACEBOOK:
        base = "Testing the validity of the post — please ignore ✅"

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
                token = hashlib.sha1(
                    f"test:{dt.datetime.utcnow().isoformat()}".encode("utf-8")
                ).hexdigest()[:10]
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
                image_urls=camera_image_urls,
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
            post_to_x(f"{base}\n\n(X)", image_urls=camera_image_urls)

        if ENABLE_FB_POSTING and TEST_FACEBOOK:
            fb_state = load_state()
            fb_state.pop("fb_cooldown_until", None)
            fb_state.pop("fb_last_posted_at", None)

            fb_images = materialize_images_for_facebook(camera_image_urls)
            try:
                fb_result = fb.safe_post_facebook(
                    fb_state,
                    caption=f"{base}\n\n(Facebook)",
                    image_urls=fb_images,
                    has_new_social_event=True,
                    state_path="state.json",
                )
                print("FB result:", fb_result)
            finally:
                cleanup_tmp_cam_files(fb_images)

        return

    # =========================================================
    # Normal mode (process real alerts)
    # =========================================================
    state = load_state()
    posted = set(state.get("posted_guids", []))
    posted_text_hashes = set(state.get("posted_text_hashes", []))

    tree, channel = load_rss_tree()

    new_rss_items = 0
    social_posted = 0
    social_skipped_cooldown = 0
    active_candidates = 0

    # Load care statements once (cheap + consistent)
    care_rows = []
    try:
        care_rows = load_care_statements_rows()
    except Exception as e:
        print(f"⚠️ CareStatements load failed (global): {e}")
        care_rows = []

    try:
        atom_entries = fetch_atom_entries(ALERT_FEED_URL)
    except Exception as e:
        print(f"⚠️ ATOM feed unavailable: {e}")
        print("Exiting cleanly; will retry on next scheduled run.")
        return

    for entry in atom_entries:
        guid = atom_entry_guid(entry)
        if not guid:
            continue

        title = atom_title_for_tay((entry.get("title") or "Weather alert").strip())
        pub_dt = entry.get("updated_dt") or dt.datetime.now(dt.timezone.utc)
        pub_date = email.utils.format_datetime(pub_dt)
        link = get_more_info_url()
        description = build_rss_description_from_atom(entry)

        title_l = (title or "").lower()
        summary_l = ((entry.get("summary") or "")).lower()
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
            print(f"Info: non-active / no-alert item in feed — skipping social post: {title}")
            posted.add(guid)
            continue

        active_candidates += 1

        if not rss_item_exists(channel, guid):
            add_rss_item(channel, title=title, link=link, guid=guid, pub_date=pub_date, description=description)
            new_rss_items += 1

        if guid in posted:
            continue

        allowed, reason = cooldown_allows_post(state, DISPLAY_AREA_NAME, kind="alert")
        if not allowed:
            social_skipped_cooldown += 1
            print("Social skipped:", reason)
            continue

        # ---- Care statement (FB only) ----
        type_label = title.split("(")[0].strip().lower()
        sev = severity_emoji(title)
        care = pick_care_statement(care_rows, sev, type_label) if care_rows else ""

        # ---- Build platform-specific texts ----
        x_text = build_x_post_text(entry)
        fb_text = build_facebook_post_text(entry, care=care)

        # Dedupe: use the X text as the "event signature" (stable + strict)
        h = text_hash(x_text)
        if h in posted_text_hashes:
            print("Social skipped: duplicate text hash already posted")
            posted.add(guid)
            continue

        print("Social preview (X):", x_text.replace("\n", " | "))

        # Wind warning rule you mentioned: don’t use highway cameras for wind warnings
        # (If you have a different exact rule, adjust the keyword match here.)
        img_urls = camera_image_urls
        if "wind warning" in (title or "").lower():
            img_urls = []

        # ================================
        # TELEGRAM PREVIEW + POLICY
        # ================================
        if TELEGRAM_ENABLE_GATE:
            alert_kind = classify_alert_kind(title)  # "warning" | "watch" | "other"
            token = hashlib.sha1(guid.encode("utf-8")).hexdigest()[:10]

            ingest_telegram_actions(state, save_state)
            maybe_send_reminders(state, save_state)

            preview_text = (
                f"🚨 {title}\n\n"
                f"X:\n{x_text}\n\n"
                f"Facebook:\n{fb_text}\n\n"
                f"Alert type: {alert_kind.upper()}\n\n"
                f"More information:\n{get_more_info_url()}"
            )
            ensure_preview_sent(
                state,
                save_state,
                token,
                preview_text,
                kind=alert_kind,
                image_urls=img_urls,
            )

            d = decision_for(state, token)

            if alert_kind in ("watch", "other"):
                if d == "denied":
                    print(f"Telegram: denied ({alert_kind}). Skipping.")
                    continue

                if d != "approved":
                    if is_expired(state, token):
                        print(f"Telegram: expired ({alert_kind}). Skipping.")
                        continue

                    wait_seconds = int(os.getenv("TELEGRAM_WAIT_SECONDS", "600"))
                    print(f"Telegram: pending ({alert_kind}). Waiting up to {wait_seconds}s for approval.")
                    d = wait_for_decision(
                        state,
                        save_state,
                        token,
                        max_wait_seconds=wait_seconds,
                        poll_interval_seconds=4,
                    )

                    if d == "denied":
                        print(f"Telegram: denied ({alert_kind}) after wait. Skipping.")
                        continue
                    if d != "approved":
                        print(f"Telegram: still pending ({alert_kind}) after wait. Skipping this run.")
                        continue

            elif alert_kind == "warning":
                if d == "denied":
                    print("Telegram: denied (warning). Skipping.")
                    continue

                if not warning_delay_elapsed(state, token):
                    print("Telegram: warning preview delay not elapsed yet. Skipping this run.")
                    continue

        # ================================
        # END TELEGRAM POLICY
        # ================================
        posted_this = False

        if ENABLE_X_POSTING:
            post_to_x(x_text, image_urls=img_urls)
            posted_this = True

        if ENABLE_FB_POSTING:
            fb_images = materialize_images_for_facebook(img_urls)
            try:
                fb_result = fb.safe_post_facebook(
                    state,
                    caption=fb_text,
                    image_urls=fb_images,
                    has_new_social_event=True,
                    state_path="state.json",
                )
                print("FB result:", fb_result)
                posted_this = True
            finally:
                cleanup_tmp_cam_files(fb_images)

        if posted_this:
            posted.add(guid)
            posted_text_hashes.add(h)
            state["posted_guids"] = list(posted)
            state["posted_text_hashes"] = list(posted_text_hashes)
            save_state(state)
            social_posted += 1

    try:
        tree.write(RSS_PATH, encoding="utf-8", xml_declaration=True)
    except Exception as e:
        print(f"⚠️ Failed writing RSS to {RSS_PATH}: {e}")

    state["posted_guids"] = list(posted)
    state["posted_text_hashes"] = list(posted_text_hashes)
    save_state(state)

    if active_candidates == 0:
        print("Info: no active weather alerts — nothing to post.")


if __name__ == "__main__":
    main()
