# tay_weather_bot.py
#
# Tay Township Weather Bot (v2.2)
# - Uses Environment Canada ATOM feed as the alert list (source of truth)
# - For each entry, fetches the EC HTML alert page and extracts from *page-source JSON*:
#     * issueTimeText -> issued_short
#     * "text" -> headline, What, When
# - Posts to X + Facebook (optional toggles)
# - X: hard 280 chars including spaces
# - Facebook: includes fuller care statement
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
from typing import Any, Dict, List, Optional, Tuple

import requests
from PIL import Image
from requests_oauthlib import OAuth1

# ----------------------------
# Feature toggles
# ----------------------------
ENABLE_X_POSTING = os.getenv("ENABLE_X_POSTING", "false").lower() == "true"
ENABLE_FB_POSTING = os.getenv("ENABLE_FB_POSTING", "false").lower() == "true"
TEST_TWEET = os.getenv("TEST_TWEET", "false").lower() == "true"

# ----------------------------
# Paths
# ----------------------------
STATE_PATH = "state.json"
RSS_PATH = "tay-weather.xml"
ROTATED_X_REFRESH_TOKEN_PATH = "x_refresh_token_rotated.txt"

# Watermark options (optional)
WATERMARK_ON511 = os.getenv("WATERMARK_ON511", "true").lower() == "true"
ON511_LOGO_PATH = os.getenv("ON511_LOGO_PATH", "assets/On511_logo.png").strip()
WATERMARK_OPACITY = float(os.getenv("WATERMARK_OPACITY", "0.35"))  # 0..1

USER_AGENT = "tay-weather-rss-bot/2.2"

# Public “more info” URL (prefer GitHub page)
TAY_COORDS_URL = os.getenv(
    "TAY_COORDS_URL",
    "https://weather.gc.ca/en/location/index.html?coords=44.751,-79.768",
).strip()
TAY_ALERTS_URL = os.getenv("TAY_ALERTS_URL", "").strip()
MORE_INFO_URL = (TAY_ALERTS_URL or TAY_COORDS_URL).strip()

ALERT_FEED_URL = os.getenv(
    "ALERT_FEED_URL", "https://weather.gc.ca/rss/battleboard/onrm94_e.xml"
).strip()

DISPLAY_AREA_NAME = "Tay Township area"

# Ontario 511 cameras API
ON511_CAMERAS_API = "https://511on.ca/api/v2/get/cameras"
ON511_CAMERA_KEYWORD = os.getenv("ON511_CAMERA_KEYWORD", "CR-29").strip() or "CR-29"

# How many "What" lines
X_WHAT_MAX = 2
FB_WHAT_MAX = 3

HASHTAGS = "#TayTownship #ONStorm"

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

MAX_RSS_ITEMS = 25

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

    data.setdefault("seen_ids", [])
    data.setdefault("posted_guids", [])
    data.setdefault("posted_text_hashes", [])
    data.setdefault("cooldowns", {})
    data.setdefault("global_last_post_ts", 0)
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


# ----------------------------
# Image helpers (ON511 watermark)
# ----------------------------
def overlay_on511_logo(img_bytes: bytes, logo_path: str) -> bytes:
    """Overlay ON511 logo bottom-right; return JPEG bytes."""
    with Image.open(BytesIO(img_bytes)).convert("RGBA") as base:
        with Image.open(logo_path).convert("RGBA") as logo:
            bw, bh = base.size

            target_w = max(40, int(bw * 0.06))
            scale = target_w / max(1, logo.size[0])
            target_h = max(1, int(logo.size[1] * scale))
            logo = logo.resize((target_w, target_h), Image.LANCZOS)

            if WATERMARK_OPACITY < 1.0:
                alpha = logo.split()[-1]
                opacity = max(0.0, min(1.0, WATERMARK_OPACITY))
                alpha = alpha.point(lambda p: int(p * opacity))
                logo.putalpha(alpha)

            pad = max(6, int(bw * 0.01))
            x = bw - logo.size[0] - pad
            y = bh - logo.size[1] - pad

            base.alpha_composite(logo, dest=(x, y))

            out = BytesIO()
            base.convert("RGB").save(out, format="JPEG", quality=92, optimize=True)
            return out.getvalue()


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


def atom_entry_guid(entry: Dict[str, Any]) -> str:
    return (entry.get("id") or entry.get("link") or entry.get("title") or "").strip()


def emoji_from_atom_title(title_raw: str) -> str:
    t = (title_raw or "").strip().lower()
    if t.startswith("red "):
        return "🔴"
    if t.startswith("orange "):
        return "🟠"
    return "🟡"


def clean_atom_event_name(title_raw: str) -> str:
    """
    From ATOM title like:
      "Yellow Snow Squall Warning, Midland - Coldwater - Orr Lake"
      "Yellow WARNING - SNOW SQUALL, Midland - Coldwater - Orr Lake"
    produce something sane for fallback line1.
    """
    t = (title_raw or "").strip()
    t = re.sub(r"^(yellow|orange|red)\s+", "", t, flags=re.I).strip()
    t = t.replace(", Midland - Coldwater - Orr Lake", "").strip()
    t = t.replace("Midland - Coldwater - Orr Lake", "").strip()

    t = re.sub(r"^(warning|watch|advisory|statement)\s*-\s*", "", t, flags=re.I).strip()

    if t.isupper():
        t = t.title()

    return t or "Weather alert"


# ----------------------------
# EC parsing helpers (page-source JSON first)
# ----------------------------
def _html_to_text(html: str) -> str:
    """Fallback: naive html -> text."""
    if not html:
        return ""
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    html = re.sub(r"(?i)</p\s*>", "\n", html)
    html = re.sub(r"(?i)</div\s*>", "\n", html)
    html = re.sub(r"(?i)</li\s*>", "\n", html)
    html = re.sub(r"(?is)<script.*?>.*?</script>", "", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", "", html)
    text = re.sub(r"(?s)<.*?>", "", html)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\r", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_json_string_value(html: str, key: str) -> str:
    """
    Find a JSON string value for a key like "text":"...."
    Returns decoded string, or "" if not found.
    """
    if not html or not key:
        return ""

    needle = f'"{key}":"'
    start = html.find(needle)
    if start == -1:
        return ""

    i = start + len(needle)
    out_chars: List[str] = []
    escaped = False

    while i < len(html):
        ch = html[i]
        if escaped:
            out_chars.append("\\" + ch)
            escaped = False
            i += 1
            continue

        if ch == "\\":
            escaped = True
            i += 1
            continue

        if ch == '"':
            raw_escaped = "".join(out_chars)
            try:
                return json.loads(f'"{raw_escaped}"')
            except Exception:
                return ""
        else:
            out_chars.append(ch)
            i += 1

    return ""


def _issued_short_from_issue_time_text(issue_time_text: str) -> Tuple[str, str]:
    """
    issue_time_text example:
      "6:41 p.m. EST Friday 2 January 2026"
    returns (issued_raw, issued_short)
    """
    issued_raw = (issue_time_text or "").strip()
    if not issued_raw:
        return "", ""

    issued_short = ""
    m = re.search(
        r"(\d{1,2}):(\d{2})\s*(a\.m\.|p\.m\.)\s*EST\s+\w+\s+(\d{1,2})\s+(\w+)\s+(\d{4})",
        issued_raw,
        flags=re.I,
    )
    if m:
        hh = int(m.group(1))
        mm = m.group(2)
        ap = m.group(3).lower()
        day = int(m.group(4))
        mon_name = m.group(5)

        mon_map = {
            "january": "Jan", "february": "Feb", "march": "Mar", "april": "Apr",
            "may": "May", "june": "Jun", "july": "Jul", "august": "Aug",
            "september": "Sep", "october": "Oct", "november": "Nov", "december": "Dec",
        }
        mon = mon_map.get(mon_name.strip().lower(), mon_name[:3].title())
        issued_short = f"Issued {mon} {day} {hh}:{mm}{'a' if 'a.m.' in ap else 'p'}"

    return issued_raw, issued_short


def _parse_alert_text_blocks(alert_text: str) -> Dict[str, Any]:
    """
    Parse EC alert narrative text (from JSON "text"):

      Snow squalls continue tonight.

      What:
      ...
      When:
      ...
      Where:
      ...
    """
    if not alert_text:
        return {"headline": "", "what_lines": [], "when_line": ""}

    t = alert_text.replace("\r\n", "\n").replace("\r", "\n").strip()

    headline = ""
    what_lines: List[str] = []
    when_line = ""

    if "What:" in t:
        before_what, after_what = t.split("What:", 1)
        headline_raw = before_what.strip()

        # headline: first non-empty line (EC usually puts the key sentence first)
        head_line = ""
        for ln in headline_raw.split("\n"):
            ln = ln.strip()
            if ln:
                head_line = ln
                break
        if head_line:
            headline = head_line if head_line.endswith(".") else head_line + "."

        # What block ends at When:
        if "When:" in after_what:
            what_part, after_when = after_what.split("When:", 1)
            when_part = after_when
        else:
            what_part, when_part = after_what, ""

        # Collect What lines as non-empty lines, skipping section headers
        skip = {"what:", "when:", "where:", "additional information:", "additional info:"}
        for ln in what_part.split("\n"):
            ln = ln.strip()
            if not ln:
                continue
            if ln.lower() in skip:
                continue
            what_lines.append(ln)

        # When line: first meaningful line after When:
        when_part = when_part.strip()
        if when_part:
            for ln in when_part.split("\n"):
                ln = ln.strip()
                if not ln:
                    continue
                if ln.lower() in skip:
                    continue
                when_line = ln if ln.endswith(".") else ln + "."
                break
    else:
        # No What/When structure — just take first non-empty line as headline
        for ln in t.split("\n"):
            ln = ln.strip()
            if ln:
                headline = ln if ln.endswith(".") else ln + "."
                break

    return {"headline": headline, "what_lines": what_lines, "when_line": when_line}


def fetch_ec_page_details(url: str) -> Dict[str, Any]:
    """
    Preferred:
      - Parse from embedded JSON in the HTML (same content visible in "view page source")
        * "text" contains narrative with What/When
        * "issueTimeText" contains issued timestamp
    Fallback:
      - crude HTML-to-text (should still not crash; may return empty What/When)
    """
    if not url:
        return {}

    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=(8, 25))
    r.raise_for_status()
    html = r.text

    # JSON-first
    alert_text = _extract_json_string_value(html, "text")
    issue_time_text = _extract_json_string_value(html, "issueTimeText")

    # fallback if missing
    if not alert_text:
        alert_text = _html_to_text(html)

    blocks = _parse_alert_text_blocks(alert_text)

    issued_raw, issued_short = _issued_short_from_issue_time_text(issue_time_text)

    # very last resort issued regex (only if issueTimeText missing)
    if not issued_raw:
        txt = _html_to_text(html)
        m = re.search(
            r"\b(\d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.)\s*EST\s+\w+\s+\d{1,2}\s+\w+\s+\d{4})\b",
            txt,
            flags=re.I,
        )
        if m:
            issued_raw, issued_short = _issued_short_from_issue_time_text(m.group(1))

    return {
        "issued_raw": issued_raw,
        "issued_short": issued_short,
        "headline": blocks["headline"],
        "what_lines": blocks["what_lines"],
        "when_line": blocks["when_line"],
    }


# ----------------------------
# Social text builders
# ----------------------------
def build_x_text(event_name: str, emoji: str, details: Dict[str, Any]) -> str:
    """
    X:
    - Line 1 must end with "in Tay Township."
    - Must TRY to include: What (up to 2), When, care, More, Issued, hashtags
    - Hard limit 280 chars incl spaces
    """
    headline = (details.get("headline") or "").strip()
    what_lines = [w.strip() for w in (details.get("what_lines") or []) if (w or "").strip()]
    when_line = (details.get("when_line") or "").strip()
    issued_short = (details.get("issued_short") or "").strip()

    care_long = "Please take care, travel only if needed and check on neighbours who may need support."
    care_mid = "Please take care, travel only if needed and check on neighbours who may need support."
    care_short = "Please take care, travel only if needed."

    if headline:
        h = headline[:-1] if headline.endswith(".") else headline
        line1 = f"{emoji} - {h} in Tay Township."
    else:
        line1 = f"{emoji} - {event_name} in Tay Township."

    chosen_what = what_lines[:X_WHAT_MAX]

    def compose(
        what_count: int,
        include_when: bool,
        care: str,
        include_issued: bool,
        include_tags: bool,
    ) -> str:
        lines: List[str] = [line1]
        if what_count > 0:
            lines.extend(chosen_what[:what_count])
        if include_when and when_line:
            lines.append(when_line)
        if care:
            lines.append(care)
        lines.append(f"More: {MORE_INFO_URL}")
        if include_issued and issued_short:
            lines.append(issued_short)
        if include_tags and HASHTAGS:
            lines.append(HASHTAGS)
        return "\n".join([ln for ln in lines if ln]).strip()

    candidates = [
        compose(2, True, care_long, True, True),
        compose(2, True, care_mid, False, True),     # drop issued
        compose(2, True, care_short, False, True),   # shorter care
        compose(1, True, care_short, False, True),   # drop 2nd what
        compose(1, True, "", False, True),           # drop care
        compose(1, True, "", False, False),          # drop hashtags
        compose(0, True, "", False, False),          # when only
        compose(0, False, "", False, False),         # absolute minimum
    ]

    for t in candidates:
        if len(t) <= 280:
            return t

    return line1[:280]


def build_fb_text(event_name: str, emoji: str, details: Dict[str, Any]) -> str:
    """
    Facebook:
    - Line 1 must end with "in Tay Township."
    - Include: What (up to 3), When, care, More, Issued, hashtags
    """
    headline = (details.get("headline") or "").strip()
    what_lines = [w.strip() for w in (details.get("what_lines") or []) if (w or "").strip()]
    when_line = (details.get("when_line") or "").strip()
    issued_short = (details.get("issued_short") or "").strip()

    care = (
        "If you can, please stay off the roads and give crews room to work. "
        "If you must go out, slow down, leave extra space and keep your lights on. "
        "Please check on neighbours who may need help staying warm or getting supplies."
    )

    if headline:
        h = headline[:-1] if headline.endswith(".") else headline
        line1 = f"{emoji} - {h} in Tay Township."
    else:
        line1 = f"{emoji} - {event_name} in Tay Township."

    lines: List[str] = [line1]
    lines.extend(what_lines[:FB_WHAT_MAX])

    if when_line:
        lines.append(when_line)

    lines.append(care)
    lines.append(f"More: {MORE_INFO_URL}")

    if issued_short:
        lines.append(issued_short)

    if HASHTAGS:
        lines.append(HASHTAGS)

    return "\n".join([ln for ln in lines if ln]).strip()


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


def add_rss_item(
    channel: ET.Element,
    title: str,
    link: str,
    guid: str,
    pub_date: str,
    description: str,
) -> None:
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


def build_rss_description_from_atom(entry: Dict[str, Any]) -> str:
    title = (entry.get("title") or "").strip()
    issued = (entry.get("summary") or "").strip()
    official = (entry.get("link") or "").strip()
    bits = [title]
    if issued:
        bits.append(issued)
    bits.append(f"More info (Tay Township): {MORE_INFO_URL}")
    if official:
        bits.append(f"Official alert details: {official}")
    return "\n".join(bits)


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
    if r.status_code >= 400:
        try:
            print("X token refresh error body:", r.text)
        except Exception:
            pass
        raise RuntimeError("X_TOKEN_REFRESH_FAILED")

    payload = r.json()
    access = payload.get("access_token")
    if not access:
        print("X token refresh missing access_token:", payload)
        raise RuntimeError("X_TOKEN_REFRESH_FAILED")

    new_refresh = payload.get("refresh_token")
    if new_refresh and new_refresh != refresh_token:
        print("⚠️ X refresh token rotated. Workflow will update the repo secret.")
        write_rotated_refresh_token(new_refresh)

    if payload.get("scope"):
        print("X OAuth2 scopes:", payload.get("scope"))

    return access


# ----------------------------
# X media upload helpers (OAuth 1.0a)
# ----------------------------
def download_image_bytes(image_url: str) -> Tuple[bytes, str]:
    image_url = (image_url or "").strip()
    if not image_url:
        raise RuntimeError("No image_url provided")

    r = requests.get(image_url, headers={"User-Agent": USER_AGENT}, timeout=(10, 30), allow_redirects=True)
    r.raise_for_status()

    content_type = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if not content_type.startswith("image/"):
        raise RuntimeError(f"URL did not return an image. Content-Type={content_type}")

    img = r.content

    if WATERMARK_ON511 and os.path.exists(ON511_LOGO_PATH):
        try:
            img = overlay_on511_logo(img, ON511_LOGO_PATH)
            content_type = "image/jpeg"
        except Exception as e:
            print(f"⚠️ Watermark skipped: {e}")

    return img, content_type


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
        raise RuntimeError(f"X media upload failed {r.status_code} {r.text}")

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

        raise RuntimeError(f"X post failed {r.status_code} {r.text}")

    return r.json()


# ----------------------------
# Facebook posting helpers
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
        raise RuntimeError(f"Facebook feed post failed {r.status_code} {r.text}")
    return r.json()


def post_photo_to_facebook_page(caption: str, image_url: str) -> Dict[str, Any]:
    page_id = os.getenv("FB_PAGE_ID", "").strip()
    page_token = os.getenv("FB_PAGE_ACCESS_TOKEN", "").strip()
    if not page_id or not page_token:
        raise RuntimeError("Missing FB_PAGE_ID or FB_PAGE_ACCESS_TOKEN")
    if not image_url:
        raise RuntimeError("Missing image_url for FB photo post")

    img_bytes, mime_type = download_image_bytes(image_url)

    url = f"https://graph.facebook.com/v24.0/{page_id}/photos"
    files = {"source": ("image.jpg", img_bytes, mime_type)}
    data = {"caption": caption, "access_token": page_token}

    r = requests.post(url, data=data, files=files, timeout=60)
    print("FB POST /photos (source) status:", r.status_code)
    if r.status_code >= 400:
        raise RuntimeError(f"Facebook photo post failed {r.status_code} {r.text}")
    return r.json()


def post_carousel_to_facebook_page(caption: str, image_urls: List[str]) -> Dict[str, Any]:
    image_urls = [u for u in (image_urls or []) if (u or "").strip()]
    if not image_urls:
        return post_to_facebook_page(caption)
    if len(image_urls) == 1:
        return post_photo_to_facebook_page(caption, image_urls[0])

    page_id = os.getenv("FB_PAGE_ID", "").strip()
    page_token = os.getenv("FB_PAGE_ACCESS_TOKEN", "").strip()
    if not page_id or not page_token:
        raise RuntimeError("Missing FB_PAGE_ID or FB_PAGE_ACCESS_TOKEN")

    upload_url = f"https://graph.facebook.com/v24.0/{page_id}/photos"

    media_fbids: List[str] = []
    for u in image_urls[:10]:
        try:
            img_bytes, mime_type = download_image_bytes(u)
            files = {"source": ("image.jpg", img_bytes, mime_type)}
            r = requests.post(
                upload_url,
                data={"published": "false", "access_token": page_token},
                files=files,
                timeout=60,
            )
            if r.status_code >= 400:
                print(f"⚠️ FB carousel upload failed for one image: {r.status_code} {r.text}")
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
        raise RuntimeError(f"Facebook carousel post failed {r.status_code} {r.text}")

    return r.json()


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    # clear rotated token marker if left over
    if os.path.exists(ROTATED_X_REFRESH_TOKEN_PATH):
        try:
            os.remove(ROTATED_X_REFRESH_TOKEN_PATH)
        except Exception:
            pass

    camera_image_urls = resolve_cr29_image_urls()

    if TEST_TWEET:
        text = "Test post from Tay weather bot ✅"
        if ENABLE_X_POSTING:
            try:
                post_to_x(text, image_urls=camera_image_urls)
            except Exception as e:
                print(f"⚠️ X test skipped: {e}")
        if ENABLE_FB_POSTING:
            try:
                post_carousel_to_facebook_page(text, camera_image_urls)
            except Exception as e:
                print(f"⚠️ FB test skipped: {e}")
        return

    state = load_state()
    posted = set(state.get("posted_guids", []))
    posted_text_hashes = set(state.get("posted_text_hashes", []))

    tree, channel = load_rss_tree()

    new_rss_items = 0
    social_posted = 0
    social_skipped_cooldown = 0

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

        # RSS update
        title = (entry.get("title") or "Weather alert").strip()
        pub_dt = entry.get("updated_dt") or dt.datetime.now(dt.timezone.utc)
        pub_date = email.utils.format_datetime(pub_dt)
        link = MORE_INFO_URL
        description = build_rss_description_from_atom(entry)

        if not rss_item_exists(channel, guid):
            add_rss_item(channel, title=title, link=link, guid=guid, pub_date=pub_date, description=description)
            new_rss_items += 1

        # Only social-post each GUID once
        if guid in posted:
            continue

        allowed, reason = cooldown_allows_post(state, DISPLAY_AREA_NAME, kind="alert")
        if not allowed:
            social_skipped_cooldown += 1
            print("Social skipped:", reason)
            continue

        emoji = emoji_from_atom_title(entry.get("title") or "")
        event_name = clean_atom_event_name(entry.get("title") or "")

        try:
            details = fetch_ec_page_details((entry.get("link") or "").strip())
        except Exception as e:
            print(f"⚠️ EC page parse failed, falling back to ATOM-only: {e}")
            details = {}

        x_text = build_x_text(event_name, emoji, details)
        fb_text = build_fb_text(event_name, emoji, details)

        # Dedupe using what we actually post to X (most restrictive)
        h = text_hash(x_text)
        if h in posted_text_hashes:
            print("Social skipped: duplicate text hash already posted")
            posted.add(guid)
            continue

        print("X preview:", x_text.replace("\n", " | "))
        print("FB preview:", fb_text.replace("\n", " | "))

        posted_anywhere = False

        if ENABLE_X_POSTING:
            try:
                post_to_x(x_text, image_urls=camera_image_urls)
                posted_anywhere = True
            except RuntimeError as e:
                if str(e) == "X_DUPLICATE_TWEET":
                    print("X rejected duplicate tweet text, skipping.")
                else:
                    # IMPORTANT: don't crash the run; let FB + RSS still proceed
                    print(f"⚠️ X skipped: {e}")
            except Exception as e:
                print(f"⚠️ X unexpected error, skipped: {e}")

        if ENABLE_FB_POSTING:
            try:
                post_carousel_to_facebook_page(fb_text, camera_image_urls)
                posted_anywhere = True
            except RuntimeError as e:
                print(f"⚠️ Facebook skipped: {e}")
            except Exception as e:
                print(f"⚠️ Facebook unexpected error, skipped: {e}")

        if posted_anywhere:
            social_posted += 1
            posted.add(guid)
            posted_text_hashes.add(h)
            mark_posted(state, DISPLAY_AREA_NAME, kind="alert")
        else:
            print("No social posts sent for this alert.")

    # RSS housekeeping
    lbd = channel.find("lastBuildDate")
    if lbd is None:
        lbd = ET.SubElement(channel, "lastBuildDate")
    lbd.text = email.utils.format_datetime(now_utc())

    trim_rss_items(channel, MAX_RSS_ITEMS)
    tree.write(RSS_PATH, encoding="utf-8", xml_declaration=True)

    # Persist state
    state["posted_guids"] = list(posted)
    state["posted_text_hashes"] = list(posted_text_hashes)
    save_state(state)

    print(
        "Run summary:",
        f"new_rss_items_added={new_rss_items}",
        f"social_posted={social_posted}",
        f"social_skipped_cooldown={social_skipped_cooldown}",
    )


if __name__ == "__main__":
    main()
