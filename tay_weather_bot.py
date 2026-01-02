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

USER_AGENT = "tay-weather-rss-bot/1.1"

# Public “more info” URL
# Prefer your GitHub Pages Tay page (tay/index.html), fall back to WeatherCAN coords page
TAY_COORDS_URL = os.getenv(
    "TAY_COORDS_URL",
    "https://weather.gc.ca/en/location/index.html?coords=44.751,-79.768",
).strip()

TAY_ALERTS_URL = os.getenv("TAY_ALERTS_URL", "").strip()  # e.g. https://<your-pages-site>/tay/
MORE_INFO_URL = TAY_ALERTS_URL or TAY_COORDS_URL

ALERT_FEED_URL = os.getenv("ALERT_FEED_URL", "https://weather.gc.ca/rss/battleboard/onrm94_e.xml").strip()
DISPLAY_AREA_NAME = "Tay Township area"

# Ontario 511 cameras API
ON511_CAMERAS_API = "https://511on.ca/api/v2/get/cameras"
ON511_CAMERA_KEYWORD = os.getenv("ON511_CAMERA_KEYWORD", "CR-29").strip() or "CR-29"

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


def build_social_text_from_atom(entry: Dict[str, Any]) -> str:
    title_raw = (entry.get("title") or "").strip()
    summary_raw = (entry.get("summary") or "").strip()

    # ----------------------------
    # 1) Colour circle from EC colour-coded prefix (YELLOW/ORANGE/RED)
    # ----------------------------
    t_low = title_raw.lower()
    if t_low.startswith("red "):
        circle = "🔴"
    elif t_low.startswith("orange "):
        circle = "🟠"
    else:
        circle = "🟡"

    # Remove ONLY the colour word, keep Weather Canada event wording (Warning/Watch/Advisory)
    title = re.sub(r"^(yellow|orange|red)\s+", "", title_raw, flags=re.I).strip()

    # Replace forecast-region wording with Tay Township (no parentheses, no “area”)
    title = title.replace(", Midland - Coldwater - Orr Lake", " in Tay Township")
    title = title.replace("Midland - Coldwater - Orr Lake", "Tay Township")

    
    # Conversational opener, but keep the official event wording in the body
    event_for_header = re.sub(r"\bWarning\b|\bWatch\b|\bAdvisory\b", "conditions", title, flags=re.I).strip()
    header = f"{circle} {event_for_header}"
    if "tay township" not in header.lower():
        header = f"{circle} {event_for_header} in Tay Township"

    # ----------------------------
    # 2) Use Weather Canada phrasing from summary, but avoid duplicated issued/timing lines
    # ----------------------------
    summary = summary_raw

    # Remove any leading "Issued at ..." line if present
    summary = re.sub(r"^\s*issued\s+at.*?(\n+|$)", "", summary, flags=re.I)

    # Pull first “timing” sentence out so it can be its own line (no WHAT/WHEN labels)
    timing = ""

    # Prefer splitting by lines first (EC summaries often use line breaks)
    chunks: List[str] = []

    # Protect a.m./p.m. so we don’t split inside them
    protected = re.sub(
        r"\b([ap])\.m\.\b",
        lambda m: f"{m.group(1).upper()}M_TOKEN",
        summary,
        flags=re.I,
)

    for line in protected.splitlines():
        line = line.strip()
        if not line:
            continue

    # Split into sentences on periods, then restore a.m./p.m.
    parts = [p.strip() for p in line.split(".") if p.strip()]
    for p in parts:
        p = p.replace("AM_TOKEN", "a.m.").replace("PM_TOKEN", "p.m.")
        chunks.append(p)

sentences = chunks



    keep: List[str] = []
    for s in sentences:
        s_low = s.lower()
        if not timing and (
            "conditions are expected" in s_low
            or "expected" in s_low and ("conditions" in s_low or "starting" in s_low or "ending" in s_low or "continuing" in s_low)
            or "this evening" in s_low
            or "tonight" in s_low
            or "today" in s_low
            or "overnight" in s_low
            or "into" in s_low and ("morning" in s_low or "afternoon" in s_low or "evening" in s_low)
        ):
            timing = s.strip()
            continue
        keep.append(s.strip())

    # Keep the first 2–3 sentences for impacts so it stays social-friendly
    impacts = ". ".join(keep[:3]).strip()
    if impacts and not impacts.endswith("."):
        impacts += "."

    if timing and not timing.endswith("."):
        timing += "."

    # ----------------------------
    # 3) Issued time at the end (Canadian style, no Oxford commas)
    # ----------------------------
    issued_dt = entry.get("updated_dt")
    issued_str = ""
    if isinstance(issued_dt, dt.datetime):
        # Example: "Issued Jan 1, 6:58 p.m."
        issued_str = issued_dt.strftime("Issued %b %-d, %-I:%M %p")
        issued_str = issued_str.replace("AM", "a.m.").replace("PM", "p.m.")

    care = "Please take care, avoid unnecessary travel and check on neighbours who may need support."

    parts = [header, ""]
    if impacts:
        parts.append(impacts)
    if timing:
        parts.extend(["", timing])
    parts.extend(["", care, "", f"More information: {MORE_INFO_URL}"])
    if issued_str:
        parts.extend(["", issued_str])
    parts.append("#TayTownship #ONStorm")

    text = "\n".join([p for p in parts if p]).strip()

    # X hard limit
    if len(text) > 280:
        text = text[:277].rstrip() + "…"

    return text

def build_rss_description_from_atom(entry: Dict[str, Any]) -> str:
    title = atom_title_for_tay((entry.get("title") or "").strip())
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

def download_image_bytes(image_url: str) -> Tuple[bytes, str]:
    image_url = (image_url or "").strip()
    if not image_url:
        raise RuntimeError("No image_url provided")

    r = requests.get(image_url, headers={"User-Agent": USER_AGENT}, timeout=(10, 30), allow_redirects=True)
    r.raise_for_status()

    content_type = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if not content_type.startswith("image/"):
        raise RuntimeError(f"URL did not return an image. Content-Type={content_type}")

    return r.content, content_type


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

def main() -> None:
    # Clean up any previous rotated token file
    if os.path.exists(ROTATED_X_REFRESH_TOKEN_PATH):
        try:
            os.remove(ROTATED_X_REFRESH_TOKEN_PATH)
        except Exception:
            pass

    camera_image_urls = resolve_cr29_image_urls()

    if TEST_TWEET:
        text = "Test post from Tay weather bot ✅"
        if ENABLE_X_POSTING:
            post_to_x(text, image_urls=camera_image_urls)
        if ENABLE_FB_POSTING:
            post_carousel_to_facebook_page(text, camera_image_urls)
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

        title = atom_title_for_tay((entry.get("title") or "Weather alert").strip())
        pub_dt = entry.get("updated_dt") or dt.datetime.now(dt.timezone.utc)
        pub_date = email.utils.format_datetime(pub_dt)
        link = MORE_INFO_URL
        description = build_rss_description_from_atom(entry)

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

        social_text = build_social_text_from_atom(entry)
        h = text_hash(social_text)

        if h in posted_text_hashes:
            print("Social skipped: duplicate text hash already posted")
            posted.add(guid)
            continue


        print("Social preview:", social_text.replace("\n", " "))

        posted_anywhere = False

        if ENABLE_X_POSTING:
            try:
                post_to_x(social_text, image_urls=camera_image_urls)
                posted_anywhere = True
            except RuntimeError as e:
                if str(e) == "X_DUPLICATE_TWEET":
                    print("X rejected duplicate tweet text; skipping.")
                else:
                    raise

        if ENABLE_FB_POSTING:
            try:
                post_carousel_to_facebook_page(social_text, camera_image_urls)
                posted_anywhere = True
            except RuntimeError as e:
                print(f"Facebook skipped: {e}")

        if posted_anywhere:
            social_posted += 1
            posted.add(guid)
            posted_text_hashes.add(h)
            mark_posted(state, DISPLAY_AREA_NAME, kind="alert")
        else:
            print("No social posts sent for this alert.")

    lbd = channel.find("lastBuildDate")
    if lbd is None:
        lbd = ET.SubElement(channel, "lastBuildDate")
    lbd.text = email.utils.format_datetime(now_utc())

    trim_rss_items(channel, MAX_RSS_ITEMS)
    tree.write(RSS_PATH, encoding="utf-8", xml_declaration=True)

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
