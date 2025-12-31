# tay_weather_bot.py
#
# Tay Township Weather Bot
# - Pulls Environment Canada alerts from regional ATOM feed (source of truth)
# - Writes RSS feed: tay-weather.xml
# - Posts to X automatically using OAuth 2.0 (refresh token)
# - Posts to Facebook Page automatically (Page access token)
# - Supports cooldowns + dedupe
#
# REQUIRED GitHub Secrets:
#   X_CLIENT_ID
#   X_CLIENT_SECRET
#   X_REFRESH_TOKEN
#   FB_PAGE_ID
#   FB_PAGE_ACCESS_TOKEN
#
# OPTIONAL workflow env vars:
#   ENABLE_X_POSTING=true|false
#   ENABLE_FB_POSTING=true|false
#   TEST_TWEET=true
#   ALERT_FEED_URL=<ATOM feed url>
#   TAY_COORDS_URL=<coords link>
#
import base64
import json
import os
import re
import time
import hashlib
import datetime as dt
import email.utils
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, List, Tuple

import requests


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

USER_AGENT = "tay-weather-rss-bot/1.0"

# Public “more info” URL (Tay coords format)
TAY_COORDS_URL = os.getenv(
    "TAY_COORDS_URL",
    "https://weather.gc.ca/en/location/index.html?coords=44.751,-79.768",
).strip()
MORE_INFO_URL = TAY_COORDS_URL

ALERT_FEED_URL = os.getenv("ALERT_FEED_URL", "https://weather.gc.ca/rss/battleboard/onrm94_e.xml").strip()
DISPLAY_AREA_NAME = "Tay Township area"

# When X rotates refresh tokens, we write the newest value here
ROTATED_X_REFRESH_TOKEN_PATH = "x_refresh_token_rotated.txt"


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
    """Stable short hash used to dedupe near-identical social posts across runs."""
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
# ATOM (regional alert feed) helpers
# ----------------------------
ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}


def _parse_atom_dt(s: str) -> dt.datetime:
    # Example: 2025-12-30T23:43:20Z
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
    """Replace forecast-region wording with Tay Township wording for public posts."""
    if not title:
        return title
    t = title.replace(", Midland - Coldwater - Orr Lake", f" ({DISPLAY_AREA_NAME})")
    t = t.replace("Midland - Coldwater - Orr Lake", DISPLAY_AREA_NAME)
    return t


def atom_entry_guid(entry: Dict[str, Any]) -> str:
    return (entry.get("id") or entry.get("link") or entry.get("title") or "").strip()


def build_social_text_from_atom(entry: Dict[str, Any]) -> str:
    title = atom_title_for_tay((entry.get("title") or "").strip())
    issued = (entry.get("summary") or "").strip()

    parts = [f"⚠️ {title}"]
    if issued:
        parts.append(issued)
    parts.append(f"More: {MORE_INFO_URL}")
    parts.append("#TayTownship #ONStorm")

    text = " | ".join([p for p in parts if p])
    return text if len(text) <= 280 else (text[:277].rstrip() + "…")


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
# X (OAuth 2.0) helpers
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
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    r = requests.post("https://api.x.com/2/oauth2/token", headers=headers, data=data, timeout=30)
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


def post_to_x(text: str) -> Dict[str, Any]:
    url = "https://api.x.com/2/tweets"
    access_token = get_oauth2_access_token()
    r = requests.post(
        url,
        json={"text": text},
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
            detail = ""

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

    missing = [k for k, v in [
        ("FB_PAGE_ID", page_id),
        ("FB_PAGE_ACCESS_TOKEN", page_token),
    ] if not v]
    if missing:
        raise RuntimeError(f"Missing required FB env vars: {', '.join(missing)}")

    url = f"https://graph.facebook.com/v24.0/{page_id}/feed"
    r = requests.post(
        url,
        data={"message": message, "access_token": page_token},
        timeout=30,
    )

    print("FB POST /feed status:", r.status_code)

    if r.status_code >= 400:
        print("FB error body:", r.text)  # 👈 THIS is the key line
        raise RuntimeError(f"Facebook post failed {r.status_code}")

    return r.json()

# ----------------------------
# Main
# ----------------------------
def main() -> None:
    print("ENABLE_FB_POSTING =", ENABLE_FB_POSTING)
    print("FB_PAGE_ID set =", bool(os.getenv("FB_PAGE_ID")))
    print("FB_PAGE_ACCESS_TOKEN set =", bool(os.getenv("FB_PAGE_ACCESS_TOKEN")))

    # Clean up any previous rotated token file
    if os.path.exists(ROTATED_X_REFRESH_TOKEN_PATH):
        try:
            os.remove(ROTATED_X_REFRESH_TOKEN_PATH)
        except Exception:
            pass

    if TEST_TWEET:
        text = "Test post from Tay weather bot ✅"
        print("TEST_TWEET enabled.")
        if ENABLE_X_POSTING:
            post_to_x(text)
        if ENABLE_FB_POSTING:
            try:
                post_to_facebook_page(text)
            except RuntimeError as e:
                print(f"Facebook skipped: {e}")
        return

    state = load_state()
    posted = set(state.get("posted_guids", []))
    posted_text_hashes = set(state.get("posted_text_hashes", []))

    tree, channel = load_rss_tree()

    new_rss_items = 0
    social_posted = 0
    social_skipped_cooldown = 0

    # Fetch regional ATOM alert feed
    try:
        atom_entries = fetch_atom_entries(ALERT_FEED_URL)
    except Exception as e:
        # Non-fatal: Weather Canada can time out or throttle occasionally.
        print(f"⚠️ ATOM feed unavailable: {e}")
        print("Exiting cleanly; will retry on next scheduled run.")
        return

    for entry in atom_entries:
        guid = atom_entry_guid(entry)
        if not guid:
            continue

        # RSS item
        title = atom_title_for_tay((entry.get("title") or "Weather alert").strip())
        pub_dt = entry.get("updated_dt") or dt.datetime.now(dt.timezone.utc)
        pub_date = email.utils.format_datetime(pub_dt)
        link = MORE_INFO_URL
        description = build_rss_description_from_atom(entry)

        if not rss_item_exists(channel, guid):
            add_rss_item(channel, title=title, link=link, guid=guid, pub_date=pub_date, description=description)
            new_rss_items += 1

        # Social posting (dedupe + cooldown)
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
                post_to_x(social_text)
                posted_anywhere = True
            except RuntimeError as e:
                if str(e) == "X_DUPLICATE_TWEET":
                    print("X rejected duplicate tweet text; skipping.")
                else:
                    raise

        if ENABLE_FB_POSTING:
            try:
                post_to_facebook_page(social_text)
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

    # Update lastBuildDate
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
