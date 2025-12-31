# tay_weather_bot.py
#
# Tay Township Weather Bot
# - Pulls Environment Canada CAP alerts from Datamart
# - Filters to Tay-area regions (strict allow-list match on CAP <areaDesc>)
# - Writes RSS feed: tay-weather.xml
# - Posts to X automatically using OAuth 2.0 (refresh token)
# - Posts to Facebook Page automatically (Page access token)
# - Supports cooldowns + dedupe + "all clear" follow-up for Cancel messages
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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


# ----------------------------
# Feature toggles
# ----------------------------
INCLUDE_SPECIAL_WEATHER_STATEMENTS = True
INCLUDE_ALERTS = True
STRICT_AREA_MATCH = True

ENABLE_X_POSTING = os.getenv("ENABLE_X_POSTING", "false").lower() == "true"
ENABLE_FB_POSTING = os.getenv("ENABLE_FB_POSTING", "false").lower() == "true"
TEST_TWEET = os.getenv("TEST_TWEET", "false").lower() == "true"


# ----------------------------
# Exclusions
# ----------------------------
EXCLUDED_EVENTS = {
    "test",
    "alert ready test",
    "broadcast intrusion",
}


# ----------------------------
# Tay / target areas (exact CAP <areaDesc> strings)
# ----------------------------
AREA_ALLOWLIST = [
    # Land (Tay region)
    "Midland - Coldwater - Orr Lake",

    # Marine (optional)
    "Southern Georgian Bay",
]

# CAP Datamart offices
OFFICES = ["CWTO"]  # Ontario Storm Prediction Centre

# Look-back window (hours)
HOURS_BACK_TO_SCAN = 12

# RSS retention
MAX_RSS_ITEMS = 25

# Paths
STATE_PATH = "state.json"
RSS_PATH = "tay-weather.xml"

USER_AGENT = "tay-weather-rss-bot/1.0"

# Stable public “more info” URL (avoid CAP link 404s)
MORE_INFO_URL = "https://weather.gc.ca/en/location/index.html?coords=44.751,-79.768"
ALERT_FEED_URL = "https://weather.gc.ca/rss/battleboard/onrm94_e.xml"  # Midland–Coldwater–Orr Lake alert feed
DISPLAY_AREA_NAME = "Tay Township area"
COMMUNITY_COORDS = {
    "Tay Township": (44.751, -79.768),
    "Waubaushene": (44.754, -79.710),
    "Victoria Harbour": (44.751, -79.768),
    "Port McNicoll": (44.749, -79.811),
}

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
# Social templates
# ----------------------------
TWEET_TEMPLATES = {
    "alert": (
        "⚠️ {event_label} for {areas_short}\n"
        "{headline}\n"
        "{advice}\n"
        "More: {more_info}\n"
        "#TayTownship #ONStorm"
    ),
    "statement": (
        "🌦️ Special Weather Statement for {areas_short}\n"
        "{headline}\n"
        "{advice}\n"
        "More: {more_info}\n"
        "#TayTownship"
    ),
    "allclear": (
        "✅ All clear: {event_label} ended for {areas_short}\n"
        "Continue to use caution as conditions may still be hazardous.\n"
        "Details: {more_info}\n"
        "#TayTownship"
    ),
}


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


def load_state() -> dict:
    default = {"seen_ids": [], "posted_guids": [], "cooldowns": {}, "global_last_post_ts": 0}
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
    data.setdefault("cooldowns", {})
    data.setdefault("global_last_post_ts", 0)
    return data


def save_state(state: dict) -> None:
    state["seen_ids"] = state.get("seen_ids", [])[-5000:]
    state["posted_guids"] = state.get("posted_guids", [])[-5000:]

    cds = state.get("cooldowns", {})
    if isinstance(cds, dict) and len(cds) > 5000:
        items = sorted(cds.items(), key=lambda kv: kv[1], reverse=True)[:4000]
        state["cooldowns"] = dict(items)

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def utc_dirs_to_check(hours_back: int):
    n = now_utc()
    for h in range(hours_back, -1, -1):
        t = n - dt.timedelta(hours=h)
        yield t.strftime("%Y%m%d"), t.strftime("%H")


def list_cap_files(directory_url: str) -> List[str]:
    r = requests.get(directory_url, headers={"User-Agent": USER_AGENT}, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".cap"):
            out.append(urljoin(directory_url.rstrip("/") + "/", href))
    return sorted(set(out))


def find_text(elem: Optional[ET.Element], tag_name: str) -> str:
    if elem is None:
        return ""
    found = elem.find(f".//{{*}}{tag_name}")
    return found.text.strip() if (found is not None and found.text) else ""


def pick_info_block(root: ET.Element) -> Optional[ET.Element]:
    infos = root.findall(".//{*}info")
    if not infos:
        return None
    for info in infos:
        lang = find_text(info, "language")
        if normalize(lang).startswith("en"):
            return info
    return infos[0]



# ----------------------------
# Regional ATOM feed helpers
# ----------------------------
ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}

def fetch_atom_entries(feed_url: str) -> List[Dict[str, Any]]:
    """Fetch and parse a Weather.gc.ca regional ATOM feed. Returns newest-first entries."""
    r = requests.get(feed_url, headers={"User-Agent": USER_AGENT}, timeout=20)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    entries: List[Dict[str, Any]] = []
    for e in root.findall("a:entry", ATOM_NS):
        def t(tag: str) -> str:
            el = e.find(f"a:{tag}", ATOM_NS)
            return (el.text or "").strip() if el is not None else ""
        link_el = e.find("a:link", ATOM_NS)
        link = link_el.attrib.get("href", "").strip() if link_el is not None else ""
        entries.append({
            "id": t("id"),
            "updated": t("updated"),
            "title": t("title"),
            "summary": t("summary"),
            "link": link,
        })
    # Newest first by updated
    def key(ent: Dict[str, Any]):
        try:
            return datetime.fromisoformat(ent.get("updated", "").replace("Z", "+00:00"))
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
    entries.sort(key=key, reverse=True)
    return entries

def atom_entry_to_alert(ent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert an ATOM entry into a minimal 'alert' dict that reuses the CAP-style pipeline.
    """
    title = (ent.get("title") or "").strip()
    summary = (ent.get("summary") or "").strip()
    updated = (ent.get("updated") or "").strip()
    alert_id = (ent.get("id") or title or updated).strip()

    # Example title: "ORANGE WARNING - SNOW SQUALL, Midland - Coldwater - Orr Lake"
    event = ""
    headline = title
    if " - " in title:
        # Split "LEVEL TYPE - EVENT, Region"
        _, rest = title.split(" - ", 1)
        # Event is before comma
        event = rest.split(",", 1)[0].strip()
    if not event:
        event = title.split(",", 1)[0].strip()

    return {
        "identifier": alert_id,
        "from_atom": True,
        "sent": updated,
        "event": event,
        "headline": headline,
        "summary": summary,
        "areaDesc": DISPLAY_AREA_NAME,
        "more_info": MORE_INFO_URL,
        "source_link": ent.get("link") or "",
    }

def parse_cap(xml_text: str) -> Dict[str, Any]:
    root = ET.fromstring(xml_text)

    identifier = find_text(root, "identifier")
    sent = find_text(root, "sent")

    info = pick_info_block(root)
    event = find_text(info, "event")
    headline = find_text(info, "headline")
    description = find_text(info, "description")
    instruction = find_text(info, "instruction")

    msg_type = find_text(info, "msgType")
    severity = find_text(info, "severity")
    urgency = find_text(info, "urgency")
    certainty = find_text(info, "certainty")

    areas = []
    if info is not None:
        for area in info.findall(".//{*}area"):
            ad = find_text(area, "areaDesc")
            if ad:
                areas.append(ad)

    return {
        "identifier": identifier,
        "sent": sent,
        "event": event,
        "headline": headline,
        "description": description,
        "instruction": instruction,
        "areas": areas,
        "msg_type": msg_type,
        "severity": severity,
        "urgency": urgency,
        "certainty": certainty,
    }


def should_include_event(cap: Dict[str, Any]) -> bool:
    event = normalize(cap.get("event", ""))
    headline = normalize(cap.get("headline", ""))

    if not event and not headline:
        return False
    if any(bad in event for bad in EXCLUDED_EVENTS) or any(bad in headline for bad in EXCLUDED_EVENTS):
        return False

    is_sws = (event == "special weather statement")
    if is_sws and INCLUDE_SPECIAL_WEATHER_STATEMENTS:
        return True
    if (not is_sws) and INCLUDE_ALERTS:
        return True
    return False


def primary_allowlisted_area(cap: Dict[str, Any]) -> str:
    """
    Pick the first CAP areaDesc that matches our allowlist.
    This prevents “Uxbridge …” showing up when Midland is also included.
    """
    areas = cap.get("areas", []) or []
    allow_norm = [normalize(a) for a in AREA_ALLOWLIST]
    for a in areas:
        if normalize(a) in allow_norm:
            return a.strip()
    return ""


def area_matches(cap: Dict[str, Any]) -> bool:
    # If we are using the regional ATOM feed, the feed itself is already scoped to our region.
    if cap.get("from_atom"):
        return True
    areas = cap.get("areas", []) or []
    if STRICT_AREA_MATCH and not areas:
        return False
    return bool(primary_allowlisted_area(cap))


def rfc2822_date_from_sent(sent: str) -> str:
    if sent:
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
            try:
                if fmt.endswith("%z"):
                    d = dt.datetime.strptime(sent, fmt)
                else:
                    d = dt.datetime.strptime(sent, fmt).replace(tzinfo=dt.timezone.utc)
                return email.utils.format_datetime(d)
            except Exception:
                pass
    return email.utils.format_datetime(now_utc())


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


def build_rss_description(cap: Dict[str, Any]) -> str:
    bits = []
    area = primary_allowlisted_area(cap) or ((cap.get("areas") or [""])[0]).strip()
    if area:
        bits.append(f"Area: {area}")

    if cap.get("event"):
        bits.append(f"Event: {cap['event'].strip()}")

    if cap.get("headline"):
        bits.append(cap["headline"].strip())

    if cap.get("description"):
        bits.append(cap["description"].strip())

    if cap.get("instruction"):
        bits.append("Advice: " + cap["instruction"].strip())

    bits.append(f"More info: {MORE_INFO_URL}")

    text = "\n\n".join(bits).strip()
    if len(text) > 2000:
        text = text[:2000].rstrip() + "…"
    return text


# ----------------------------
# Cooldown logic
# ----------------------------
def classify_event_kind(cap: Dict[str, Any]) -> str:
    event = normalize(cap.get("event", ""))
    headline = normalize(cap.get("headline", ""))

    if event == "special weather statement" or "special weather statement" in headline:
        return "statement"
    if "warning" in headline or "warning" in event:
        return "warning"
    if "watch" in headline or "watch" in event:
        return "watch"
    if "advisory" in headline or "advisory" in event:
        return "advisory"
    return "alert"


def is_all_clear(cap: Dict[str, Any]) -> bool:
    msg_type = normalize(cap.get("msg_type", ""))
    headline = normalize(cap.get("headline", ""))
    desc = normalize(cap.get("description", ""))

    if msg_type == "cancel":
        return True
    if "has ended" in headline or " ended" in headline:
        return True
    if "has ended" in desc:
        return True
    return False


def group_key_for_cooldown(cap: Dict[str, Any]) -> str:
    area_primary = primary_allowlisted_area(cap) or ""
    kind = "allclear" if is_all_clear(cap) else classify_event_kind(cap)
    raw = f"{normalize(area_primary)}|{kind}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def get_cooldown_minutes_for(cap: Dict[str, Any]) -> int:
    if is_all_clear(cap):
        return COOLDOWN_MINUTES["allclear"]
    kind = classify_event_kind(cap)
    return COOLDOWN_MINUTES.get(kind, COOLDOWN_MINUTES["default"])


def cooldown_allows_post(state: Dict[str, Any], cap: Dict[str, Any]) -> Tuple[bool, str]:
    now_ts = int(time.time())

    last_global = safe_int(state.get("global_last_post_ts", 0), 0)
    if last_global and (now_ts - last_global) < (GLOBAL_COOLDOWN_MINUTES * 60):
        return False, f"Global cooldown active ({GLOBAL_COOLDOWN_MINUTES}m)."

    key = group_key_for_cooldown(cap)
    cooldowns = state.get("cooldowns", {}) if isinstance(state.get("cooldowns"), dict) else {}
    last_ts = safe_int(cooldowns.get(key, 0), 0)

    mins = get_cooldown_minutes_for(cap)
    if last_ts and (now_ts - last_ts) < (mins * 60):
        return False, f"Cooldown active for group ({mins}m)."

    return True, "OK"


def mark_posted(state: Dict[str, Any], cap: Dict[str, Any]) -> None:
    now_ts = int(time.time())
    key = group_key_for_cooldown(cap)
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
    """
    Uses refresh token to mint a short-lived access token.
    If X returns a new refresh token, write it to ROTATED_X_REFRESH_TOKEN_PATH
    so the workflow can update GitHub Secrets automatically.
    """
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
    print("X token refresh:", r.status_code, r.text[:400])
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
    print("X POST /2/tweets:", r.status_code, r.text[:500])
    if r.status_code >= 400:
        raise RuntimeError(f"X post failed {r.status_code}: {r.text}")
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
    r = requests.post(url, data={"message": message, "access_token": page_token}, timeout=30)
    print("FB POST /feed:", r.status_code, r.text[:500])
    if r.status_code >= 400:
        raise RuntimeError(f"Facebook post failed {r.status_code}: {r.text}")
    return r.json()


def build_areas_short(cap: Dict[str, Any]) -> str:
    area = primary_allowlisted_area(cap)
    if not area:
        return "Tay Township area"
    s = area.strip()
    if len(s) > 70:
        s = s[:67].rstrip() + "…"
    return s


def extract_advice_short(cap: Dict[str, Any]) -> str:
    inst = (cap.get("instruction") or "").strip()
    if inst:
        return inst if len(inst) <= 120 else (inst[:117].rstrip() + "…")

    desc = (cap.get("description") or "").strip()
    if not desc:
        return "Take precautions and monitor conditions."

    parts = re.split(r"(?<=[.!?])\s+", desc)
    first = (parts[0] if parts else desc).strip()
    if len(first) > 120:
        first = first[:117].rstrip() + "…"
    return first


def build_social_text(cap: Dict[str, Any]) -> str:
    areas_short = build_areas_short(cap)
    headline = (cap.get("headline") or "").strip() or (cap.get("event") or "Weather alert").strip()
    advice = extract_advice_short(cap)
    event_label = (cap.get("event") or "Weather alert").strip()

    if is_all_clear(cap):
        template = TWEET_TEMPLATES["allclear"]
        text = template.format(event_label=event_label, areas_short=areas_short, more_info=MORE_INFO_URL)
    else:
        kind = classify_event_kind(cap)
        template = TWEET_TEMPLATES["statement"] if kind == "statement" else TWEET_TEMPLATES["alert"]
        text = template.format(
            event_label=event_label,
            areas_short=areas_short,
            headline=headline,
            advice=advice,
            more_info=MORE_INFO_URL,
        )

    if len(text) > 280:
        text = text[:277].rstrip() + "…"
    return text


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

    if TEST_TWEET:
        text = "Test post from Tay weather bot ✅"
        print("TEST_TWEET enabled.")
        if ENABLE_X_POSTING:
            post_to_x(text)
        if ENABLE_FB_POSTING:
            post_to_facebook_page(text)
        return

    state = load_state()
    seen = set(state.get("seen_ids", []))
    posted = set(state.get("posted_guids", []))

    tree, channel = load_rss_tree()

    new_rss_items = 0
    social_posted = 0
    social_skipped_cooldown = 0

    # Pull alerts from the regional Weather.gc.ca ATOM feed (no CAP directory probing).
    try:
        atom_entries = fetch_atom_entries(ALERT_FEED_URL)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch ATOM feed: {e}")

    for ent in atom_entries:
        cap = atom_entry_to_alert(ent)
        cap_id = (cap.get("identifier") or "").strip()
        if not cap_id:
            continue

        if cap_id in seen:
            continue
        seen.add(cap_id)

        if not should_include_event(cap):
            continue
        if not area_matches(cap):
            continue

        title = (cap.get("headline") or cap.get("event") or "Weather alert").strip()
        pub_date = rfc2822_date_from_sent(cap.get("sent", ""))
        guid = cap_id

        # IMPORTANT: stable link (avoid CAP link 404)
        link = MORE_INFO_URL
        description = build_rss_description(cap)

        if not rss_item_exists(channel, guid):
            add_rss_item(channel, title=title, link=link, guid=guid, pub_date=pub_date, description=description)
            new_rss_items += 1

        # Social posting (dedupe + cooldown)
        if guid in posted:
            continue

        allowed, reason = cooldown_allows_post(state, cap)
        if not allowed:
            social_skipped_cooldown += 1
            print("Social skipped:", reason)
            continue

        social_text = build_social_text(cap)
        print("Social preview:", social_text.replace("\n", " | "))
        print("Matched allowlisted area:", primary_allowlisted_area(cap))

        if ENABLE_X_POSTING:
            post_to_x(social_text)
        if ENABLE_FB_POSTING:
            post_to_facebook_page(social_text)

        social_posted += 1
        posted.add(guid)
        mark_posted(state, cap)

    # Update lastBuildDate
    lbd = channel.find("lastBuildDate")
    if lbd is None:
        lbd = ET.SubElement(channel, "lastBuildDate")
    lbd.text = email.utils.format_datetime(now_utc())

    trim_rss_items(channel, MAX_RSS_ITEMS)

    tree.write(RSS_PATH, encoding="utf-8", xml_declaration=True)
    state["seen_ids"] = list(seen)
    state["posted_guids"] = list(posted)
    save_state(state)

    print(
        "Run summary:",
        f"new_rss_items_added={new_rss_items}",
        f"social_posted={social_posted}",
        f"social_skipped_cooldown={social_skipped_cooldown}",
    )


if __name__ == "__main__":
    main()
