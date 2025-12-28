import json
import os
import re
import datetime as dt
import email.utils
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests_oauthlib import OAuth1

INCLUDE_SPECIAL_WEATHER_STATEMENTS = True
INCLUDE_ALERTS = True  # warnings/watches/advisories etc.

EXCLUDED_EVENTS = {
    "test",
    "alert ready test",
    "broadcast intrusion",
}

# Cooldowns (recommended)
COOLDOWN_MINUTES_ALERTS = 45
COOLDOWN_MINUTES_SWS = 120

# X posting
ENABLE_X_POSTING = False
MAX_TWEETS_PER_DAY = 15  # Free tier safety (under ~17/day)

X_TEMPLATE_ALERT = (
    "⚠️ Weather Canada {event}\n"
    "Area: {area}\n"
    "{headline}\n"
    "Use caution and plan ahead.\n"
    "Details: {url}\n"
    "#ONwx #TayTownship"
)

X_TEMPLATE_SWS = (
    "Weather Canada Special Weather Statement\n"
    "Area: {area}\n"
    "{headline}\n"
    "Stay aware of changing conditions.\n"
    "Details: {url}\n"
    "#ONwx #TayTownship"
)

# ----------------------------
# Settings (Tay hamlets)
# ----------------------------
AREA_KEYWORDS = [
    "Victoria Harbour",
    "Port McNicoll",
    "Waubaushene",
    "Waverley",
    # Common alert region name that often covers Tay-area communities:
    "Midland - Coldwater - Orr Lake",
]

OFFICES = ["CWTO"]  # Ontario Storm Prediction Centre
HOURS_BACK_TO_SCAN = 6  # look back a few hours to avoid missing items
MAX_RSS_ITEMS = 25  # keep RSS tidy
STATE_PATH = "state.json"
RSS_PATH = "tay-weather.xml"
USER_AGENT = "tay-weather-rss-bot/1.0"

# Optional: include a stable “more info” link in descriptions
MORE_INFO_URL = "https://weather.gc.ca/warnings/index_e.html?prov=on"


# ----------------------------
# Helpers
# ----------------------------
def normalize(s: str) -> str:
    """Normalise punctuation/spacing to improve matching."""
    if not s:
        return ""
    s = s.lower()
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def load_state() -> dict:
    """
    Load state.json safely.
    If missing/invalid, reset to defaults.
    """
    default = {"seen_ids": [], "cooldowns": {}, "posted_guids": [], "daily_counts": {}}

    if not os.path.exists(STATE_PATH):
        return default

    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return default

            if "seen_ids" not in data or not isinstance(data["seen_ids"], list):
                data["seen_ids"] = []
            if "cooldowns" not in data or not isinstance(data["cooldowns"], dict):
                data["cooldowns"] = {}
            if "posted_guids" not in data or not isinstance(data["posted_guids"], list):
                data["posted_guids"] = []
            if "daily_counts" not in data or not isinstance(data["daily_counts"], dict):
                data["daily_counts"] = {}

            return data
    except Exception:
        return default


def save_state(state: dict) -> None:
    # Prevent endless growth
    state["seen_ids"] = state.get("seen_ids", [])[-5000:]
    state["posted_guids"] = state.get("posted_guids", [])[-5000:]

    # Keep cooldowns from growing forever: retain only last 7 days
    cooldowns = state.get("cooldowns", {})
    if isinstance(cooldowns, dict) and cooldowns:
        now = dt.datetime.now(dt.timezone.utc).timestamp()
        seven_days = 7 * 24 * 60 * 60
        state["cooldowns"] = {
            k: v
            for k, v in cooldowns.items()
            if isinstance(v, (int, float)) and (now - float(v)) <= seven_days
        }
    else:
        state["cooldowns"] = {}

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def utc_dirs_to_check(hours_back: int):
    now = dt.datetime.utcnow()
    for h in range(hours_back, -1, -1):
        t = now - dt.timedelta(hours=h)
        yield t.strftime("%Y%m%d"), t.strftime("%H")


def list_cap_files(directory_url: str) -> list[str]:
    """
    The Datamart directory returns an HTML listing.
    We scrape links ending in .cap
    """
    r = requests.get(directory_url, headers={"User-Agent": USER_AGENT}, timeout=20)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".cap"):
            out.append(urljoin(directory_url.rstrip("/") + "/", href))
    return sorted(set(out))


def find_text(elem, tag_name: str) -> str:
    """
    Find first descendant whose tag endswith tag_name regardless of namespace.
    """
    if elem is None:
        return ""
    found = elem.find(f".//{{*}}{tag_name}")
    return found.text.strip() if (found is not None and found.text) else ""


def pick_info_block(root: ET.Element) -> ET.Element | None:
    """
    Prefer English info block if present.
    CAP can have multiple <info> blocks (en/fr).
    """
    infos = root.findall(".//{*}info")
    if not infos:
        return None

    # Try to find an English one
    for info in infos:
        lang = find_text(info, "language")
        if normalize(lang).startswith("en"):
            return info

    # Otherwise use first
    return infos[0]


def parse_cap(xml_text: str) -> dict:
    root = ET.fromstring(xml_text)

    identifier = find_text(root, "identifier")
    sent = find_text(root, "sent")

    info = pick_info_block(root)
    event = find_text(info, "event")
    headline = find_text(info, "headline")
    description = find_text(info, "description")
    instruction = find_text(info, "instruction")

    # Areas: possibly multiple
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
    }


def should_include_event(cap: dict) -> bool:
    event = normalize(cap.get("event", ""))

    if not event:
        return False

    # Skip tests
    if any(bad in event for bad in EXCLUDED_EVENTS):
        return False

    is_sws_event = (event == "special weather statement")

    if is_sws_event and INCLUDE_SPECIAL_WEATHER_STATEMENTS:
        return True

    # “Alerts” = everything else (warnings/watches/advisories etc.)
    if (not is_sws_event) and INCLUDE_ALERTS:
        return True

    return False


def area_matches(cap: dict) -> bool:
    hay = normalize(" | ".join(cap.get("areas", [])))
    # If areas are empty, fall back to headline/description text
    fallback = normalize((cap.get("headline") or "") + " " + (cap.get("description") or ""))

    for kw in AREA_KEYWORDS:
        nkw = normalize(kw)
        if nkw and (nkw in hay or nkw in fallback):
            return True
    return False


def rfc2822_date_from_sent(sent: str) -> str:
    """
    CAP 'sent' is ISO-ish. Convert best-effort to RFC 2822 for RSS.
    If parsing fails, use current UTC.
    """
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

    return email.utils.format_datetime(dt.datetime.now(dt.timezone.utc))


def parse_sent_to_epoch(sent: str) -> float:
    """
    Convert CAP 'sent' to epoch seconds (UTC). Best-effort.
    """
    if sent:
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
            try:
                if fmt.endswith("%z"):
                    d = dt.datetime.strptime(sent, fmt)
                    return d.timestamp()
                else:
                    d = dt.datetime.strptime(sent, fmt).replace(tzinfo=dt.timezone.utc)
                    return d.timestamp()
            except Exception:
                pass
    return dt.datetime.now(dt.timezone.utc).timestamp()


def cooldown_key(cap: dict) -> str:
    """
    Key by event + primary area. Prevents spam on bulletin updates.
    """
    event = normalize(cap.get("event", ""))
    areas = cap.get("areas", []) or []
    primary_area = normalize(areas[0]) if areas else ""
    if not primary_area:
        primary_area = "tay-area"
    return f"{event}|{primary_area}"


def cooldown_minutes_for(cap: dict) -> int:
    """
    Use different cooldowns for SWS vs alerts.
    """
    event = normalize(cap.get("event", ""))
    if event == "special weather statement":
        return COOLDOWN_MINUTES_SWS
    return COOLDOWN_MINUTES_ALERTS


def ensure_rss_exists() -> None:
    if os.path.exists(RSS_PATH):
        return

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = "Tay Township Weather Statements"
    ET.SubElement(channel, "link").text = "https://weatherpresenter.github.io/tay-weather-rss/"
    ET.SubElement(channel, "description").text = (
        "Automated Weather Canada alerts and Special Weather Statements for Victoria Harbour, "
        "Port McNicoll, Waubaushene, and Waverley."
    )
    ET.SubElement(channel, "language").text = "en-ca"

    tree = ET.ElementTree(rss)
    tree.write(RSS_PATH, encoding="utf-8", xml_declaration=True)


def load_rss_tree() -> tuple[ET.ElementTree, ET.Element]:
    ensure_rss_exists()
    tree = ET.parse(RSS_PATH)
    root = tree.getroot()
    channel = root.find("channel")
    if channel is None:
        raise RuntimeError("RSS file missing <channel> element")
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


def build_description(cap: dict) -> str:
    bits = []

    areas = cap.get("areas", [])
    if areas:
        bits.append(f"Area: {areas[0]}")

    if cap.get("description"):
        bits.append(cap["description"].strip())

    if cap.get("instruction"):
        bits.append("Advice: " + cap["instruction"].strip())

    bits.append(f"More info: {MORE_INFO_URL}")

    text = "\n\n".join(bits).strip()
    if len(text) > 1200:
        text = text[:1200].rstrip() + "…"
    return text


# ----------------------------
# X / Tweet helpers
# ----------------------------
def x_trim(text: str, limit: int = 280) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def is_sws(cap: dict) -> bool:
    return normalize(cap.get("event", "")) == "special weather statement"


def format_x_post(cap: dict, url: str) -> str:
    event = (cap.get("event") or "").strip() or "Weather update"
    headline = (cap.get("headline") or "").strip()
    areas = cap.get("areas") or []
    area = areas[0].strip() if areas else "Tay area"

    tmpl = X_TEMPLATE_SWS if is_sws(cap) else X_TEMPLATE_ALERT
    text = tmpl.format(event=event, headline=headline, area=area, url=url)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return x_trim(text, 280)


def post_to_x(text: str) -> str:
    api_key = os.environ.get("X_API_KEY", "")
    api_secret = os.environ.get("X_API_SECRET", "")
    access_token = os.environ.get("X_ACCESS_TOKEN", "")
    access_secret = os.environ.get("X_ACCESS_TOKEN_SECRET", "")

    if not all([api_key, api_secret, access_token, access_secret]):
        raise RuntimeError("Missing X credentials in environment variables.")

    auth = OAuth1(api_key, api_secret, access_token, access_secret)

    r = requests.post(
        "https://api.x.com/2/tweets",
        auth=auth,
        json={"text": text},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    return data.get("data", {}).get("id", "")


def today_key_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")


def can_tweet_today(state: dict) -> bool:
    daily = state.get("daily_counts", {})
    if not isinstance(daily, dict):
        daily = {}
        state["daily_counts"] = daily

    k = today_key_utc()
    count = int(daily.get(k, 0))
    return count < MAX_TWEETS_PER_DAY


def increment_tweet_today(state: dict) -> None:
    daily = state.get("daily_counts", {})
    if not isinstance(daily, dict):
        daily = {}
        state["daily_counts"] = daily

    k = today_key_utc()
    daily[k] = int(daily.get(k, 0)) + 1


def main():
    state = load_state()
    seen = set(state.get("seen_ids", []))

    cooldowns = state.get("cooldowns", {})
    if not isinstance(cooldowns, dict):
        cooldowns = {}

    posted_guids = set(state.get("posted_guids", []))

    tree, channel = load_rss_tree()

    new_items = 0
    new_tweets = 0

    for yyyymmdd, hh in utc_dirs_to_check(HOURS_BACK_TO_SCAN):
        for office in OFFICES:
            directory_url = f"https://dd.weather.gc.ca/today/alerts/cap/{yyyymmdd}/{office}/{hh}/"

            try:
                cap_urls = list_cap_files(directory_url)
            except Exception:
                continue

            for cap_url in cap_urls:
                try:
                    xml_text = requests.get(
                        cap_url, headers={"User-Agent": USER_AGENT}, timeout=20
                    ).text
                    cap = parse_cap(xml_text)
                except Exception:
                    continue

                cap_id = cap.get("identifier", "").strip()
                if not cap_id or cap_id in seen:
                    continue

                # Mark as seen so we don't keep reprocessing it
                seen.add(cap_id)

                if not should_include_event(cap):
                    continue

                if not area_matches(cap):
                    continue

                # Cooldown check
                key = cooldown_key(cap)
                now_ts = parse_sent_to_epoch(cap.get("sent", ""))
                last_ts = cooldowns.get(key)
                cooldown_mins = cooldown_minutes_for(cap)

                if isinstance(last_ts, (int, float)):
                    minutes_since = (now_ts - float(last_ts)) / 60.0
                    if minutes_since < cooldown_mins:
                        continue

                # Build RSS fields
                title = cap.get("headline") or cap.get("event") or "Weather update"
                pub_date = rfc2822_date_from_sent(cap.get("sent", ""))
                guid = cap_id
                link = cap_url  # RSS item link points to CAP file
                description = build_description(cap)

                if not rss_item_exists(channel, guid):
                    add_rss_item(
                        channel,
                        title=title,
                        link=link,
                        guid=guid,
                        pub_date=pub_date,
                        description=description,
                    )
                    new_items += 1
                    cooldowns[key] = now_ts

                    # Post to X once per GUID, subject to daily cap
                    if ENABLE_X_POSTING and guid not in posted_guids and can_tweet_today(state):
                        try:
                            x_text = format_x_post(cap, MORE_INFO_URL)
                            _tweet_id = post_to_x(x_text)
                            posted_guids.add(guid)
                            increment_tweet_today(state)
                            new_tweets += 1
                        except Exception as e:
                            # Don't crash the whole run if X fails; RSS still updates
                            print(f"X post failed: {e}")

    # Update lastBuildDate
    now_rfc = email.utils.format_datetime(dt.datetime.now(dt.timezone.utc))
    lbd = channel.find("lastBuildDate")
    if lbd is None:
        lbd = ET.SubElement(channel, "lastBuildDate")
    lbd.text = now_rfc

    trim_rss_items(channel, MAX_RSS_ITEMS)

    # Save RSS + state
    tree.write(RSS_PATH, encoding="utf-8", xml_declaration=True)
    state["seen_ids"] = list(seen)
    state["cooldowns"] = cooldowns
    state["posted_guids"] = list(posted_guids)
    save_state(state)

    print(f"Done. Added {new_items} new RSS item(s). Posted {new_tweets} tweet(s).")


if __name__ == "__main__":
    main()
    
