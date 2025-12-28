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

# Kill switch (set ENABLE_X_POSTING="false" in workflow env to stop posting)
ENABLE_X_POSTING = os.environ.get("ENABLE_X_POSTING", "true").lower() == "true"

# X Free tier safety
MAX_TWEETS_PER_DAY = 15

# Severity cooldowns (recommended)
COOLDOWN_MINUTES_WARNING = 60
COOLDOWN_MINUTES_WATCH = 90
COOLDOWN_MINUTES_ADVISORY = 120
COOLDOWN_MINUTES_SWS = 180
COOLDOWN_MINUTES_CANCEL = 15  # allow quick all clear without repeats

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
HOURS_BACK_TO_SCAN = 6
MAX_RSS_ITEMS = 25
STATE_PATH = "state.json"
RSS_PATH = "tay-weather.xml"
USER_AGENT = "tay-weather-rss-bot/1.0"

MORE_INFO_URL = "https://weather.gc.ca/warnings/index_e.html?prov=on"


# ----------------------------
# Tweet templates
# ----------------------------
X_TEMPLATE_ALERT = (
    "⚠️ Weather Canada {event}\n"
    "Area: {area}\n"
    "{headline}\n"
    "Details: {url}\n"
    "{tags}"
)

X_TEMPLATE_SWS = (
    "Weather Canada Special Weather Statement\n"
    "Area: {area}\n"
    "{headline}\n"
    "Details: {url}\n"
    "{tags}"
)

X_TEMPLATE_ALL_CLEAR = (
    "✅ Weather update: All clear\n"
    "Area: {area}\n"
    "Previous: {event}\n"
    "Details: {url}\n"
    "{tags}"
)


# ----------------------------
# Helpers
# ----------------------------
def normalize(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def load_state() -> dict:
    default = {
        "seen_ids": [],
        "cooldowns": {},
        "posted_guids": [],
        "daily_counts": {},
        "posted_tweets": {},
        "active_alerts": {},
    }

    if not os.path.exists(STATE_PATH):
        return default

    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return default

            for k, v in default.items():
                if k not in data or not isinstance(data[k], type(v)):
                    data[k] = v

            return data
    except Exception:
        return default


def save_state(state: dict) -> None:
    state["seen_ids"] = state.get("seen_ids", [])[-5000:]
    state["posted_guids"] = state.get("posted_guids", [])[-5000:]

    # Trim posted_tweets to avoid unbounded growth
    posted_tweets = state.get("posted_tweets", {})
    if isinstance(posted_tweets, dict) and len(posted_tweets) > 5000:
        # Keep latest 5000 by insertion order (Python 3.7+ preserves insertion order)
        items = list(posted_tweets.items())[-5000:]
        state["posted_tweets"] = dict(items)

    # Keep cooldowns from growing forever (retain last 7 days)
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

    # Keep daily_counts tidy (retain last 30 days)
    daily_counts = state.get("daily_counts", {})
    if isinstance(daily_counts, dict) and daily_counts:
        today = dt.datetime.now(dt.timezone.utc)
        keep = {}
        for k, v in daily_counts.items():
            try:
                d = dt.datetime.strptime(k, "%Y%m%d").replace(tzinfo=dt.timezone.utc)
                if (today - d).days <= 30:
                    keep[k] = int(v)
            except Exception:
                continue
        state["daily_counts"] = keep
    else:
        state["daily_counts"] = {}

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def utc_dirs_to_check(hours_back: int):
    now = dt.datetime.utcnow()
    for h in range(hours_back, -1, -1):
        t = now - dt.timedelta(hours=h)
        yield t.strftime("%Y%m%d"), t.strftime("%H")


def list_cap_files(directory_url: str) -> list[str]:
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
    if elem is None:
        return ""
    found = elem.find(f".//{{*}}{tag_name}")
    return found.text.strip() if (found is not None and found.text) else ""


def pick_info_block(root: ET.Element) -> ET.Element | None:
    infos = root.findall(".//{*}info")
    if not infos:
        return None
    for info in infos:
        lang = find_text(info, "language")
        if normalize(lang).startswith("en"):
            return info
    return infos[0]


def parse_cap(xml_text: str) -> dict:
    root = ET.fromstring(xml_text)

    identifier = find_text(root, "identifier")
    sent = find_text(root, "sent")

    # CAP root-level fields
    msg_type = find_text(root, "msgType")  # Alert, Update, Cancel, etc.
    status = find_text(root, "status")     # Actual, Test

    info = pick_info_block(root)
    event = find_text(info, "event")
    headline = find_text(info, "headline")
    description = find_text(info, "description")
    instruction = find_text(info, "instruction")
    severity = find_text(info, "severity")
    certainty = find_text(info, "certainty")
    urgency = find_text(info, "urgency")

    areas = []
    if info is not None:
        for area in info.findall(".//{*}area"):
            ad = find_text(area, "areaDesc")
            if ad:
                areas.append(ad)

    return {
        "identifier": identifier,
        "sent": sent,
        "msgType": msg_type,
        "status": status,
        "event": event,
        "headline": headline,
        "description": description,
        "instruction": instruction,
        "severity": severity,
        "certainty": certainty,
        "urgency": urgency,
        "areas": areas,
    }


def should_include_event(cap: dict) -> bool:
    event = normalize(cap.get("event", ""))
    if not event:
        return False

    # Skip tests
    if any(bad in event for bad in EXCLUDED_EVENTS):
        return False

    # Skip test status at CAP root level
    if normalize(cap.get("status", "")) == "test":
        return False

    is_sws_event = (event == "special weather statement")

    if is_sws_event and INCLUDE_SPECIAL_WEATHER_STATEMENTS:
        return True

    if (not is_sws_event) and INCLUDE_ALERTS:
        return True

    return False


def area_matches(cap: dict) -> bool:
    hay = normalize(" | ".join(cap.get("areas", [])))
    fallback = normalize((cap.get("headline") or "") + " " + (cap.get("description") or ""))
    for kw in AREA_KEYWORDS:
        nkw = normalize(kw)
        if nkw and (nkw in hay or nkw in fallback):
            return True
    return False


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
    return email.utils.format_datetime(dt.datetime.now(dt.timezone.utc))


def parse_sent_to_epoch(sent: str) -> float:
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


def is_sws(cap: dict) -> bool:
    return normalize(cap.get("event", "")) == "special weather statement"


def is_cancel(cap: dict) -> bool:
    return normalize(cap.get("msgType", "")) == "cancel"


def cooldown_key(cap: dict) -> str:
    # event + primary area (stable enough for spam prevention)
    event = normalize(cap.get("event", ""))
    areas = cap.get("areas", []) or []
    primary_area = normalize(areas[0]) if areas else "tay-area"
    return f"{event}|{primary_area}"


def cooldown_minutes_for(cap: dict) -> int:
    if is_cancel(cap):
        return COOLDOWN_MINUTES_CANCEL
    if is_sws(cap):
        return COOLDOWN_MINUTES_SWS

    ev = normalize(cap.get("event", ""))
    if "warning" in ev:
        return COOLDOWN_MINUTES_WARNING
    if "watch" in ev:
        return COOLDOWN_MINUTES_WATCH
    if "advisory" in ev:
        return COOLDOWN_MINUTES_ADVISORY

    # default if it does not match known buckets
    return COOLDOWN_MINUTES_ADVISORY


def ensure_rss_exists() -> None:
    if os.path.exists(RSS_PATH):
        return

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = "Tay Township Weather Alerts"
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


def build_description(cap: dict) -> str:
    bits = []
    areas = cap.get("areas", [])
    if areas:
        bits.append(f"Area: {areas[0]}")

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
    if len(text) > 1200:
        text = text[:1200].rstrip() + "…"
    return text


# ----------------------------
# X helpers
# ----------------------------
def x_trim(text: str, limit: int = 280) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def hashtag_pack(cap: dict) -> str:
    """
    Add minimal, relevant tags. Keep it conservative for municipal comms.
    Always include #ONwx and a local tag.
    """
    base = ["#ONwx", "#TayTownship"]

    hay = normalize(" ".join([
        cap.get("event", "") or "",
        cap.get("headline", "") or "",
        cap.get("description", "") or "",
    ]))

    extra = []
    if any(k in hay for k in ["thunderstorm", "tornado"]):
        extra.append("#Storm")
    elif any(k in hay for k in ["winter", "snow", "blizzard", "squall", "ice", "freezing rain"]):
        extra.append("#Winter")
    elif any(k in hay for k in ["rainfall", "flood", "flooding"]):
        extra.append("#Flood")
    elif "heat" in hay:
        extra.append("#Heat")
    elif any(k in hay for k in ["cold", "wind chill"]):
        extra.append("#Cold")
    elif any(k in hay for k in ["fog", "visibility"]):
        extra.append("#Fog")
    elif "wind" in hay:
        extra.append("#Wind")

    tags = base + extra
    # Keep it short
    return " ".join(tags[:4])


def format_x_post(cap: dict, url: str) -> str:
    event = (cap.get("event") or "").strip() or "Weather update"
    headline = (cap.get("headline") or "").strip()
    areas = cap.get("areas") or []
    area = areas[0].strip() if areas else "Tay area"
    tags = hashtag_pack(cap)

    if is_cancel(cap):
        # All clear uses the event stored from active_alerts, but this is a fallback
        text = X_TEMPLATE_ALL_CLEAR.format(area=area, event=event, url=url, tags=tags)
    elif is_sws(cap):
        text = X_TEMPLATE_SWS.format(area=area, headline=headline, url=url, tags=tags)
    else:
        text = X_TEMPLATE_ALERT.format(event=event, area=area, headline=headline, url=url, tags=tags)

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
    posted_guids = set(state.get("posted_guids", []))
    posted_tweets = state.get("posted_tweets", {})
    active_alerts = state.get("active_alerts", {})

    if not isinstance(cooldowns, dict):
        cooldowns = {}
    if not isinstance(posted_tweets, dict):
        posted_tweets = {}
    if not isinstance(active_alerts, dict):
        active_alerts = {}

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
                    xml_text = requests.get(cap_url, headers={"User-Agent": USER_AGENT}, timeout=20).text
                    cap = parse_cap(xml_text)
                except Exception:
                    continue

                cap_id = cap.get("identifier", "").strip()
                if not cap_id or cap_id in seen:
                    continue

                # Mark seen so we do not keep reprocessing
                seen.add(cap_id)

                if not should_include_event(cap):
                    continue
                if not area_matches(cap):
                    continue

                key = cooldown_key(cap)
                now_ts = parse_sent_to_epoch(cap.get("sent", ""))
                last_ts = cooldowns.get(key)
                cooldown_mins = cooldown_minutes_for(cap)

                if isinstance(last_ts, (int, float)):
                    minutes_since = (now_ts - float(last_ts)) / 60.0
                    if minutes_since < cooldown_mins:
                        continue

                # Build RSS item (include cancels too for audit trail)
                title = cap.get("headline") or cap.get("event") or "Weather update"
                pub_date = rfc2822_date_from_sent(cap.get("sent", ""))
                guid = cap_id
                link = cap_url
                description = build_description(cap)

                if not rss_item_exists(channel, guid):
                    add_rss_item(channel, title=title, link=link, guid=guid, pub_date=pub_date, description=description)
                    new_items += 1
                    cooldowns[key] = now_ts

                # Decide whether to tweet
                if not ENABLE_X_POSTING:
                    continue

                if guid in posted_guids:
                    continue

                if not can_tweet_today(state):
                    continue

                # All clear logic:
                # If this CAP is a Cancel and we have an active alert for the same key, post an all clear.
                # If we do not have an active alert, we skip tweeting the cancel to avoid confusion.
                if is_cancel(cap):
                    active = active_alerts.get(key)
                    if not (isinstance(active, dict) and active.get("active") is True):
                        # No known active alert to clear
                        posted_guids.add(guid)
                        continue

                    prev_event = (active.get("event") or "").strip() or (cap.get("event") or "").strip() or "Weather alert"
                    areas = cap.get("areas") or []
                    area = areas[0].strip() if areas else (active.get("area") or "Tay area")
                    tags = hashtag_pack(cap)

                    clear_text = X_TEMPLATE_ALL_CLEAR.format(
                        area=area,
                        event=prev_event,
                        url=MORE_INFO_URL,
                        tags=tags,
                    )
                    clear_text = x_trim(re.sub(r"\n{3,}", "\n\n", clear_text).strip(), 280)

                    try:
                        tweet_id = post_to_x(clear_text)
                        posted_tweets[guid] = tweet_id
                        posted_guids.add(guid)
                        increment_tweet_today(state)
                        new_tweets += 1

                        # Mark as no longer active
                        active_alerts[key] = {
                            "active": False,
                            "event": prev_event,
                            "area": area,
                            "last_change_ts": now_ts,
                            "last_guid": guid,
                        }
                    except Exception as e:
                        print(f"X post failed (all clear): {e}")

                    continue

                # Normal alert or SWS
                try:
                    x_text = format_x_post(cap, MORE_INFO_URL)
                    tweet_id = post_to_x(x_text)
                    posted_tweets[guid] = tweet_id
                    posted_guids.add(guid)
                    increment_tweet_today(state)
                    new_tweets += 1

                    # Track actives: only treat non-SWS alerts as active for all clear
                    areas = cap.get("areas") or []
                    area = areas[0].strip() if areas else "Tay area"
                    if not is_sws(cap):
                        active_alerts[key] = {
                            "active": True,
                            "event": (cap.get("event") or "").strip(),
                            "area": area,
                            "last_change_ts": now_ts,
                            "last_guid": guid,
                        }

                except Exception as e:
                    print(f"X post failed: {e}")

    # Update RSS metadata
    now_rfc = email.utils.format_datetime(dt.datetime.now(dt.timezone.utc))
    lbd = channel.find("lastBuildDate")
    if lbd is None:
        lbd = ET.SubElement(channel, "lastBuildDate")
    lbd.text = now_rfc

    trim_rss_items(channel, MAX_RSS_ITEMS)

    # Save outputs
    tree.write(RSS_PATH, encoding="utf-8", xml_declaration=True)

    state["seen_ids"] = list(seen)
    state["cooldowns"] = cooldowns
    state["posted_guids"] = list(posted_guids)
    state["posted_tweets"] = posted_tweets
    state["active_alerts"] = active_alerts
    save_state(state)

    print(f"Done. Added {new_items} RSS item(s). Posted {new_tweets} tweet(s).")


if __name__ == "__main__":
    main()
