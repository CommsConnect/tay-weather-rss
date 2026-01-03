#!/usr/bin/env python3
# Tay Township Weather Bot (battleboard RSS -> related warnings report -> parse "What/When")
#
# Key behaviour:
# - Fetch Environment Canada "battleboard" RSS (ALERT_FEED_URL)
# - Extract the most relevant "related" link (warnings report page)
# - Fetch that warnings report page and parse:
#     * issued time text
#     * alert label/type (e.g., Snow Squall)
#     * headline sentence
#     * What block
#     * When line
#     * impact/confidence (if present)
# - Build X + Facebook post text
#   - Headline ALWAYS ends with "in Tay Township."
# - Print previews every run
# - Optional posting:
#     * X: OAuth2 for posting + OAuth1 for media upload (if enabled)
#     * Facebook: page feed post with optional images (if enabled)
#
# Notes:
# - EC HTML markup can change. This parser is designed to be resilient by using text-pattern extraction.
# - You can force the report URL with REPORT_URL env var if needed.

import base64
import datetime as dt
import hashlib
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from requests_oauthlib import OAuth1


# ----------------------------
# Config / env
# ----------------------------
ALERT_FEED_URL = os.getenv("ALERT_FEED_URL", "https://weather.gc.ca/rss/battleboard/onrm94_e.xml")
# If set, overrides RSS "related" parsing:
REPORT_URL_OVERRIDE = os.getenv("REPORT_URL", "").strip()

TAY_ALERTS_URL = os.getenv("TAY_ALERTS_URL", "https://weatherpresenter.github.io/tay-weather-rss/tay/")

CR29_NORTH_IMAGE_URL = os.getenv("CR29_NORTH_IMAGE_URL", "https://511on.ca/map/Cctv/400")
CR29_SOUTH_IMAGE_URL = os.getenv("CR29_SOUTH_IMAGE_URL", "https://511on.ca/map/Cctv/402")

ENABLE_X_POSTING = os.getenv("ENABLE_X_POSTING", "false").lower() == "true"
ENABLE_FB_POSTING = os.getenv("ENABLE_FB_POSTING", "false").lower() == "true"
TEST_TWEET = os.getenv("TEST_TWEET", "false").lower() == "true"

STATE_PATH = os.getenv("STATE_PATH", "state.json")

# X OAuth2 (posting)
X_CLIENT_ID = os.getenv("X_CLIENT_ID", "")
X_CLIENT_SECRET = os.getenv("X_CLIENT_SECRET", "")
X_REFRESH_TOKEN = os.getenv("X_REFRESH_TOKEN", "")

# X OAuth1 (media upload v1.1)
X_API_KEY = os.getenv("X_API_KEY", "")
X_API_SECRET = os.getenv("X_API_SECRET", "")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN", "")
X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET", "")

# Facebook
FB_PAGE_ID = os.getenv("FB_PAGE_ID", "")
FB_PAGE_ACCESS_TOKEN = os.getenv("FB_PAGE_ACCESS_TOKEN", "")

# Cooldowns (seconds) to avoid FB spam throttles / repeat posts
FB_MIN_SECONDS_BETWEEN_POSTS = int(os.getenv("FB_MIN_SECONDS_BETWEEN_POSTS", "900"))  # 15 min default
X_MIN_SECONDS_BETWEEN_POSTS = int(os.getenv("X_MIN_SECONDS_BETWEEN_POSTS", "300"))   # 5 min default

# Requests defaults
UA = "Mozilla/5.0 (compatible; TayWeatherBot/2.0; +https://github.com/weatherpresenter/tay-weather-rss)"
TIMEOUT = 25


# ----------------------------
# Helpers: state
# ----------------------------
def load_state() -> Dict:
    if not os.path.exists(STATE_PATH):
        return {
            "seen_keys": [],
            "last_fb_post_ts": 0,
            "last_x_post_ts": 0,
            "last_report_url": "",
            "last_hash": "",
        }
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: Dict) -> None:
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, STATE_PATH)


def now_ts() -> int:
    return int(time.time())


# ----------------------------
# Helpers: networking
# ----------------------------
def http_get(url: str) -> requests.Response:
    headers = {
        "User-Agent": UA,
        "Accept": "*/*",
    }
    r = requests.get(url, headers=headers, timeout=TIMEOUT)
    return r


# ----------------------------
# Step 1: parse battleboard RSS and get related report URL
# ----------------------------
def extract_report_url_from_battleboard(xml_bytes: bytes) -> Optional[str]:
    """
    Battleboard feeds vary, but commonly contain entry/item link(s).
    We try multiple strategies:
    - ATOM <entry><link rel="related" href="...">
    - ATOM <entry><link href="..."> (pick warnings/report)
    - RSS <item><link>...</link>
    - Any URL in feed text matching weather.gc.ca/warnings/report_*.html?...=...
    """
    text = xml_bytes.decode("utf-8", errors="replace")

    # Fast regex scan for warnings report URL
    m = re.search(r"https?://weather\.gc\.ca/warnings/report_[a-z]_\.html\?[^\s\"'<]+", text, flags=re.I)
    if m:
        return m.group(0)

    # XML parse
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None

    # Helper to iterate elements without caring about namespaces
    def strip_ns(tag: str) -> str:
        return tag.split("}", 1)[-1] if "}" in tag else tag

    # Collect candidate hrefs
    candidates: List[str] = []

    for elem in root.iter():
        if strip_ns(elem.tag) == "link":
            href = elem.attrib.get("href", "") or (elem.text or "")
            rel = (elem.attrib.get("rel", "") or "").lower()
            if href:
                candidates.append(href.strip())
            # Sometimes <link rel="related" href="...">
            if rel == "related" and href:
                candidates.insert(0, href.strip())

    # RSS style <item><link>...</link>
    for elem in root.iter():
        if strip_ns(elem.tag) == "link" and (elem.text or "").strip():
            candidates.append(elem.text.strip())

    # Prefer warnings report links for the location
    for c in candidates:
        if "weather.gc.ca/warnings/report_" in c and "onrm94" in c:
            return c
    for c in candidates:
        if "weather.gc.ca/warnings/report_" in c:
            return c

    return candidates[0] if candidates else None


# ----------------------------
# Step 2: parse the warnings report page for fields
# ----------------------------
def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def parse_report_fields(report_html: str) -> Dict[str, str]:
    """
    Returns:
      issued_text
      impact_level
      forecast_confidence
      alert_label (e.g., "Snow Squall" or fallback "Weather information")
      headline_sentence (first sentence before "What:" if present)
      what_block
      when_line
    """
    soup = BeautifulSoup(report_html, "html.parser")
    page_text = clean_spaces(soup.get_text(" ", strip=True))

    # Issued time: look for e.g. "6:41 PM EST Friday 2 January 2026"
    issued_text = ""
    m = re.search(r"\b\d{1,2}:\d{2}\s*(AM|PM)\s*[A-Z]{2,4}\s+\w+\s+\d{1,2}\s+\w+\s+\d{4}\b", page_text)
    if m:
        issued_text = m.group(0)

    # Impact level & forecast confidence often appear as "Impact Level: Moderate" etc.
    impact_level = ""
    m = re.search(r"Impact Level:\s*([A-Za-z]+)", page_text)
    if m:
        impact_level = m.group(1)

    forecast_confidence = ""
    m = re.search(r"Forecast Confidence:\s*([A-Za-z]+)", page_text)
    if m:
        forecast_confidence = m.group(1)

    # Alert label:
    # Try to find a phrase like "Yellow Warning - Snow Squall" or "Snow Squall Warning"
    alert_label = "Weather information"
    # Prefer "Snow Squall" etc.
    m = re.search(r"(Snow Squall|Winter Storm|Snowfall|Freezing Rain|Blizzard|Tornado|Thunderstorm|Rainfall|Heat|Cold)\b", page_text, flags=re.I)
    if m:
        alert_label = m.group(1).title()

    # The main alert paragraph often contains:
    # "<headline>. What: ... When: ... Where: ..."
    headline_sentence = ""
    what_block = ""
    when_line = ""

    # Extract What and When using robust patterns
    # 1) What: ... When:
    m = re.search(r"\bWhat:\s*(.*?)\s*\bWhen:\s*(.*?)(\s*\bWhere:\b|\s*\bAdditional information:\b|$)", page_text, flags=re.I)
    if m:
        what_block = clean_spaces(m.group(1))
        when_line = clean_spaces(m.group(2))

        # Headline is the text immediately before "What:" if possible.
        pre = page_text[: m.start()]
        # Take last ~200 chars before What: and grab last sentence.
        tail = pre[-300:]
        # Find last sentence-like chunk
        sent = re.split(r"(?<=[.!?])\s+", tail)
        headline_sentence = clean_spaces(sent[-1]) if sent else ""
        # Sometimes it still includes separators; clean known junk.
        headline_sentence = re.sub(r"^\*+\s*", "", headline_sentence).strip()

    # Fallback: if no What/When found, use first sentence after the timestamp line if present
    if not headline_sentence:
        # Try to find a sentence that follows the issued time.
        if issued_text:
            idx = page_text.find(issued_text)
            if idx != -1:
                after = page_text[idx + len(issued_text):]
                sent = re.split(r"(?<=[.!?])\s+", after.strip())
                if sent and sent[0]:
                    headline_sentence = clean_spaces(sent[0])

    return {
        "issued_text": issued_text,
        "impact_level": impact_level,
        "forecast_confidence": forecast_confidence,
        "alert_label": alert_label,
        "headline_sentence": headline_sentence,
        "what_block": what_block,
        "when_line": when_line,
    }


# ----------------------------
# Post text builders
# ----------------------------
def severity_emoji_from_label(label: str) -> str:
    # Keep your existing "yellow warning" vibe; if you later parse colour explicitly, adjust here.
    return "🟡"


def build_headline(alert_label: str) -> str:
    # REQUIRED: headline ends with "in Tay Township."
    # Keep it short + consistent.
    return f"{alert_label} in Tay Township."


def split_sentences(text: str) -> List[str]:
    text = clean_spaces(text)
    if not text:
        return []
    parts = re.split(r"(?<=[.!?])\s+", text)
    # Remove trailing punctuation duplicates
    return [p.strip() for p in parts if p.strip()]


def build_x_text(fields: Dict[str, str]) -> str:
    emoji = severity_emoji_from_label(fields["alert_label"])
    headline = build_headline(fields["alert_label"])

    # What/When (keep X short)
    what_sents = split_sentences(fields.get("what_block", ""))
    when_line = fields.get("when_line", "").strip()

    # Simple care line (your style)
    care = "Please take care, travel only if needed and check on neighbours who may need support."

    issued_short = ""
    if fields.get("issued_text"):
        # Turn "6:41 PM EST Friday 2 January 2026" -> "Jan 2 6:41p"
        issued_short = format_issued_short(fields["issued_text"])

    chunks: List[str] = [f"{emoji} - {headline}"]

    # Add 1-2 What sentences if available
    if what_sents:
        chunks.append(f"What: {what_sents[0]}")
        if len(what_sents) > 1:
            chunks.append(what_sents[1])

    # Add When if room
    if when_line:
        chunks.append(f"When: {when_line}")

    chunks.append(care)
    chunks.append(f"More: {TAY_ALERTS_URL}")
    if issued_short:
        chunks.append(f"Issued {issued_short}")
    chunks.append("#TayTownship #ONStorm")

    text = " | ".join([c for c in chunks if c])

    # Hard trim to 280
    if len(text) <= 280:
        return text

    # If too long, progressively drop less critical parts
    order_to_drop = [
        ("when", f"When: {when_line}" if when_line else ""),
        ("what2", what_sents[1] if len(what_sents) > 1 else ""),
        ("what1", f"What: {what_sents[0]}" if what_sents else ""),
        ("care", care),
    ]

    cur_chunks = [c for c in chunks if c]
    for _, drop in order_to_drop:
        if drop and any(drop == c for c in cur_chunks):
            cur_chunks = [c for c in cur_chunks if c != drop]
            text = " | ".join(cur_chunks)
            if len(text) <= 280:
                return text

    # Final trim: brutal cut but keep hashtags
    # Ensure hashtags remain at end
    hashtags = "#TayTownship #ONStorm"
    base = text.replace(hashtags, "").strip(" |")
    max_base = 280 - (len(hashtags) + 3)
    if max_base < 0:
        return hashtags[:280]
    base = base[:max_base].rstrip(" |")
    return f"{base} | {hashtags}"


def build_fb_text(fields: Dict[str, str]) -> str:
    emoji = severity_emoji_from_label(fields["alert_label"])
    headline = build_headline(fields["alert_label"])

    what_sents = split_sentences(fields.get("what_block", ""))
    when_line = fields.get("when_line", "").strip()

    # Longer care statement for Facebook
    care = (
        "If you can, please stay off the roads and give crews room to work. "
        "If you must go out, slow down, leave extra space and keep your lights on. "
        "Please check on neighbours who may need help staying warm or getting supplies."
    )

    issued_short = ""
    if fields.get("issued_text"):
        issued_short = format_issued_short(fields["issued_text"])

    chunks: List[str] = [f"{emoji} - {headline}"]

    # Add up to 3 What sentences
    if what_sents:
        chunks.append("What: " + " ".join(what_sents[:3]))

    if when_line:
        chunks.append(f"When: {when_line}")

    chunks.append(care)
    chunks.append(f"More: {TAY_ALERTS_URL}")
    if issued_short:
        chunks.append(f"Issued {issued_short}")
    chunks.append("#TayTownship #ONStorm")

    return " | ".join([c for c in chunks if c])


def format_issued_short(issued_text: str) -> str:
    """
    Example input: "6:41 PM EST Friday 2 January 2026"
    Output: "Jan 2 6:41p"
    """
    s = issued_text.strip()
    # Capture time + day + month name
    m = re.search(r"(\d{1,2}:\d{2})\s*(AM|PM)\s*[A-Z]{2,4}\s+\w+\s+(\d{1,2})\s+(\w+)\s+\d{4}", s)
    if not m:
        return s
    hhmm = m.group(1)
    ampm = m.group(2).lower()
    day = int(m.group(3))
    mon_name = m.group(4)
    mon = mon_name[:3].title()
    return f"{mon} {day} {hhmm}{'a' if ampm=='am' else 'p'}"


# ----------------------------
# X posting (OAuth2 tweet, OAuth1 media upload)
# ----------------------------
def get_oauth2_access_token() -> Tuple[Optional[str], Optional[str]]:
    """
    Uses refresh_token to obtain a bearer access token.
    Returns (access_token, rotated_refresh_token_or_None)
    """
    if not (X_CLIENT_ID and X_CLIENT_SECRET and X_REFRESH_TOKEN):
        return None, None

    url = "https://api.x.com/2/oauth2/token"
    auth = (X_CLIENT_ID, X_CLIENT_SECRET)

    data = {
        "grant_type": "refresh_token",
        "refresh_token": X_REFRESH_TOKEN,
    }

    headers = {"User-Agent": UA}

    r = requests.post(url, auth=auth, data=data, headers=headers, timeout=TIMEOUT)
    # Print helpful debug on failure
    if r.status_code >= 400:
        try:
            print(f"X token refresh status: {r.status_code}")
            print(f"X token refresh error body: {r.text}")
        except Exception:
            pass
        r.raise_for_status()

    payload = r.json()
    access_token = payload.get("access_token")
    new_refresh = payload.get("refresh_token")
    rotated = None
    if new_refresh and new_refresh != X_REFRESH_TOKEN:
        rotated = new_refresh
    print(f"X token refresh status: {r.status_code}")
    return access_token, rotated


def upload_media_to_x(image_url: str) -> Optional[str]:
    """
    Upload an image to X using v1.1 media endpoint (requires OAuth1 user context).
    Returns media_id_string.
    """
    if not (X_API_KEY and X_API_SECRET and X_ACCESS_TOKEN and X_ACCESS_TOKEN_SECRET):
        return None

    # Download image bytes
    r = http_get(image_url)
    r.raise_for_status()
    b64 = base64.b64encode(r.content).decode("ascii")

    oauth = OAuth1(
        X_API_KEY,
        client_secret=X_API_SECRET,
        resource_owner_key=X_ACCESS_TOKEN,
        resource_owner_secret=X_ACCESS_TOKEN_SECRET,
    )

    upload_url = "https://upload.twitter.com/1.1/media/upload.json"
    resp = requests.post(upload_url, auth=oauth, data={"media_data": b64}, timeout=TIMEOUT)
    print(f"X media upload status: {resp.status_code}")
    if resp.status_code >= 400:
        print(resp.text)
        return None
    j = resp.json()
    return j.get("media_id_string")


def post_to_x(text: str, image_urls: Optional[List[str]] = None) -> None:
    access_token, rotated = get_oauth2_access_token()
    if not access_token:
        raise RuntimeError("X_TOKEN_REFRESH_FAILED")

    if rotated:
        print("⚠️ X refresh token rotated. Workflow will update the repo secret.")
        with open("x_refresh_token_rotated.txt", "w", encoding="utf-8") as f:
            f.write(rotated)

    media_ids: List[str] = []
    if image_urls:
        for u in image_urls[:4]:
            mid = upload_media_to_x(u)
            if mid:
                media_ids.append(mid)

    url = "https://api.x.com/2/tweets"
    headers = {"Authorization": f"Bearer {access_token}", "User-Agent": UA, "Content-Type": "application/json"}
    payload = {"text": text}
    if media_ids:
        payload["media"] = {"media_ids": media_ids}

    if TEST_TWEET:
        print("⚠️ TEST_TWEET enabled: skipping X post.")
        return

    r = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
    print(f"X POST /2/tweets status: {r.status_code}")
    if r.status_code >= 400:
        raise RuntimeError(f"X post failed {r.status_code} {r.text}")


# ----------------------------
# Facebook posting
# ----------------------------
def fb_post_message_with_images(message: str, image_urls: List[str]) -> None:
    """
    Simplest approach: post message to /feed.
    If you want true multi-image carousel, you need unpublished photo uploads + attached_media.
    This function does an "unpublished photo" upload flow and attaches them.
    """
    if not (FB_PAGE_ID and FB_PAGE_ACCESS_TOKEN):
        raise RuntimeError("FB_MISSING_CREDS")

    # Upload photos unpublished
    media_fbid: List[str] = []
    for u in image_urls[:4]:
        # Download bytes
        r = http_get(u)
        r.raise_for_status()

        photo_url = f"https://graph.facebook.com/v24.0/{FB_PAGE_ID}/photos"
        files = {"source": ("image.jpg", r.content, "image/jpeg")}
        data = {"published": "false", "access_token": FB_PAGE_ACCESS_TOKEN}
        resp = requests.post(photo_url, files=files, data=data, timeout=TIMEOUT)
        if resp.status_code >= 400:
            raise RuntimeError(f"Facebook photo upload failed {resp.status_code} {resp.text}")
        j = resp.json()
        if "id" in j:
            media_fbid.append(j["id"])

    # Post feed with attached_media
    feed_url = f"https://graph.facebook.com/v24.0/{FB_PAGE_ID}/feed"
    data = {"message": message, "access_token": FB_PAGE_ACCESS_TOKEN}
    for idx, mid in enumerate(media_fbid):
        data[f"attached_media[{idx}]"] = json.dumps({"media_fbid": mid})

    r = requests.post(feed_url, data=data, timeout=TIMEOUT)
    print(f"FB POST /feed (carousel) status: {r.status_code}")
    if r.status_code >= 400:
        raise RuntimeError(f"Facebook carousel post failed {r.status_code} {r.text}")


# ----------------------------
# Main workflow
# ----------------------------
def main() -> None:
    state = load_state()

    # 1) Determine report URL
    report_url = REPORT_URL_OVERRIDE
    if not report_url:
        rss = http_get(ALERT_FEED_URL)
        rss.raise_for_status()
        report_url = extract_report_url_from_battleboard(rss.content) or ""

    if not report_url:
        raise RuntimeError("Could not determine report URL from battleboard RSS. Set REPORT_URL env var to override.")

    # 2) Fetch report page + parse fields
    rep = http_get(report_url)
    rep.raise_for_status()
    fields = parse_report_fields(rep.text)

    # 3) Build texts (always show previews)
    x_text = build_x_text(fields)
    fb_text = build_fb_text(fields)

    print(f"X preview: {x_text}")
    print(f"FB preview: {fb_text}")

    # 4) Create a stable "alert key" to avoid reposting the same content
    key_material = json.dumps(
        {
            "report_url": report_url,
            "issued_text": fields.get("issued_text", ""),
            "what": fields.get("what_block", ""),
            "when": fields.get("when_line", ""),
            "label": fields.get("alert_label", ""),
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    content_hash = hashlib.sha256(key_material.encode("utf-8")).hexdigest()[:24]

    # If we've already posted this hash, stop
    seen = set(state.get("seen_keys", []))
    if content_hash in seen:
        print("No new alert content (hash already seen).")
        state["last_report_url"] = report_url
        state["last_hash"] = content_hash
        save_state(state)
        return

    # 5) Posting cooldown checks
    camera_image_urls = [CR29_NORTH_IMAGE_URL, CR29_SOUTH_IMAGE_URL]

    posted_any = False
    skipped_reasons: List[str] = []

    # X
    if ENABLE_X_POSTING:
        if now_ts() - int(state.get("last_x_post_ts", 0)) < X_MIN_SECONDS_BETWEEN_POSTS:
            skipped_reasons.append("X cooldown")
        else:
            try:
                post_to_x(x_text, image_urls=camera_image_urls)
                state["last_x_post_ts"] = now_ts()
                posted_any = True
            except Exception as e:
                skipped_reasons.append(f"X failed: {e}")
                print(f"⚠️ X skipped: {e}")
    else:
        skipped_reasons.append("X disabled")

    # Facebook
    if ENABLE_FB_POSTING:
        if now_ts() - int(state.get("last_fb_post_ts", 0)) < FB_MIN_SECONDS_BETWEEN_POSTS:
            skipped_reasons.append("FB cooldown")
        else:
            try:
                fb_post_message_with_images(fb_text, camera_image_urls)
                state["last_fb_post_ts"] = now_ts()
                posted_any = True
            except Exception as e:
                skipped_reasons.append(f"FB failed: {e}")
                print(f"⚠️ Facebook skipped: {e}")
    else:
        skipped_reasons.append("FB disabled")

    # 6) Update state
    seen.add(content_hash)
    # keep last 200
    state["seen_keys"] = list(seen)[-200:]
    state["last_report_url"] = report_url
    state["last_hash"] = content_hash
    save_state(state)

    if not posted_any:
        print("No social posts sent for this alert.")
        if skipped_reasons:
            print("Reasons:", " | ".join(skipped_reasons))


if __name__ == "__main__":
    main()
