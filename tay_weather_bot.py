# tay_weather_bot.py
#
# Tay Township Weather Bot
# - Source of truth: Environment Canada Battleboard ATOM feed (ALERT_FEED_URL)
# - From the feed, discover the related warnings report page (report_e.html?...),
#   then parse the report page source for:
#     * Alert title (from feed entry title)
#     * Issued time (from feed entry updated/published)
#     * "What" lines and "When" line (from embedded JSON "text" blob in report page source)
# - Builds message previews for X (280 char limit) and Facebook (longer)
# - Posts:
#     * X: OAuth2 for tweet + OAuth1 for media upload (2 images)
#     * Facebook Page feed: message + optional attachments (currently message only; images optional)
# - Adds a small logo overlay to the bottom-right of images (PIL)
#
# NOTE: This script is designed for GitHub Actions. It uses state.json for de-duplication
#       and for platform cooldowns to avoid spam-rate limits.

import base64
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import List, Optional, Tuple

import requests
from PIL import Image
from requests_oauthlib import OAuth1
from zoneinfo import ZoneInfo


# ----------------------------
# Config (env)
# ----------------------------
ALERT_FEED_URL = os.getenv("ALERT_FEED_URL", "https://weather.gc.ca/rss/battleboard/onrm94_e.xml")
TAY_ALERTS_URL = os.getenv("TAY_ALERTS_URL", "https://weatherpresenter.github.io/tay-weather-rss/tay/")

CR29_NORTH_IMAGE_URL = os.getenv("CR29_NORTH_IMAGE_URL", "")
CR29_SOUTH_IMAGE_URL = os.getenv("CR29_SOUTH_IMAGE_URL", "")

ENABLE_X_POSTING = os.getenv("ENABLE_X_POSTING", "false").lower() == "true"
ENABLE_FB_POSTING = os.getenv("ENABLE_FB_POSTING", "false").lower() == "true"
TEST_TWEET = os.getenv("TEST_TWEET", "false").lower() == "true"

# X OAuth2 (tweet)
X_CLIENT_ID = os.getenv("X_CLIENT_ID", "")
X_CLIENT_SECRET = os.getenv("X_CLIENT_SECRET", "")
X_REFRESH_TOKEN = os.getenv("X_REFRESH_TOKEN", "")

# X OAuth1 (media upload)
X_API_KEY = os.getenv("X_API_KEY", "")
X_API_SECRET = os.getenv("X_API_SECRET", "")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN", "")
X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET", "")

# Facebook Page
FB_PAGE_ID = os.getenv("FB_PAGE_ID", "")
FB_PAGE_ACCESS_TOKEN = os.getenv("FB_PAGE_ACCESS_TOKEN", "")

# Logo overlay
LOGO_PATH = os.getenv("LOGO_PATH", "assets/On511_logo.png")
LOGO_SCALE = float(os.getenv("LOGO_SCALE", "0.20"))  # logo width as a fraction of image width
LOGO_MARGIN_PX = int(os.getenv("LOGO_MARGIN_PX", "18"))

# Cooldowns / anti-spam
FB_MIN_INTERVAL_SECONDS = int(os.getenv("FB_MIN_INTERVAL_SECONDS", "1800"))  # 30 minutes
X_MIN_INTERVAL_SECONDS = int(os.getenv("X_MIN_INTERVAL_SECONDS", "300"))     # 5 minutes

TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "25"))

TZ_TORONTO = ZoneInfo("America/Toronto")

# Endpoints
X_TOKEN_URL = "https://api.x.com/2/oauth2/token"
X_TWEET_URL = "https://api.x.com/2/tweets"
X_MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"
FB_FEED_URL = "https://graph.facebook.com/v24.0/{page_id}/feed"


# ----------------------------
# State
# ----------------------------
STATE_PATH = "state.json"

def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return {
            "last_alert_hash": None,
            "last_x_post_ts": None,
            "last_fb_post_ts": None,
            "fb_cooldown_until": None,
        }
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        # ensure keys
        data.setdefault("last_alert_hash", None)
        data.setdefault("last_x_post_ts", None)
        data.setdefault("last_fb_post_ts", None)
        data.setdefault("fb_cooldown_until", None)
        return data
    except Exception:
        return {
            "last_alert_hash": None,
            "last_x_post_ts": None,
            "last_fb_post_ts": None,
            "fb_cooldown_until": None,
        }

def save_state(state: dict) -> None:
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)


# ----------------------------
# Helpers
# ----------------------------
def fetch_text(url: str) -> str:
    r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "tay-weather-bot/1.0"})
    r.raise_for_status()
    return r.text

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def format_issued_short(dt_local: dt.datetime) -> str:
    # Example: "Jan 2 6:41p"
    mon = dt_local.strftime("%b")
    day = str(int(dt_local.strftime("%d")))  # no leading zero
    hour_12 = dt_local.strftime("%I").lstrip("0") or "12"
    minute = dt_local.strftime("%M")
    ampm = dt_local.strftime("%p").lower()
    ampm = "a" if ampm.startswith("a") else "p"
    return f"{mon} {day} {hour_12}:{minute}{ampm}"

def now_ts() -> int:
    return int(time.time())

def clamp_lines(lines: List[str], max_lines: int) -> List[str]:
    out=[]
    for ln in lines:
        ln = re.sub(r"\s+", " ", ln).strip()
        if not ln:
            continue
        out.append(ln)
        if len(out) >= max_lines:
            break
    return out

def safe_print(s: str) -> None:
    sys.stdout.write(s + "\n")
    sys.stdout.flush()

def is_no_alert_title(title: str) -> bool:
    """
    Battleboard sometimes publishes a placeholder like "No alerts in effect."
    This is defensive: treat common variants as "no active alerts."
    """
    t = re.sub(r"\s+", " ", (title or "")).strip().lower()
    if not t:
        return True
    needles = [
        "no alerts",
        "no alert",
        "no warnings",
        "no warning",
        "no watches",
        "no watch",
        "no advisories",
        "no advisory",
        "no statements",
        "no statement",
        "none in effect",
        "not in effect",
    ]
    return any(n in t for n in needles)


# ----------------------------
# Battleboard parsing
# ----------------------------
ATOM_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "cap": "urn:oasis:names:tc:emergency:cap:1.2",
}

@dataclass
class AlertInfo:
    title: str
    issued_dt_local: Optional[dt.datetime]
    report_url: str
    what_lines: List[str]
    when_line: str

def parse_battleboard_first_entry(feed_xml: str) -> Tuple[Optional[str], Optional[dt.datetime], Optional[str]]:
    """
    Returns (title, issued_dt_local, entry_html_snippet).

    If there are no entries, returns (None, None, None).
    """
    root = ET.fromstring(feed_xml)

    entry = root.find("atom:entry", ATOM_NS)
    if entry is None:
        return (None, None, None)

    title = (entry.findtext("atom:title", default="", namespaces=ATOM_NS) or "").strip()
    if is_no_alert_title(title):
        return (None, None, None)

    # issued time: prefer <published>, else <updated>
    ts = (entry.findtext("atom:published", default="", namespaces=ATOM_NS) or "").strip()
    if not ts:
        ts = (entry.findtext("atom:updated", default="", namespaces=ATOM_NS) or "").strip()

    issued_local = None
    if ts:
        try:
            issued_utc = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            issued_local = issued_utc.astimezone(TZ_TORONTO)
        except Exception:
            issued_local = None

    # Content can include the report href
    content = (entry.findtext("atom:content", default="", namespaces=ATOM_NS) or "")
    # Some feeds use <summary>
    if not content:
        content = (entry.findtext("atom:summary", default="", namespaces=ATOM_NS) or "")

    return title, issued_local, content

def extract_report_url_from_entry_content(entry_content: str) -> str:
    """
    Battleboard entries usually contain an <a href=".../warnings/report_e.html?...">.
    """
    if not entry_content:
        raise RuntimeError("Empty entry content; cannot extract report URL")

    # Look for report_e.html?onrm94 (or any report_e.html?... token)
    m = re.search(r'href="(https?://weather\.gc\.ca/warnings/report_e\.html\?[^"]+)"', entry_content)
    if m:
        return m.group(1)

    # Sometimes links are relative
    m = re.search(r'href="(/warnings/report_e\.html\?[^"]+)"', entry_content)
    if m:
        return "https://weather.gc.ca" + m.group(1)

    # Fallback: scan for plain URL
    m = re.search(r'(https?://weather\.gc\.ca/warnings/report_e\.html\?[\w\d]+)', entry_content)
    if m:
        return m.group(1)

    # As a last resort, derive from feed URL (onrm94_e.xml -> onrm94)
    mm = re.search(r'/battleboard/([a-z0-9]+)_e\.xml', ALERT_FEED_URL, re.I)
    if mm:
        return f"https://weather.gc.ca/warnings/report_e.html?{mm.group(1)}"

    raise RuntimeError("Could not find report URL in battleboard entry content")

def extract_alert_text_blob_from_report_source(html: str) -> str:
    """
    The report page has embedded JSON with a "text":"...What:\n...\nWhen:\n..." field.
    We pick the first blob that contains 'What:' and 'When:'.
    """
    candidates = re.findall(r'"text"\s*:\s*"([^"]{20,5000}?)"', html, flags=re.DOTALL)
    for c in candidates:
        if "What:" in c and "When:" in c:
            try:
                return json.loads('"' + c.replace("\\", "\\\\").replace('"', '\\"') + '"')
            except Exception:
                return c.replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "\r").replace('\\"', '"')

    m = re.search(r'"text"\s*:\s*"(.+?)"\s*,\s*"warnings"', html, flags=re.DOTALL)
    if m:
        c = m.group(1)
        try:
            return c.encode("utf-8").decode("unicode_escape")
        except Exception:
            return c.replace("\\n", "\n")

    raise RuntimeError("Could not locate embedded alert text blob on report page")

def parse_what_when_from_alert_text(alert_text: str) -> Tuple[List[str], str]:
    """
    Parse the text blob and return (what_lines, when_line).
    """
    t = alert_text.replace("\r\n", "\n").replace("\r", "\n")

    what_block = ""
    m = re.search(r"\bWhat:\s*\n(.+?)\n\s*When:\s*\n", t, flags=re.DOTALL | re.IGNORECASE)
    if m:
        what_block = m.group(1).strip()
    else:
        m2 = re.search(r"\bWhat:\s*(.+?)\n\s*When:\s*", t, flags=re.DOTALL | re.IGNORECASE)
        if m2:
            what_block = m2.group(1).strip()

    what_lines = [ln.strip() for ln in what_block.split("\n") if ln.strip()]
    what_lines = clamp_lines(what_lines, 5)

    when_block = ""
    m = re.search(r"\bWhen:\s*\n(.+?)(\n\s*Where:\s*\n|\n\s*Details:\s*\n|$)", t, flags=re.DOTALL | re.IGNORECASE)
    if m:
        when_block = m.group(1).strip()

    when_lines = [ln.strip() for ln in when_block.split("\n") if ln.strip()]
    when_lines = clamp_lines(when_lines, 3)
    when_line = " ".join(when_lines).strip()

    return what_lines, when_line


# ----------------------------
# Image helpers
# ----------------------------
def _open_image_bytes(b: bytes) -> Image.Image:
    from io import BytesIO
    return Image.open(BytesIO(b)).convert("RGBA")

def add_logo_overlay(img: Image.Image) -> Image.Image:
    """
    Bottom-right overlay. No-op if logo missing.
    """
    if not LOGO_PATH or not os.path.exists(LOGO_PATH):
        return img

    try:
        logo = Image.open(LOGO_PATH).convert("RGBA")
    except Exception:
        return img

    w, h = img.size
    target_w = max(80, int(w * LOGO_SCALE))
    ratio = target_w / max(1, logo.size[0])
    target_h = max(1, int(logo.size[1] * ratio))
    logo = logo.resize((target_w, target_h))

    x = max(0, w - target_w - LOGO_MARGIN_PX)
    y = max(0, h - target_h - LOGO_MARGIN_PX)

    out = img.copy()
    out.alpha_composite(logo, (x, y))
    return out

def fetch_camera_image(url: str) -> Optional[bytes]:
    if not url:
        return None
    r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "tay-weather-bot/1.0"})
    if r.status_code != 200:
        return None
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "image/" in ctype:
        return r.content

    html = r.text
    m = re.search(r'<img[^>]+src="([^"]+)"', html, flags=re.I)
    if m:
        img_url = m.group(1)
        if img_url.startswith("//"):
            img_url = "https:" + img_url
        elif img_url.startswith("/"):
            img_url = "https://511on.ca" + img_url
        rr = requests.get(img_url, timeout=TIMEOUT, headers={"User-Agent": "tay-weather-bot/1.0"})
        if rr.status_code == 200 and "image/" in (rr.headers.get("Content-Type") or "").lower():
            return rr.content

    return None

def prepare_images_for_post(urls: List[str]) -> List[bytes]:
    out=[]
    for u in urls:
        b = fetch_camera_image(u)
        if not b:
            continue
        try:
            img = _open_image_bytes(b)
            img = add_logo_overlay(img)
            from io import BytesIO
            buf = BytesIO()
            img.convert("RGB").save(buf, format="JPEG", quality=88, optimize=True)
            out.append(buf.getvalue())
        except Exception:
            continue
    return out


# ----------------------------
# X (Twitter) auth + posting
# ----------------------------
def get_oauth2_access_token() -> str:
    if not (X_CLIENT_ID and X_CLIENT_SECRET and X_REFRESH_TOKEN):
        raise RuntimeError("Missing X_CLIENT_ID / X_CLIENT_SECRET / X_REFRESH_TOKEN")

    basic = base64.b64encode(f"{X_CLIENT_ID}:{X_CLIENT_SECRET}".encode("utf-8")).decode("ascii")
    headers = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": X_REFRESH_TOKEN,
    }
    r = requests.post(X_TOKEN_URL, headers=headers, data=data, timeout=TIMEOUT)
    safe_print(f"X token refresh status: {r.status_code}")
    if r.status_code != 200:
        safe_print(f"X token refresh error body: {r.text}")
        r.raise_for_status()
    payload = r.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError("No access_token returned from X token refresh")

    new_refresh = payload.get("refresh_token")
    if new_refresh and new_refresh != X_REFRESH_TOKEN:
        safe_print("⚠️ X refresh token rotated. Workflow will update the repo secret.")
        with open("x_refresh_token_rotated.txt", "w", encoding="utf-8") as f:
            f.write(new_refresh)

    return access_token

def x_upload_media(image_bytes: bytes) -> str:
    if not (X_API_KEY and X_API_SECRET and X_ACCESS_TOKEN and X_ACCESS_TOKEN_SECRET):
        raise RuntimeError("Missing X OAuth1 keys for media upload")

    oauth = OAuth1(X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET)
    payload = {"media_data": base64.b64encode(image_bytes).decode("ascii")}
    r = requests.post(X_MEDIA_UPLOAD_URL, auth=oauth, data=payload, timeout=TIMEOUT)
    safe_print(f"X media upload status: {r.status_code}")
    r.raise_for_status()
    j = r.json()
    media_id = j.get("media_id_string") or str(j.get("media_id"))
    if not media_id:
        raise RuntimeError("No media_id returned from X media upload")
    return media_id

def x_post_tweet(text: str, images: List[bytes]) -> bool:
    access_token = get_oauth2_access_token()
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    media_ids = []
    for b in images[:2]:
        try:
            media_ids.append(x_upload_media(b))
        except Exception as e:
            safe_print(f"⚠️ X media upload failed: {e}")

    payload = {"text": text}
    if media_ids:
        payload["media"] = {"media_ids": media_ids}

    if TEST_TWEET:
        safe_print("TEST_TWEET=true; skipping actual X post.")
        return False

    r = requests.post(X_TWEET_URL, headers=headers, json=payload, timeout=TIMEOUT)
    safe_print(f"X POST /2/tweets status: {r.status_code}")
    r.raise_for_status()
    return True


# ----------------------------
# Facebook posting
# ----------------------------
def fb_post_message(message: str) -> Tuple[bool, bool]:
    """
    Returns (ok, rate_limited). Rate limit is commonly returned as code 368.
    """
    if not (FB_PAGE_ID and FB_PAGE_ACCESS_TOKEN):
        raise RuntimeError("Missing FB_PAGE_ID / FB_PAGE_ACCESS_TOKEN")

    if TEST_TWEET:
        safe_print("TEST_TWEET=true; skipping actual Facebook post.")
        return (False, False)

    url = FB_FEED_URL.format(page_id=FB_PAGE_ID)
    r = requests.post(url, data={"message": message, "access_token": FB_PAGE_ACCESS_TOKEN}, timeout=TIMEOUT)
    safe_print(f"FB POST /feed status: {r.status_code}")

    if r.status_code in (200, 201):
        return (True, False)

    body = r.text or ""
    safe_print(f"⚠️ Facebook post error: {body}")

    rate_limited = ("\"code\":368" in body) or ("error_subcode\":1390008" in body)
    return (False, rate_limited)


# ----------------------------
# Message builders
# ----------------------------
CARE_X = "Please take care, travel only if needed and check on neighbours who may need support."
CARE_FB = ("If you can, please stay off the roads and give crews room to work. "
           "If you must go out, slow down, leave extra space and keep your lights on. "
           "Please check on neighbours who may need help staying warm or getting supplies.")

def build_headline_line(title: str) -> str:
    t = re.sub(r"\s+", " ", title).strip()
    t = t.title()
    return f"🟡 - {t} in Tay Township"

def build_x_message(alert: AlertInfo) -> str:
    headline = build_headline_line(alert.title)

    what_lines = clamp_lines(alert.what_lines, 2)
    when_line = re.sub(r"\s+", " ", alert.when_line).strip()

    issued = format_issued_short(alert.issued_dt_local) if alert.issued_dt_local else ""
    footer = f"More: {TAY_ALERTS_URL}\nIssued {issued} #TayTownship #ONStorm".strip()

    parts = [headline, ""]
    parts += what_lines
    if when_line:
        parts.append(when_line)
    parts.append(CARE_X)
    parts.append("")
    parts.append(footer)

    msg = "\n".join([p for p in parts if p is not None])

    if len(msg) <= 280:
        return msg

    if len(what_lines) > 1:
        what_lines = what_lines[:1]
        parts = [headline, ""] + what_lines
        if when_line:
            parts.append(when_line)
        parts.append(CARE_X)
        parts.append("")
        parts.append(footer)
        msg = "\n".join(parts)
        if len(msg) <= 280:
            return msg

    short_care = "Please take care, travel only if needed."
    parts = [headline, ""] + what_lines
    if when_line:
        parts.append(when_line)
    parts.append(short_care)
    parts.append("")
    parts.append(footer)
    msg = "\n".join(parts)
    if len(msg) <= 280:
        return msg

    parts = [headline, ""] + what_lines + [short_care, "", footer]
    msg = "\n".join(parts)
    return msg[:280]

def build_fb_message(alert: AlertInfo) -> str:
    headline = build_headline_line(alert.title)

    what_lines = clamp_lines(alert.what_lines, 3)
    when_line = re.sub(r"\s+", " ", alert.when_line).strip()

    issued = format_issued_short(alert.issued_dt_local) if alert.issued_dt_local else ""
    footer = f"More: {TAY_ALERTS_URL}\nIssued {issued} #TayTownship #ONStorm".strip()

    parts = [headline, ""]
    parts += what_lines
    if when_line:
        parts.append(when_line)
    parts.append(CARE_FB)
    parts.append("")
    parts.append(footer)
    return "\n".join(parts)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    state = load_state()

    # Fetch battleboard feed
    feed_xml = fetch_text(ALERT_FEED_URL)

    title, issued_local, entry_content = parse_battleboard_first_entry(feed_xml)

    # ✅ No active alerts: exit cleanly and do not post
    if not title:
        safe_print("✅ No active alerts found in battleboard feed. Nothing to post.")
        return

    report_url = extract_report_url_from_entry_content(entry_content or "")

    # Fetch warnings report page source
    report_html = fetch_text(report_url)
    alert_text = extract_alert_text_blob_from_report_source(report_html)
    what_lines, when_line = parse_what_when_from_alert_text(alert_text)

    alert = AlertInfo(
        title=title,
        issued_dt_local=issued_local,
        report_url=report_url,
        what_lines=what_lines,
        when_line=when_line,
    )

    dedupe_basis = json.dumps(
        {
            "title": title,
            "report_url": report_url,
            "what": what_lines,
            "when": when_line,
            "issued": issued_local.isoformat() if issued_local else None,
        },
        sort_keys=True,
    )
    alert_hash = sha256_hex(dedupe_basis)

    x_text = build_x_message(alert)
    fb_text = build_fb_message(alert)

    safe_print("X preview:")
    safe_print(x_text)
    safe_print("\nFB preview:")
    safe_print(fb_text)

    if state.get("last_alert_hash") == alert_hash:
        safe_print("No changes in alert; nothing to post.")
        return

    images = prepare_images_for_post([CR29_NORTH_IMAGE_URL, CR29_SOUTH_IMAGE_URL])

    posted_any = False

    # X post cooldown
    if ENABLE_X_POSTING:
        last_x = state.get("last_x_post_ts")
        if last_x and (now_ts() - int(last_x)) < X_MIN_INTERVAL_SECONDS:
            safe_print("⚠️ X skipped: cooldown")
        else:
            try:
                if x_post_tweet(x_text, images):
                    state["last_x_post_ts"] = now_ts()
                    posted_any = True
            except Exception as e:
                safe_print(f"⚠️ X skipped: {e}")

    # FB post cooldown + spam block handling
    if ENABLE_FB_POSTING:
        cooldown_until = state.get("fb_cooldown_until")
        if cooldown_until and now_ts() < int(cooldown_until):
            safe_print("⚠️ Facebook skipped: cooldown")
        else:
            last_fb = state.get("last_fb_post_ts")
            if last_fb and (now_ts() - int(last_fb)) < FB_MIN_INTERVAL_SECONDS:
                safe_print("⚠️ Facebook skipped: min interval")
            else:
                try:
                    ok, rate_limited = fb_post_message(fb_text)
                    if ok:
                        state["last_fb_post_ts"] = now_ts()
                        posted_any = True
                    elif rate_limited:
                        state["fb_cooldown_until"] = now_ts() + 2 * 60 * 60
                        safe_print("⚠️ Facebook rate limit hit. Backing off for 2 hours.")
                except Exception as e:
                    safe_print(f"⚠️ Facebook skipped: {e}")

    # ✅ Save hash if ANY platform posted (works even if FB disabled)
    if posted_any:
        state["last_alert_hash"] = alert_hash
        save_state(state)
        safe_print("Social posts sent: 1+")
    else:
        save_state(state)
        safe_print("No social posts sent for this alert.")

if __name__ == "__main__":
    main()
