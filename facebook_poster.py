# facebook_poster.py
from __future__ import annotations

import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests


# ----------------------------
# Settings (tune these safely)
# ----------------------------
# Cooldown between SUCCESSFUL FB posts (prevents overwhelming FB + followers)
DEFAULT_FB_COOLDOWN_SECONDS = int(os.getenv("FB_COOLDOWN_SECONDS", "3600"))  # 60 minutes

# If FB blocks/rate-limits you (code 368), we pause ALL FB attempts for this long
DEFAULT_FB_BLOCK_SECONDS = int(os.getenv("FB_BLOCK_SECONDS", "21600"))       # 6 hours

# Network
DEFAULT_FB_TIMEOUT_SECONDS = int(os.getenv("FB_TIMEOUT_SECONDS", "30"))

# Small per-request jitter (helps avoid "same-second" spam heuristics)
DEFAULT_FB_JITTER_SECONDS = float(os.getenv("FB_JITTER_SECONDS", "3"))      # seconds


# ----------------------------
# State helpers
# ----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(ts: str) -> Optional[datetime]:
    try:
        if not ts:
            return None
        # accepts "2026-01-03T17:08:00Z" or "...+00:00"
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_state(path: str = "state.json") -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception:
        # If state is corrupt, don't crash the run
        return {}


def save_state(state: Dict[str, Any], path: str = "state.json") -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
    except Exception as e:
        print("⚠️ Failed to save state.json:", e)


# ----------------------------
# Facebook error parsing
# ----------------------------
def _fb_error_info(resp: requests.Response) -> Dict[str, Any]:
    try:
        j = resp.json()
        return (j or {}).get("error", {}) or {}
    except Exception:
        return {}


def is_fb_rate_limit(resp: requests.Response) -> bool:
    """
    Facebook "action blocked / rate limited" often shows as:
      HTTP 400
      error.code = 368
      error.error_subcode = 1390008
    """
    err = _fb_error_info(resp)
    return (
        resp.status_code >= 400
        and err.get("code") == 368
        and str(err.get("error_subcode")) == "1390008"
    )


# ----------------------------
# Image loader hook
# ----------------------------
def load_image_bytes(image_ref: str) -> Tuple[bytes, str]:
    """
    Wire this to your existing loader if you ever pass local paths / non-URL refs.
    In this repo, you usually pass URLs, so this won't be hit.
    """
    raise NotImplementedError("Wire this to your existing image loader (bytes, mime_type).")


# ----------------------------
# Posting logic
# ----------------------------
@dataclass
class FBDecision:
    ok_to_post: bool
    reason: str


def should_post_to_facebook(
    state: Dict[str, Any],
    *,
    has_new_social_event: bool,
    now: Optional[datetime] = None,
    cooldown_seconds: int = DEFAULT_FB_COOLDOWN_SECONDS,
) -> FBDecision:
    """
    Prevent overwhelming Facebook:
      - Only post when there's something new (or a deliberate test run)
      - Respect a cooldown between successful posts (fb_cooldown_until or derived from fb_last_posted_at)
      - Respect a block window if FB rate-limited you recently (fb_blocked_until)
    """
    now = now or utc_now()

    if not has_new_social_event:
        return FBDecision(False, "no_new_event")

    fb_blocked_until = _parse_iso(str(state.get("fb_blocked_until", "")))
    if fb_blocked_until and now < fb_blocked_until:
        return FBDecision(False, f"blocked_until_{_iso(fb_blocked_until)}")

    # Prefer explicit cooldown timestamp if present
    fb_cooldown_until = _parse_iso(str(state.get("fb_cooldown_until", "")))
    if fb_cooldown_until and now < fb_cooldown_until:
        return FBDecision(False, f"cooldown_until_{_iso(fb_cooldown_until)}")

    # Back-compat: derive cooldown from last successful post time
    last_ok = _parse_iso(str(state.get("fb_last_posted_at", "")))
    if last_ok:
        derived_until = last_ok + timedelta(seconds=cooldown_seconds)
        if now < derived_until:
            return FBDecision(False, f"cooldown_until_{_iso(derived_until)}")

    return FBDecision(True, "allowed")


def _fb_env() -> Tuple[str, str, str]:
    page_id = os.getenv("FB_PAGE_ID", "").strip()
    page_token = os.getenv("FB_PAGE_ACCESS_TOKEN", "").strip()
    api_ver = os.getenv("FB_API_VERSION", "v24.0").strip()
    if not page_id or not page_token:
        raise RuntimeError("Missing FB_PAGE_ID or FB_PAGE_ACCESS_TOKEN")
    return page_id, page_token, api_ver


def _post(url: str, *, data: Dict[str, Any], files: Optional[Dict[str, Any]] = None) -> requests.Response:
    jitter = max(0.0, float(DEFAULT_FB_JITTER_SECONDS))
    if jitter:
        time.sleep(random.uniform(0.0, jitter))
    return requests.post(url, data=data, files=files, timeout=DEFAULT_FB_TIMEOUT_SECONDS)


def _raise_for_status(resp: requests.Response, label: str) -> None:
    if resp.status_code < 400:
        return
    # Always print body for debug (but keep the job alive higher up)
    print(f"FB error body ({label}):", resp.text)
    raise requests.HTTPError(f"{label} failed {resp.status_code}", response=resp)


def post_to_facebook_page(message: str) -> Dict[str, Any]:
    page_id, page_token, api_ver = _fb_env()
    url = f"https://graph.facebook.com/{api_ver}/{page_id}/feed"
    r = _post(url, data={"message": message, "access_token": page_token})
    print("FB POST /feed status:", r.status_code)
    _raise_for_status(r, "FB /feed")
    return r.json()


def post_photo_to_facebook_page(caption: str, image_ref: str) -> Dict[str, Any]:
    page_id, page_token, api_ver = _fb_env()
    if not image_ref:
        raise RuntimeError("Missing image_ref for FB photo post")

    url = f"https://graph.facebook.com/{api_ver}/{page_id}/photos"

    # Always upload bytes so we can apply local processing (e.g., On511 bug overlay)
    img_bytes, mime_type = load_image_bytes(image_ref)
    r = _post(
        url,
        data={"caption": caption, "access_token": page_token},
        files={"source": ("image", img_bytes, mime_type)},
    )

    print("FB POST /photos status:", r.status_code)
    if r.status_code >= 400:
        print("FB error body:", r.text)
        raise requests.HTTPError(f"FB /photos failed {r.status_code}", response=r)

    return r.json()


def post_carousel_to_facebook_page(caption: str, image_urls: List[str]) -> Dict[str, Any]:
    """
    Posts up to 10 images as a single feed post with attached_media.

    NOTE: If Facebook blocks you (368), it can happen on /photos or /feed.
    If we detect that, we raise an HTTPError so the caller can set fb_blocked_until.
    """
    image_urls = [u for u in (image_urls or []) if (u or "").strip()]

    if not image_urls:
        return post_to_facebook_page(caption)

    if len(image_urls) == 1:
        return post_photo_to_facebook_page(caption, image_urls[0])

    page_id, page_token, api_ver = _fb_env()
    photos_url = f"https://graph.facebook.com/{api_ver}/{page_id}/photos"

    media_fbids: List[str] = []

    for u in image_urls[:10]:
        if re.match(r"^https?://", u, flags=re.IGNORECASE):
            r = _post(photos_url, data={"url": u, "published": "false", "access_token": page_token})
        else:
            img_bytes, mime_type = load_image_bytes(u)
            r = _post(
                photos_url,
                data={"published": "false", "access_token": page_token},
                files={"source": ("image", img_bytes, mime_type)},
            )

        # If we're rate-limited, bail immediately (don't keep firing requests)
        if is_fb_rate_limit(r):
            _raise_for_status(r, "FB /photos (rate-limit)")

        if r.status_code >= 400:
            print("⚠️ FB carousel upload failed for one image:", r.status_code, r.text)
            continue

        j = r.json()
        fbid = j.get("id")
        if fbid:
            media_fbids.append(str(fbid))

    if not media_fbids:
        return post_to_facebook_page(caption)

    if len(media_fbids) == 1:
        return post_photo_to_facebook_page(caption, image_urls[0])

    data: Dict[str, Any] = {"message": caption, "access_token": page_token}
    for i, fbid in enumerate(media_fbids):
        data[f"attached_media[{i}]"] = json.dumps({"media_fbid": fbid})

    feed_url = f"https://graph.facebook.com/{api_ver}/{page_id}/feed"
    r = _post(feed_url, data=data)
    print("FB POST /feed (carousel) status:", r.status_code)

    _raise_for_status(r, "FB /feed (carousel)")
    return r.json()


# ----------------------------
# High-level "safe post" wrapper
# ----------------------------
def safe_post_facebook(
    state: Dict[str, Any],
    *,
    caption: str,
    image_urls: List[str],
    has_new_social_event: bool,
    state_path: str = "state.json",
    cooldown_seconds: int = DEFAULT_FB_COOLDOWN_SECONDS,
    block_seconds: int = DEFAULT_FB_BLOCK_SECONDS,
) -> Dict[str, Any]:
    """
    One FB "decision" per run:
      - If on cooldown or blocked: skip (no API calls)
      - Try: carousel -> single photo -> text
      - If FB rate-limits (368): set fb_blocked_until and SKIP without failing the job
      - Record fb_last_posted_at on success
    """
    now = utc_now()

    decision = should_post_to_facebook(
        state,
        has_new_social_event=has_new_social_event,
        now=now,
        cooldown_seconds=cooldown_seconds,
    )
    if not decision.ok_to_post:
        # Persist derived cooldown so the repo state reflects what the bot is doing
        if decision.reason.startswith("cooldown_until_"):
            state["fb_cooldown_until"] = decision.reason.replace("cooldown_until_", "")
            save_state(state, state_path)
        print(f"FB: skipping ({decision.reason})")
        return {"skipped": True, "reason": decision.reason}

    def _mark_success(mode: str, result: Dict[str, Any]) -> Dict[str, Any]:
        state["fb_last_posted_at"] = _iso(now)
        state["fb_cooldown_until"] = _iso(now + timedelta(seconds=cooldown_seconds))
        state.pop("fb_blocked_until", None)
        save_state(state, state_path)
        return {"ok": True, "mode": mode, "result": result}

    def _mark_blocked(resp: requests.Response) -> Dict[str, Any]:
        blocked_until = now + timedelta(seconds=int(block_seconds))
        state["fb_blocked_until"] = _iso(blocked_until)
        save_state(state, state_path)

        err = _fb_error_info(resp)
        print("⚠️ FB rate-limited (368). Blocking FB until", state["fb_blocked_until"])
        print("FB error parsed:", {
            "status": resp.status_code,
            "code": err.get("code"),
            "error_subcode": err.get("error_subcode"),
            "type": err.get("type"),
            "message": err.get("message"),
            "fbtrace_id": err.get("fbtrace_id"),
        })
        return {"skipped": True, "reason": "fb_rate_limit", "blocked_until": state["fb_blocked_until"]}

    def _handle_http_error(e: requests.HTTPError) -> Optional[Dict[str, Any]]:
        resp = getattr(e, "response", None)
        if resp is not None and is_fb_rate_limit(resp):
            return _mark_blocked(resp)
        return None

    # 1) Carousel
    try:
        return _mark_success("carousel", post_carousel_to_facebook_page(caption, image_urls))
    except requests.HTTPError as e:
        handled = _handle_http_error(e)
        if handled:
            return handled
        print("⚠️ FB carousel failed (non-368). Trying single photo...")

    # 2) Single photo
    try:
        if image_urls:
            return _mark_success("single_photo", post_photo_to_facebook_page(caption, image_urls[0]))
    except requests.HTTPError as e:
        handled = _handle_http_error(e)
        if handled:
            return handled
        print("⚠️ FB single photo failed (non-368). Trying text-only...")

    # 3) Text-only
    try:
        return _mark_success("text", post_to_facebook_page(caption))
    except requests.HTTPError as e:
        handled = _handle_http_error(e)
        if handled:
            return handled

        # Persistent permission/config issue. Don't crash the workflow.
        resp = getattr(e, "response", None)
        if resp is not None:
            err = _fb_error_info(resp)
            print("FB error parsed:", {
                "status": resp.status_code,
                "code": err.get("code"),
                "error_subcode": err.get("error_subcode"),
                "type": err.get("type"),
                "message": err.get("message"),
                "fbtrace_id": err.get("fbtrace_id"),
            })
        print("⚠️ FB text-only failed too. Skipping FB for this run.")
        return {"skipped": True, "reason": "fb_failed_all_modes"}
