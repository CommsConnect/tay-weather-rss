# telegram_gate.py
#
# Telegram approve/deny gate + remix/custom controls
# - ✅ Approve: records approved + edits buttons message to confirm (removes buttons)
# - 🛑 Deny: records denied + edits buttons message to confirm "will NOT post" (removes buttons)
# - 🔁 Remix: increments a counter for that token (main script uses this to choose a new care statement)
# - ✏️ Custom: collects custom text via chat for X and/or Facebook, then triggers preview refresh in main script.
#
# IMPORTANT:
# - This file does NOT decide what to post. It only records decisions / inputs.
# - The main script (tay_weather_bot.py) must:
#     - read decision_for(token) and stop unless approved
#     - read remix_count_for(token) and custom_text_for(token)
#     - rebuild text + send a NEW preview when remix/custom changes
#
# Search keys are preserved so you can find behaviours quickly.

import os
import datetime as dt
import requests
from typing import Dict, Any, Optional, Callable, List, Tuple

# Secrets/env
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# TTL and Delay settings
TELEGRAM_APPROVAL_TTL_MIN = int(os.getenv("TELEGRAM_APPROVAL_TTL_MIN", "60"))
TELEGRAM_PREVIEW_DELAY_MIN = int(os.getenv("TELEGRAM_PREVIEW_DELAY_MIN", "10"))
TELEGRAM_REMIND_BEFORE_MIN = int(os.getenv("TELEGRAM_REMIND_BEFORE_MIN", "5"))

# ----------------------------
# Twitter Logic Helper
# ----------------------------
def is_twitter_length_valid(text: str) -> bool:
    """
    Search key: TWITTER_LIMIT_CHECK
    Validates if a string is within the 280 character limit.
    """
    return len(text) <= 280

# ----------------------------
# Telegram API helpers
# ----------------------------
def _tg_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"

def _config_ok() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

def _require_config() -> None:
    if not _config_ok():
        raise RuntimeError("Telegram enabled but TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing")

def tg_send_message(text: str, reply_markup: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    _require_config()
    payload: Dict[str, Any] = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    r = requests.post(_tg_api("sendMessage"), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def tg_edit_message_text(chat_id: str, message_id: int, text: str) -> None:
    _require_config()
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    r = requests.post(_tg_api("editMessageText"), json=payload, timeout=30)
    r.raise_for_status()

def tg_edit_message_reply_markup(chat_id: str, message_id: int, reply_markup: Optional[Dict[str, Any]]) -> None:
    _require_config()
    payload: Dict[str, Any] = {"chat_id": chat_id, "message_id": message_id}
    payload["reply_markup"] = reply_markup if reply_markup is not None else {"inline_keyboard": []}
    r = requests.post(_tg_api("editMessageReplyMarkup"), json=payload, timeout=30)
    r.raise_for_status()

def tg_answer_callback_query_safe(callback_query_id: str, text: str = "", fallback_message: str = "") -> None:
    try:
        payload = {"callback_query_id": callback_query_id, "text": text}
        requests.post(_tg_api("answerCallbackQuery"), json=payload, timeout=30)
    except Exception:
        if fallback_message:
            try:
                tg_send_message(fallback_message)
            except Exception:
                pass

def tg_get_updates(offset: Optional[int]) -> Dict[str, Any]:
    _require_config()
    params: Dict[str, Any] = {"timeout": 0}
    if offset is not None:
        params["offset"] = offset
    r = requests.get(_tg_api("getUpdates"), params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def tg_send_media_group(image_urls: List[str], caption: str = "") -> None:
    _require_config()
    if not image_urls:
        if caption:
            tg_send_message(caption)
        return

    image_urls = image_urls[:10]
    media = []
    for i, url in enumerate(image_urls):
        item = {"type": "photo", "media": url}
        if i == 0 and caption:
            item["caption"] = caption
        media.append(item)

    payload = {"chat_id": TELEGRAM_CHAT_ID, "media": media}
    r = requests.post(_tg_api("sendMediaGroup"), json=payload, timeout=30)
    r.raise_for_status()

# ----------------------------
# Internal State Helpers
# ----------------------------
def _utc_now_z() -> str:
    return dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _parse_iso_z(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _ensure_state_defaults(state: Dict[str, Any]) -> None:
    # Search key: STATE_INITIALIZATION
    state.setdefault("pending_approvals", {})
    state.setdefault("approval_decisions", {})
    state.setdefault("telegram_last_update_id", 0)
    state.setdefault("telegram_remix_count", {})
    state.setdefault("telegram_custom_pending", None)
    state.setdefault("telegram_custom_text", {})
    # Optional helper field to let the main script know something changed
    state.setdefault("telegram_needs_refresh", {})

def _inline_keyboard(token: str) -> Dict[str, Any]:
    # Search key: BUTTON_LAYOUT
    # Creates the 2x2 grid for post management
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Approve", "callback_data": f"go:{token}"},
                {"text": "🛑 Deny", "callback_data": f"no:{token}"},
            ],
            [
                {"text": "🔁 Remix", "callback_data": f"remix:{token}"},
                {"text": "✏️ Custom", "callback_data": f"custom:{token}"},
            ],
        ]
    }

def _buttons_record_for_token(state: Dict[str, Any], token: str) -> Tuple[Optional[str], Optional[int]]:
    pending = (state.get("pending_approvals") or {}).get(token) or {}
    chat_id = (pending.get("buttons_chat_id") or "").strip() or TELEGRAM_CHAT_ID
    msg_id = pending.get("buttons_message_id")
    if not chat_id or not isinstance(msg_id, int) or msg_id <= 0:
        return None, None
    return chat_id, msg_id

def _finalize_buttons_message(state: Dict[str, Any], token: str, final_text: str) -> None:
    """
    Search key: FINALIZE_BUTTONS_MESSAGE
    Edits the original buttons message to show a final status, and removes the keyboard.
    """
    chat_id, msg_id = _buttons_record_for_token(state, token)
    if not chat_id or not msg_id:
        return
    try:
        tg_edit_message_text(chat_id, msg_id, final_text)
    except Exception:
        # If edit fails (e.g., too old), try at least removing keyboard
        pass
    try:
        tg_edit_message_reply_markup(chat_id, msg_id, {"inline_keyboard": []})
    except Exception:
        pass

def is_expired(state: Dict[str, Any], token: str) -> bool:
    """
    Search key: IS_EXPIRED
    Returns True if token is older than TELEGRAM_APPROVAL_TTL_MIN.
    """
    _ensure_state_defaults(state)
    pending = (state.get("pending_approvals") or {}).get(token) or {}
    created_at = _parse_iso_z(pending.get("created_at") or "")
    if not created_at:
        return False  # fail-open: if missing timestamp, treat as not expired here
    age = (dt.datetime.now(dt.timezone.utc) - created_at).total_seconds() / 60.0
    return age >= float(TELEGRAM_APPROVAL_TTL_MIN)

def remix_count_for(state: Dict[str, Any], token: str) -> int:
    """
    Search key: REMIX_COUNT_FOR
    Returns remix count for this token.
    """
    _ensure_state_defaults(state)
    try:
        return int((state.get("telegram_remix_count") or {}).get(token, 0) or 0)
    except Exception:
        return 0

def custom_text_for(state: Dict[str, Any], token: str) -> Dict[str, Optional[str]]:
    """
    Search key: CUSTOM_TEXT_FOR
    Returns {"x": <str|None>, "fb": <str|None>}
    """
    _ensure_state_defaults(state)
    rec = (state.get("telegram_custom_text") or {}).get(token) or {}
    x = rec.get("x")
    fb = rec.get("fb")
    return {"x": x, "fb": fb}

def _flag_refresh(state: Dict[str, Any], token: str) -> None:
    state.setdefault("telegram_needs_refresh", {})
    state["telegram_needs_refresh"][token] = _utc_now_z()

# ----------------------------
# Ingest Actions (The Main Loop)
# ----------------------------
def ingest_telegram_actions(state: Dict[str, Any], save_fn: Callable[[Dict[str, Any]], None]) -> None:
    """
    Search key: MAIN_ACTION_HANDLER
    Processes all incoming Telegram interactions.
    """
    if not _config_ok():
        return

    _ensure_state_defaults(state)

    last_id = state.get("telegram_last_update_id", 0)
    data = tg_get_updates(last_id + 1 if last_id else None)
    if not data.get("ok"):
        return

    for upd in data.get("result", []):
        uid = upd.get("update_id")
        if isinstance(uid, int):
            state["telegram_last_update_id"] = uid

        # 1) HANDLE BUTTON CLICKS
        cb = upd.get("callback_query")
        if cb:
            cb_id = cb.get("id", "")
            cb_data = (cb.get("data") or "").strip()
            if ":" not in cb_data:
                continue

            action, token = cb_data.split(":", 1)
            action = action.strip().lower()
            token = (token or "").strip()
            if not token:
                continue

            if action == "go":
                # Search key: APPROVE_LOGIC
                state["approval_decisions"][token] = {"decision": "approved", "decided_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Approved ✅")

                _finalize_buttons_message(
                    state,
                    token,
                    f"✅ Approved — WILL post\nTOKEN: {token}",
                )

                # once decided, it's no longer pending
                state["pending_approvals"].pop(token, None)

            elif action == "no":
                # Search key: DENY_LOGIC
                state["approval_decisions"][token] = {"decision": "denied", "decided_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Denied 🛑")

                _finalize_buttons_message(
                    state,
                    token,
                    f"🛑 Denied — will NOT post\nTOKEN: {token}",
                )

                # once denied, remove from pending
                state["pending_approvals"].pop(token, None)

            elif action == "remix":
                # Search key: REMIX_LOGIC
                state["telegram_remix_count"][token] = state["telegram_remix_count"].get(token, 0) + 1
                _flag_refresh(state, token)
                tg_answer_callback_query_safe(cb_id, text="Remixing 🔁")

                # Keep it simple: the main script will regenerate + send a NEW preview.
                try:
                    tg_send_message("🔁 Remix requested — regenerating preview with a new Care Statement…")
                except Exception:
                    pass

            elif action == "custom":
                # Search key: CUSTOM_START_LOGIC
                state["telegram_custom_pending"] = {"token": token, "mode": "x", "created_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Custom Text mode")
                try:
                    tg_send_message(
                        "✏️ Custom Text\n\n"
                        "Send the text for the X post.\n"
                        "• Reply /skip to skip X and do Facebook only\n"
                        "• Reply /done to cancel"
                    )
                except Exception:
                    pass

            save_fn(state)
            continue

        # 2) HANDLE TEXT INPUT (For Custom Text Flow)
        msg = upd.get("message") or {}
        text = (msg.get("text") or "").strip()
        if not text:
            continue

        pending_custom = state.get("telegram_custom_pending")
        if not pending_custom:
            continue

        token_p = (pending_custom.get("token") or "").strip()
        mode = (pending_custom.get("mode") or "").strip().lower() or "x"
        if not token_p:
            state["telegram_custom_pending"] = None
            save_fn(state)
            continue

        # Commands
        if text.lower() == "/done":
            state["telegram_custom_pending"] = None
            try:
                tg_send_message("✅ Custom text cancelled.")
            except Exception:
                pass
            save_fn(state)
            continue

        if text.lower() == "/skip" and mode == "x":
            state["telegram_custom_pending"]["mode"] = "fb"
            try:
                tg_send_message("Skipped X. Now send the Facebook custom text:")
            except Exception:
                pass
            save_fn(state)
            continue

        # Store text
        state.setdefault("telegram_custom_text", {}).setdefault(token_p, {"x": None, "fb": None})

        if mode == "x":
            if is_twitter_length_valid(text):
                state["telegram_custom_text"][token_p]["x"] = text
                state["telegram_custom_pending"]["mode"] = "fb"
                try:
                    tg_send_message("✅ X text saved. Now send Facebook text (or /done):")
                except Exception:
                    pass
            else:
                try:
                    tg_send_message(f"⚠️ X text too long ({len(text)}/280). Try again, or /skip:")
                except Exception:
                    pass

            save_fn(state)
            continue

        # mode == "fb"
        state["telegram_custom_text"][token_p]["fb"] = text
        state["telegram_custom_pending"] = None
        _flag_refresh(state, token_p)

        try:
            tg_send_message("✅ Facebook text saved — regenerating preview…")
        except Exception:
            pass

        save_fn(state)

# ----------------------------
# Sending Previews
# ----------------------------
def maybe_send_reminders(state: dict, save_state_fn) -> None:
    """
    Optional: send reminder messages as approvals near expiry.
    Placeholder (safe no-op).
    """
    return

def ensure_preview_sent(
    state: Dict[str, Any],
    save_fn: Callable[[Dict[str, Any]], None],
    token: str,
    preview_text: str,
    kind: str,
    image_urls: Optional[List[str]] = None,
) -> None:
    """
    Search key: PREVIEW_DISPATCH
    Sends the preview and creates the pending record in state.json.
    """
    if not _config_ok():
        return

    _ensure_state_defaults(state)

    token = (token or "").strip()
    if not token:
        return

    # Do not re-send if already pending or already decided
    if token in state["pending_approvals"] or token in state["approval_decisions"]:
        return

    # Send content (images first if provided)
    try:
        if image_urls:
            tg_send_media_group(image_urls, caption=preview_text)
        else:
            tg_send_message(preview_text)
    except Exception:
        # Fall back to text-only
        try:
            tg_send_message(preview_text)
        except Exception:
            return

    # Send buttons message (this is what we edit on approve/deny)
    sent = tg_send_message(
        f"TOKEN: {token}\nSelect an action:",
        reply_markup=_inline_keyboard(token),
    )
    msg_id = sent.get("result", {}).get("message_id", 0)

    state["pending_approvals"][token] = {
        "created_at": _utc_now_z(),
        "preview_text": preview_text,
        "kind": kind,
        "buttons_message_id": int(msg_id) if isinstance(msg_id, int) else 0,
        "buttons_chat_id": TELEGRAM_CHAT_ID,
    }
    save_fn(state)

# ----------------------------
# Decision helpers (imported by tay_weather_bot.py)
# ----------------------------
def decision_for(state: Dict[str, Any], token: str) -> Optional[str]:
    """
    Search key: DECISION_FOR
    Returns: "approved" | "denied" | None
    """
    _ensure_state_defaults(state)
    rec = (state.get("approval_decisions") or {}).get(token) or {}
    d = (rec.get("decision") or "").strip().lower()
    if d in ("approved", "denied"):
        return d
    return None

def is_pending(state: Dict[str, Any], token: str) -> bool:
    """
    Search key: IS_PENDING
    True if token still awaiting a decision.
    """
    _ensure_state_defaults(state)
    return token in (state.get("pending_approvals") or {})

def mark_denied(state: Dict[str, Any], token: str, reason: str = "expired") -> None:
    """
    Search key: MARK_DENIED
    Sets a token to denied (used for TTL expiry).
    """
    _ensure_state_defaults(state)
    state["approval_decisions"][token] = {
        "decision": "denied",
        "decided_at": _utc_now_z(),
        "reason": reason,
    }
    state["pending_approvals"].pop(token, None)
