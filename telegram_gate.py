# telegram_gate.py
#
# Telegram approve/deny gate + remix/custom controls
# - ✅ Approve: records approved + edits buttons message to confirm
# - 🛑 Deny: records denied + edits buttons message to confirm "will NOT post"
# - 🔁 Remix: increments a counter (main script should regenerate care statement + preview)
# - ✏️ Custom: collects custom text via chat, stores it, and signals main to regenerate preview
#
# IMPORTANT:
# - This file does NOT decide how you generate a “new preview”.
#   It only records intent in state.json and provides helper functions.
# - Main script must:
#     * call ingest_telegram_actions(...)
#     * call wait_for_decision(...) and respect denied
#     * if remix/custom flags are present -> regenerate preview and call update_preview(...)

import os
import datetime as dt
import requests
import time
from typing import Dict, Any, Optional, Callable, List, Tuple

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TELEGRAM_APPROVAL_TTL_MIN = int(os.getenv("TELEGRAM_APPROVAL_TTL_MIN", "60"))
TELEGRAM_REMIND_BEFORE_MIN = int(os.getenv("TELEGRAM_REMIND_BEFORE_MIN", "5"))

# ----------------------------
# Twitter length helper
# ----------------------------
def is_twitter_length_valid(text: str) -> bool:
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
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "reply_markup": reply_markup if reply_markup is not None else {"inline_keyboard": []},
    }
    r = requests.post(_tg_api("editMessageReplyMarkup"), json=payload, timeout=30)
    r.raise_for_status()

def tg_answer_callback_query_safe(callback_query_id: str, text: str = "") -> None:
    try:
        payload = {"callback_query_id": callback_query_id, "text": text}
        requests.post(_tg_api("answerCallbackQuery"), json=payload, timeout=30)
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

def tg_send_media_group(image_urls: List[str], caption: str = "") -> Dict[str, Any]:
    """
    Sends up to 10 photos. Caption only applies to first item.
    Returns Telegram JSON response (ok/result)
    """
    _require_config()
    if not image_urls:
        return tg_send_message(caption) if caption else {"ok": True, "result": None}

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
    return r.json()

# ----------------------------
# State helpers
# ----------------------------
def _utc_now_z() -> str:
    return dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _ensure_state_defaults(state: Dict[str, Any]) -> None:
    state.setdefault("pending_approvals", {})          # token -> record
    state.setdefault("approval_decisions", {})         # token -> {decision, decided_at, ...}
    state.setdefault("telegram_last_update_id", 0)
    state.setdefault("telegram_remix_count", {})       # token -> int
    state.setdefault("telegram_custom_pending", None)  # {token, mode, created_at}
    state.setdefault("telegram_custom_text", {})       # token -> {"x": str|None, "fb": str|None}

def _inline_keyboard(token: str) -> Dict[str, Any]:
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

def _disable_buttons(state: Dict[str, Any], token: str) -> None:
    rec = (state.get("pending_approvals") or {}).get(token) or {}
    chat_id = rec.get("buttons_chat_id")
    msg_id = rec.get("buttons_message_id")
    if chat_id and msg_id:
        try:
            tg_edit_message_reply_markup(chat_id, int(msg_id), {"inline_keyboard": []})
        except Exception:
            pass

def _confirm_on_buttons_message(state: Dict[str, Any], token: str, line: str) -> None:
    """
    Edits the buttons message text to show final confirmation and removes buttons.
    """
    rec = (state.get("pending_approvals") or {}).get(token) or {}
    chat_id = rec.get("buttons_chat_id")
    msg_id = rec.get("buttons_message_id")
    if not (chat_id and msg_id):
        return

    new_text = f"TOKEN: {token}\n{line}"
    try:
        tg_edit_message_text(chat_id, int(msg_id), new_text)
    except Exception:
        pass

    _disable_buttons(state, token)

# ----------------------------
# Decision + flags helpers (used by main script)
# ----------------------------
def decision_for(state: Dict[str, Any], token: str) -> Optional[str]:
    _ensure_state_defaults(state)
    rec = (state.get("approval_decisions") or {}).get(token) or {}
    d = (rec.get("decision") or "").strip().lower()
    if d in ("approved", "denied"):
        return d
    return None

def is_pending(state: Dict[str, Any], token: str) -> bool:
    _ensure_state_defaults(state)
    return token in (state.get("pending_approvals") or {})

def is_expired(state: Dict[str, Any], token: str, ttl_min: Optional[int] = None) -> bool:
    """
    Returns True if the pending approval token is older than ttl_min minutes.

    Uses pending_approvals[token]["created_at"] (ISO string with trailing Z).
    If created_at is missing or unparseable, returns False (fail-open on expiry check).
    """
    _ensure_state_defaults(state)

    if ttl_min is None:
        ttl_min = TELEGRAM_APPROVAL_TTL_MIN

    rec = (state.get("pending_approvals") or {}).get(token) or {}
    created_at = (rec.get("created_at") or "").strip()
    if not created_at:
        return False

    try:
        created_dt = dt.datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=dt.timezone.utc)
        now = dt.datetime.now(dt.timezone.utc)
        age_sec = (now - created_dt).total_seconds()
        return age_sec >= float(ttl_min) * 60.0
    except Exception:
        return False

def mark_denied(state: Dict[str, Any], token: str, reason: str = "expired") -> None:
    _ensure_state_defaults(state)
    state["approval_decisions"][token] = {
        "decision": "denied",
        "decided_at": _utc_now_z(),
        "reason": reason,
    }
    state["pending_approvals"].pop(token, None)

def remix_count_for(state: Dict[str, Any], token: str) -> int:
    _ensure_state_defaults(state)
    return int((state.get("telegram_remix_count") or {}).get(token) or 0)

def custom_text_for(state: Dict[str, Any], token: str) -> Dict[str, Optional[str]]:
    _ensure_state_defaults(state)
    return (state.get("telegram_custom_text") or {}).get(token) or {"x": None, "fb": None}

def clear_custom_text(state: Dict[str, Any], token: str) -> None:
    _ensure_state_defaults(state)
    (state.get("telegram_custom_text") or {}).pop(token, None)
    # also clear pending capture if it matches this token
    pc = state.get("telegram_custom_pending")
    if pc and pc.get("token") == token:
        state["telegram_custom_pending"] = None

# ----------------------------
# Main ingest loop
# ----------------------------
def ingest_telegram_actions(state: Dict[str, Any], save_fn: Callable[[Dict[str, Any]], None]) -> None:
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

        # 1) BUTTON CLICKS
        cb = upd.get("callback_query")
        if cb:
            cb_id = cb.get("id", "")
            cb_data = (cb.get("data") or "").strip()
            if ":" not in cb_data:
                continue

            action, token = cb_data.split(":", 1)
            action = action.strip().lower()
            token = token.strip()

            if action == "go":
                state["approval_decisions"][token] = {"decision": "approved", "decided_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Approved ✅")

                # CONFIRM ON BUTTON MESSAGE
                _confirm_on_buttons_message(state, token, "✅ Approved — will post.")
                # Remove pending so main script won’t treat it as awaiting
                state["pending_approvals"].pop(token, None)

            elif action == "no":
                state["approval_decisions"][token] = {"decision": "denied", "decided_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Denied 🛑")

                # CONFIRM ON BUTTON MESSAGE
                _confirm_on_buttons_message(state, token, "🛑 Denied — will NOT post.")
                state["pending_approvals"].pop(token, None)

            elif action == "remix":
                state["telegram_remix_count"][token] = remix_count_for(state, token) + 1
                tg_answer_callback_query_safe(cb_id, text="Remixing 🔁")
                tg_send_message("🔁 Remix requested. I’ll generate a new Care Statement + send a new preview.")

            elif action == "custom":
                state["telegram_custom_pending"] = {"token": token, "mode": "x", "created_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Custom Text mode")
                tg_send_message(
                    "✏️ Custom Text:\n"
                    "Send the text for the X post.\n\n"
                    "Commands:\n"
                    "  /skip  (skip X, enter FB)\n"
                    "  /done  (cancel)\n"
                )

            save_fn(state)
            continue

        # 2) CUSTOM TEXT CHAT INPUT
        msg = upd.get("message") or {}
        text = (msg.get("text") or "").strip()
        if not text:
            continue

        pending_custom = state.get("telegram_custom_pending")
        if not pending_custom:
            continue

        token_p = pending_custom.get("token")
        mode = (pending_custom.get("mode") or "x").lower().strip()

        if text.lower() == "/done":
            state["telegram_custom_pending"] = None
            tg_send_message("✅ Custom text cancelled.")
            save_fn(state)
            continue

        if text.lower() == "/skip" and mode == "x":
            state["telegram_custom_pending"]["mode"] = "fb"
            tg_send_message("Skipped X. Please send the Facebook custom text (or /done):")
            save_fn(state)
            continue

        state.setdefault("telegram_custom_text", {}).setdefault(token_p, {"x": None, "fb": None})

        if mode == "x":
            if is_twitter_length_valid(text):
                state["telegram_custom_text"][token_p]["x"] = text
                state["telegram_custom_pending"]["mode"] = "fb"
                tg_send_message("✅ X text saved. Now send Facebook text (or /skip to keep default FB /done to cancel):")
            else:
                tg_send_message(f"⚠️ Too long for X ({len(text)}/280). Try again, or /skip:")

        elif mode == "fb":
            state["telegram_custom_text"][token_p]["fb"] = text
            state["telegram_custom_pending"] = None
            tg_send_message("✅ Facebook text saved. I’ll send a new preview.")

        save_fn(state)

# ----------------------------
# Preview sending / updating
# ----------------------------
def ensure_preview_sent(
    state: Dict[str, Any],
    save_fn: Callable[[Dict[str, Any]], None],
    token: str,
    preview_text: str,
    kind: str,
    image_urls: Optional[List[str]] = None,
) -> None:
    """
    Send preview once (initial).
    """
    _ensure_state_defaults(state)
    if token in state["pending_approvals"] or token in state["approval_decisions"]:
        return

    _send_preview_payload(preview_text, image_urls)

    sent = tg_send_message(f"TOKEN: {token}\nSelect an action:", reply_markup=_inline_keyboard(token))
    msg_id = sent.get("result", {}).get("message_id", 0)

    state["pending_approvals"][token] = {
        "created_at": _utc_now_z(),
        "preview_text": preview_text,
        "kind": kind,
        "buttons_message_id": msg_id,
        "buttons_chat_id": TELEGRAM_CHAT_ID,
    }
    save_fn(state)

def update_preview(
    state: Dict[str, Any],
    save_fn: Callable[[Dict[str, Any]], None],
    token: str,
    new_preview_text: str,
    image_urls: Optional[List[str]] = None,
) -> None:
    """
    Send an UPDATED preview for an existing pending token, and refresh stored preview_text.
    Buttons remain the same token.
    """
    _ensure_state_defaults(state)
    if token not in (state.get("pending_approvals") or {}):
        return

    _send_preview_payload(new_preview_text, image_urls)

    # Update stored text so you can audit in state.json
    state["pending_approvals"][token]["preview_text"] = new_preview_text
    save_fn(state)

def _send_preview_payload(preview_text: str, image_urls: Optional[List[str]]) -> None:
    try:
        if image_urls:
            tg_send_media_group(image_urls, caption=f"{preview_text}\n\nUse buttons below to manage.")
        else:
            tg_send_message(preview_text)
    except Exception:
        tg_send_message("Error sending media, sending text only.")
        tg_send_message(preview_text)

# ----------------------------
# Gate wait helper (MAIN SCRIPT MUST CALL THIS)
# ----------------------------
def wait_for_decision(
    load_state_fn: Callable[[], Dict[str, Any]],
    save_state_fn: Callable[[Dict[str, Any]], None],
    token: str,
    ttl_min: Optional[int] = None,
    poll_seconds: float = 2.0,
) -> str:
    """
    Blocks until approved/denied or TTL expiry.
    Returns: "approved" | "denied"
    """
    if ttl_min is None:
        ttl_min = TELEGRAM_APPROVAL_TTL_MIN

    # created_at comes from pending record
    start = time.time()

    while True:
        st = load_state_fn()
        _ensure_state_defaults(st)

        # Always ingest latest button clicks / custom text
        ingest_telegram_actions(st, save_state_fn)

        d = decision_for(st, token)
        if d in ("approved", "denied"):
            return d

        # If token no longer pending, treat as denied for safety
        if not is_pending(st, token):
            return "denied"

        # TTL expiry
        elapsed = time.time() - start
        if elapsed >= float(ttl_min) * 60.0:
            mark_denied(st, token, reason="expired")
            save_state_fn(st)
            # Try to confirm expiry on the buttons message (optional)
            _confirm_on_buttons_message(st, token, "🛑 Timed out — will NOT post.")
            return "denied"

        time.sleep(poll_seconds)

# ----------------------------
# Placeholder reminders
# ----------------------------
def maybe_send_reminders(state: dict, save_state_fn) -> None:
    return
