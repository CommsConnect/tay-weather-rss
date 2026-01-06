# telegram_gate.py
#
# Telegram approve/deny gate + remix/custom controls
# - ✅ Approve: records approved + edits buttons message to confirm
# - 🛑 Deny: records denied + edits buttons message to confirm "will NOT post"
# - 🔁 Remix: increments a counter for that token (triggers regeneration in main script)
# - ✏️ Custom: collects custom text via chat for specific platforms.
#
# Logic Overview:
# 1. Platform logic: Checks character counts for Twitter (280 limit).
# 2. State tracking: Uses state.json to remember if the user is typing a custom message.
# ----------------------------------------------------------------------------------

import os
import time
import datetime as dt
import requests
from typing import Dict, Any, Optional, Callable, List

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
            tg_send_message(fallback_message)

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
        if caption: tg_send_message(caption)
        return
    image_urls = image_urls[:10]
    media = []
    for i, url in enumerate(image_urls):
        item = {"type": "photo", "media": url}
        if i == 0 and caption: item["caption"] = caption
        media.append(item)
    payload = {"chat_id": TELEGRAM_CHAT_ID, "media": media}
    r = requests.post(_tg_api("sendMediaGroup"), json=payload, timeout=30)
    r.raise_for_status()

# ----------------------------
# Internal State Helpers
# ----------------------------
def _utc_now_z() -> str:
    return dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _ensure_state_defaults(state: Dict[str, Any]) -> None:
    # Search key: STATE_INITIALIZATION
    state.setdefault("pending_approvals", {})
    state.setdefault("approval_decisions", {})
    state.setdefault("telegram_last_update_id", 0)
    state.setdefault("telegram_remix_count", {})
    state.setdefault("telegram_custom_pending", None)
    state.setdefault("telegram_custom_text", {})

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

# ----------------------------
# Ingest Actions (The Main Loop)
# ----------------------------
def ingest_telegram_actions(state: Dict[str, Any], save_fn: Callable[[Dict[str, Any]], None]) -> None:
    """
    Search key: MAIN_ACTION_HANDLER
    Processes all incoming Telegram interactions.
    """
    if not _config_ok(): return
    _ensure_state_defaults(state)

    last_id = state.get("telegram_last_update_id", 0)
    data = tg_get_updates(last_id + 1 if last_id else None)
    if not data.get("ok"): return

    for upd in data.get("result", []):
        uid = upd.get("update_id")
        if isinstance(uid, int): state["telegram_last_update_id"] = uid

        # 1. HANDLE BUTTON CLICKS
        cb = upd.get("callback_query")
        if cb:
            cb_id = cb.get("id", "")
            cb_data = (cb.get("data") or "").strip()
            if ":" not in cb_data: continue
            
            action, token = cb_data.split(":", 1)
            action = action.strip().lower()

            if action == "go":
                # Search key: APPROVE_LOGIC
                state["approval_decisions"][token] = {"decision": "approved", "decided_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Approved ✅")
                tg_send_message(f"✅ Approved TOKEN: {token}")
                state["pending_approvals"].pop(token, None)

            elif action == "no":
                # Search key: DENY_LOGIC
                state["approval_decisions"][token] = {"decision": "denied", "decided_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Denied 🛑")
                tg_send_message(f"🛑 Denied TOKEN: {token}")
                state["pending_approvals"].pop(token, None)

            elif action == "remix":
                # Search key: REMIX_LOGIC
                # Increments remix count to signal main script to change care statement
                state["telegram_remix_count"][token] = state["telegram_remix_count"].get(token, 0) + 1
                tg_answer_callback_query_safe(cb_id, text="Remixing 🔁")
                tg_send_message("🔁 Remix requested. Picking a new Care Statement and regenerating preview...")

            elif action == "custom":
                # Search key: CUSTOM_START_LOGIC
                # Enters the custom text capture state
                state["telegram_custom_pending"] = {"token": token, "mode": "x", "created_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Custom Text mode")
                tg_send_message("✏️ Custom Text: Send the text for the X (Twitter) post.\n\nReply /skip for FB only, or /done to cancel.")

            save_fn(state)
            continue

        # 2. HANDLE TEXT INPUT (For Custom Text Flow)
        msg = upd.get("message") or {}
        text = (msg.get("text") or "").strip()
        if not text: continue

        pending_custom = state.get("telegram_custom_pending")
        if pending_custom:
            token_p = pending_custom["token"]
            mode = pending_custom["mode"]

            # Handle Commands within Custom Flow
            if text.lower() == "/done":
                state["telegram_custom_pending"] = None
                tg_send_message("✅ Custom text flow finished.")
            elif text.lower() == "/skip" and mode == "x":
                state["telegram_custom_pending"]["mode"] = "fb"
                tg_send_message("Skipped X. Please send the Facebook custom text:")
            else:
                # Store text based on mode
                state.setdefault("telegram_custom_text", {}).setdefault(token_p, {"x": None, "fb": None})
                
                if mode == "x":
                    if is_twitter_length_valid(text):
                        state["telegram_custom_text"][token_p]["x"] = text
                        state["telegram_custom_pending"]["mode"] = "fb"
                        tg_send_message("X text saved! Now send Facebook text (or /done):")
                    else:
                        tg_send_message(f"⚠️ Text too long for Twitter ({len(text)}/280). Try again or /skip:")
                elif mode == "fb":
                    state["telegram_custom_text"][token_p]["fb"] = text
                    state["telegram_custom_pending"] = None
                    tg_send_message("✅ Facebook text saved. Regenerating preview...")
            
            save_fn(state)

# ----------------------------
# Sending Previews
# ----------------------------
def ensure_preview_sent(state: Dict[str, Any], save_fn: Callable[[Dict[str, Any]], None], 
                        token: str, preview_text: str, kind: str, image_urls: Optional[List[str]] = None) -> None:
    """
    Search key: PREVIEW_DISPATCH
    Sends the message and creates the pending record in state.json
    """
    _ensure_state_defaults(state)
    if token in state["pending_approvals"] or token in state["approval_decisions"]: return

    # Send Content
    try:
        if image_urls:
            tg_send_media_group(image_urls, caption=f"{preview_text}\n\nUse buttons below to manage.")
        else:
            tg_send_message(preview_text)
    except Exception:
        tg_send_message("Error sending media, sending text only.")
        tg_send_message(preview_text)

    # Send Buttons
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
