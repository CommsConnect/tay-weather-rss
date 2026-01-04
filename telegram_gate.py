import os
import time
import datetime as dt
import requests
from typing import Dict, Any, Optional, Callable, List

# Secrets/env
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# TTL for pending approvals (minutes)
TELEGRAM_APPROVAL_TTL_MIN = int(os.getenv("TELEGRAM_APPROVAL_TTL_MIN", "60"))

# For WARNING auto-approve: how long to wait after preview before posting (minutes)
TELEGRAM_PREVIEW_DELAY_MIN = int(os.getenv("TELEGRAM_PREVIEW_DELAY_MIN", "10"))

# Reminder: send a ping when remaining minutes <= this value (optional)
TELEGRAM_REMIND_BEFORE_MIN = int(os.getenv("TELEGRAM_REMIND_BEFORE_MIN", "5"))


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


def tg_answer_callback_query(callback_query_id: str, text: str = "") -> None:
    _require_config()
    payload: Dict[str, Any] = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text

    r = requests.post(_tg_api("answerCallbackQuery"), json=payload, timeout=30)
    r.raise_for_status()


def tg_answer_callback_query_safe(
    callback_query_id: str,
    text: str = "",
    fallback_message: str = "",
) -> None:
    """
    Best-effort callback ack.

    Telegram will return HTTP 400 (e.g., QUERY_ID_INVALID) if the callback query is
    too old / already answered. That should NOT fail the workflow.

    If ack fails, we optionally send a normal message (reliable) as confirmation.
    """
    try:
        tg_answer_callback_query(callback_query_id, text=text)
    except requests.exceptions.RequestException:
        # 400 is common for stale callback queries; ignore it.
        # (We still send a normal message as the 'real' confirmation.)
        if fallback_message:
            try:
                tg_send_message(fallback_message)
            except Exception:
                pass
        # Don't re-raise.


def tg_get_updates(offset: Optional[int]) -> Dict[str, Any]:
    _require_config()
    params: Dict[str, Any] = {"timeout": 0}
    if offset is not None:
        params["offset"] = offset

    r = requests.get(_tg_api("getUpdates"), params=params, timeout=30)
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



def tg_send_media_group(image_urls: List[str], caption: str = "") -> None:
    """
    Sends up to 10 photos as a Telegram album.
    Caption (if provided) is applied to the first photo only (Telegram limitation).
    NOTE: Albums cannot have inline keyboards — send buttons as a separate message.
    """
    _require_config()

    if not image_urls:
        if caption:
            tg_send_message(caption)
        return

    image_urls = image_urls[:10]

    media: List[Dict[str, Any]] = []
    for i, url in enumerate(image_urls):
        item: Dict[str, Any] = {"type": "photo", "media": url}
        if i == 0 and caption:
            item["caption"] = caption
        media.append(item)

    payload: Dict[str, Any] = {"chat_id": TELEGRAM_CHAT_ID, "media": media}

    r = requests.post(_tg_api("sendMediaGroup"), json=payload, timeout=30)
    r.raise_for_status()


# ----------------------------
# Time helpers
# ----------------------------
def _utc_now_z() -> str:
    return dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _parse_z(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _minutes_since(created_at_z: str) -> float:
    created = _parse_z(created_at_z)
    delta = dt.datetime.now(dt.timezone.utc) - created
    return delta.total_seconds() / 60.0


def _minutes_remaining(created_at_z: str, ttl_min: int) -> float:
    return ttl_min - _minutes_since(created_at_z)


# ----------------------------
# State structure
# ----------------------------
def _ensure_state_defaults(state: Dict[str, Any]) -> None:
    # token -> {"created_at": "...Z", "preview_text": "...", "kind": "...", "reminded_at": "...Z"|None}
    state.setdefault("pending_approvals", {})
    # token -> {"decision": "approved|denied", "decided_at": "...Z"}
    state.setdefault("approval_decisions", {})
    state.setdefault("telegram_last_update_id", 0)
    # last signal (handy for confirming in logs / debugging)
    # {"token": "...", "decision": "approved|denied", "decided_at": "...Z"}
    state.setdefault("telegram_last_signal", None)


def _inline_keyboard(token: str) -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Approve", "callback_data": f"go:{token}"},
                {"text": "🛑 Deny", "callback_data": f"no:{token}"},
            ]
        ]
    }


# ----------------------------
# Public helpers your bot will call
# ----------------------------
def ingest_telegram_actions(state: Dict[str, Any], save_fn: Callable[[Dict[str, Any]], None]) -> None:
    """
    Reads Telegram updates and records decisions from:
      - inline buttons: go:<token>, no:<token>
      - commands: /go <token>, /nogo <token>
    """
    if not _config_ok():
        return

    _ensure_state_defaults(state)

    last_update_id = state.get("telegram_last_update_id", 0)
    offset = last_update_id + 1 if isinstance(last_update_id, int) else None

    data = tg_get_updates(offset)
    if not data.get("ok"):
        return

    def _record(token: str, decision: str, decided_at: str, cb_id: Optional[str] = None) -> None:
        # Store decision
        state["approval_decisions"][token] = {"decision": decision, "decided_at": decided_at}
        state["telegram_last_signal"] = {"token": token, "decision": decision, "decided_at": decided_at}
    
        # Pull pending info BEFORE removing it
        pending = (state.get("pending_approvals") or {}).get(token) or {}
        chat_id = str(pending.get("buttons_chat_id") or TELEGRAM_CHAT_ID)
        msg_id = int(pending.get("buttons_message_id") or 0)
    
        stamp = f"{'✅' if decision=='approved' else '🛑'} {decision.upper()} — TOKEN: {token}\n{decided_at}"
    
        # 1) Best-effort button ACK (do not fail run on 400)
        if cb_id:
            tg_answer_callback_query_safe(
                cb_id,
                text=("Approved ✅" if decision == "approved" else "Denied 🛑"),
                fallback_message=""  # we'll do better below
            )
    
        # 2) Edit the original buttons message (this is your visual confirmation)
        edited_ok = False
        if msg_id:
            try:
                tg_edit_message_text(chat_id=chat_id, message_id=msg_id, text=stamp)
                tg_edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)  # clears buttons
                edited_ok = True
            except Exception:
                edited_ok = False
    
        # 3) Reliable fallback confirmation message if we couldn't edit
        if not edited_ok:
            tg_send_message(stamp)
    
        # 4) Now remove pending (optional, but keeps state clean)
        state.get("pending_approvals", {}).pop(token, None)
    
        save_fn(state)

    for upd in data.get("result", []):
        uid = upd.get("update_id")
        if isinstance(uid, int):
            state["telegram_last_update_id"] = uid

        # Inline button callback
        cb = upd.get("callback_query")
        if cb:
            cb_id = cb.get("id", "")
            cb_data = (cb.get("data") or "").strip()

            msg = cb.get("message") or {}
            chat = msg.get("chat") or {}
            chat_id = str(chat.get("id", ""))

            if chat_id != TELEGRAM_CHAT_ID:
                continue

            if ":" not in cb_data:
                tg_answer_callback_query_safe(cb_id, "Invalid action.")
                continue

            action, token = cb_data.split(":", 1)
            token = token.strip()

            if action == "go":
                _record(token=token, decision="approved", decided_at=_utc_now_z(), cb_id=cb_id)
            elif action == "no":
                _record(token=token, decision="denied", decided_at=_utc_now_z(), cb_id=cb_id)
            else:
                tg_answer_callback_query_safe(cb_id, "Unknown action.")
            continue

        # Text commands fallback
        msg = upd.get("message") or {}
        chat = msg.get("chat") or {}
        chat_id = str(chat.get("id", ""))

        if chat_id != TELEGRAM_CHAT_ID:
            continue

        text = (msg.get("text") or "").strip()
        if not text:
            continue

        parts = text.split()
        cmd = parts[0].lower()
        if cmd not in ("/go", "/nogo"):
            continue
        if len(parts) < 2:
            tg_send_message("Usage: /go <token> or /nogo <token>")
            continue

        token = parts[1].strip()
        decision = "approved" if cmd == "/go" else "denied"
        _record(token=token, decision=decision, decided_at=_utc_now_z(), cb_id=None)


def ensure_preview_sent(
    state: Dict[str, Any],
    save_fn: Callable[[Dict[str, Any]], None],
    token: str,
    preview_text: str,
    kind: str,
    image_urls: Optional[List[str]] = None,
) -> None:
    """
    Sends the preview exactly once (stores a pending record).
    kind is informational: 'warning'|'watch'|'other'
    """
    if not _config_ok():
        return

    _ensure_state_defaults(state)

    if token in state["pending_approvals"] or token in state["approval_decisions"]:
        return

    image_urls = image_urls or []

    # 1) Send album/text preview (albums can't have buttons)
    album_caption = f"{preview_text}\n\nTap buttons in the next message to approve/deny."
    try:
        if image_urls:
            tg_send_media_group(image_urls=image_urls, caption=album_caption)
        else:
            tg_send_message(album_caption)
    except Exception:
        tg_send_message(album_caption)

    # 2) Send buttons + token as a separate normal message
    msg = (
        f"TOKEN: {token}\n\n"
        f"Tap: ✅ Approve / 🛑 Deny\n"
        f"Or reply: /go {token} or /nogo {token}"
    )

    sent = tg_send_message(msg, reply_markup=_inline_keyboard(token))
    buttons_message_id = int(((sent.get("result") or {}).get("message_id")) or 0)

    state["pending_approvals"][token] = {
        "created_at": _utc_now_z(),
        "preview_text": preview_text,
        "kind": kind,
        "reminded_at": None,
        "buttons_message_id": buttons_message_id,
        "buttons_chat_id": TELEGRAM_CHAT_ID,
    }
    save_fn(state)


def decision_for(state: Dict[str, Any], token: str) -> Optional[str]:
    rec = (state.get("approval_decisions") or {}).get(token)
    if not rec:
        return None
    d = rec.get("decision")
    return d if d in ("approved", "denied") else None


def pending_created_at(state: Dict[str, Any], token: str) -> Optional[str]:
    rec = (state.get("pending_approvals") or {}).get(token)
    if not rec:
        return None
    return rec.get("created_at")


def is_expired(state: Dict[str, Any], token: str) -> bool:
    created = pending_created_at(state, token)
    if not created:
        return False
    return _minutes_since(created) >= TELEGRAM_APPROVAL_TTL_MIN


def warning_delay_elapsed(state: Dict[str, Any], token: str) -> bool:
    created = pending_created_at(state, token)
    if not created:
        return False
    return _minutes_since(created) >= TELEGRAM_PREVIEW_DELAY_MIN


def maybe_send_reminders(state: Dict[str, Any], save_fn: Callable[[Dict[str, Any]], None]) -> None:
    """
    Sends one reminder for any pending token near expiry.
    Safe to call every run.
    """
    if not _config_ok():
        return

    _ensure_state_defaults(state)

    changed = False
    for token, rec in list(state["pending_approvals"].items()):
        if token in state["approval_decisions"]:
            continue

        created = rec.get("created_at")
        if not created:
            continue

        if is_expired(state, token):
            continue

        remaining = _minutes_remaining(created, TELEGRAM_APPROVAL_TTL_MIN)
        if remaining <= TELEGRAM_REMIND_BEFORE_MIN and not rec.get("reminded_at"):
            rec["reminded_at"] = _utc_now_z()
            changed = True
            tg_send_message(
                f"⏰ Reminder: approval still pending\nTOKEN: {token}\n~{int(max(0, remaining))} min remaining\n\nReply with ✅ Approve or 🛑 Deny."
            )

    if changed:
        save_fn(state)


def wait_for_decision(
    state: Dict[str, Any],
    save_fn: Callable[[Dict[str, Any]], None],
    token: str,
    max_wait_seconds: int = 600,
    poll_interval_seconds: int = 4,
) -> Optional[str]:
    """
    Polls Telegram for a decision for up to max_wait_seconds.
    Returns: "approved", "denied", or None if still pending.
    """
    if not _config_ok():
        return None

    _ensure_state_defaults(state)

    deadline = time.monotonic() + max(0, int(max_wait_seconds))
    while time.monotonic() < deadline:
        ingest_telegram_actions(state, save_fn)
        d = decision_for(state, token)
        if d:
            return d
        if is_expired(state, token):
            return None
        time.sleep(max(1, int(poll_interval_seconds)))

    return None
