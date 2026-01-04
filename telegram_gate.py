import os
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


def _safe_post(method: str, payload: Dict[str, Any], timeout: int = 30) -> Optional[Dict[str, Any]]:
    """
    Best-effort POST wrapper. Never raises.
    Returns Telegram JSON dict on success-ish, else None.
    """
    try:
        r = requests.post(_tg_api(method), json=payload, timeout=timeout)
        # Don't raise: Telegram can return 400 for old callback queries, etc.
        if r.status_code != 200:
            print(f"Telegram: {method} failed {r.status_code}: {r.text}")
            return None
        j = r.json()
        if not j.get("ok", False):
            print(f"Telegram: {method} not ok: {j}")
            return None
        return j
    except Exception as e:
        print(f"Telegram: {method} exception: {e}")
        return None


def _safe_get(method: str, params: Dict[str, Any], timeout: int = 30) -> Optional[Dict[str, Any]]:
    """
    Best-effort GET wrapper. Never raises.
    Returns Telegram JSON dict on success-ish, else None.
    """
    try:
        r = requests.get(_tg_api(method), params=params, timeout=timeout)
        if r.status_code != 200:
            print(f"Telegram: {method} failed {r.status_code}: {r.text}")
            return None
        j = r.json()
        if not j.get("ok", False):
            print(f"Telegram: {method} not ok: {j}")
            return None
        return j
    except Exception as e:
        print(f"Telegram: {method} exception: {e}")
        return None


def tg_send_message(text: str, reply_markup: Optional[Dict[str, Any]] = None) -> Optional[int]:
    """
    Sends a message. Returns message_id (int) if available.
    """
    _require_config()
    payload: Dict[str, Any] = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    j = _safe_post("sendMessage", payload, timeout=30)
    if j and isinstance(j.get("result", {}).get("message_id"), int):
        return j["result"]["message_id"]
    return None


def tg_answer_callback_query(callback_query_id: str, text: str = "") -> None:
    """
    Best-effort. Never raises.
    Note: Telegram may 400 if callback query is too old/invalid.
    """
    _require_config()
    if not callback_query_id:
        return

    payload: Dict[str, Any] = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
        payload["show_alert"] = False

    _safe_post("answerCallbackQuery", payload, timeout=15)


def tg_get_updates(offset: Optional[int]) -> Dict[str, Any]:
    """
    Returns dict with {"ok": bool, "result": [...]}. Never raises.
    """
    _require_config()
    params: Dict[str, Any] = {"timeout": 0}
    if offset is not None:
        params["offset"] = offset

    j = _safe_get("getUpdates", params=params, timeout=30)
    return j or {"ok": False, "result": []}


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
    _safe_post("sendMediaGroup", payload, timeout=30)


def tg_edit_message_text(message_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> None:
    """
    Best-effort edit. Never raises.
    """
    _require_config()
    payload: Dict[str, Any] = {
        "chat_id": TELEGRAM_CHAT_ID,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    _safe_post("editMessageText", payload, timeout=30)


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
    # token -> {
    #   "created_at": "...Z",
    #   "preview_text": "...",
    #   "kind": "...",
    #   "reminded_at": "...Z"|None,
    #   "buttons_message_id": int|None
    # }
    state.setdefault("pending_approvals", {})

    # token -> {"decision": "approved|denied", "decided_at": "...Z"}
    state.setdefault("approval_decisions", {})

    state.setdefault("telegram_last_update_id", 0)


def _inline_keyboard(token: str) -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Approve", "callback_data": f"go:{token}"},
                {"text": "🛑 Deny", "callback_data": f"no:{token}"},
            ]
        ]
    }


def _decision_banner(token: str, decision: str) -> str:
    now = _utc_now_z()
    if decision == "approved":
        return f"✅ APPROVED\nTOKEN: {token}\nTime: {now}\n\n(This preview has been approved and will proceed.)"
    return f"🛑 DENIED\nTOKEN: {token}\nTime: {now}\n\n(This preview has been denied and will NOT proceed.)"


def _record_decision_and_signal(
    state: Dict[str, Any],
    save_fn: Callable[[Dict[str, Any]], None],
    token: str,
    decision: str,
    source: str,
    callback_query_id: Optional[str] = None,
) -> None:
    """
    Records decision, signals user (toast + edit buttons message + confirmation message).
    Never raises.
    """
    decision = "approved" if decision == "approved" else "denied"

    state["approval_decisions"][token] = {"decision": decision, "decided_at": _utc_now_z()}

    # Best-effort toast
    if callback_query_id:
        tg_answer_callback_query(callback_query_id, "Approved ✅" if decision == "approved" else "Denied 🛑")

    # Edit the buttons message (permanent confirmation)
    pending = (state.get("pending_approvals") or {}).get(token) or {}
    mid = pending.get("buttons_message_id")
    if isinstance(mid, int):
        # Remove buttons after a decision (so it can't be clicked again)
        tg_edit_message_text(mid, _decision_banner(token, decision), reply_markup=None)

    # Send a separate confirmation message (also permanent + visible)
    tg_send_message(
        f"{'✅ Approved' if decision=='approved' else '🛑 Denied'} recorded.\n"
        f"TOKEN: {token}\n"
        f"Source: {source}\n"
        f"Time: {_utc_now_z()}"
    )

    # Optional cleanup: remove pending record once decided (keeps state tidy)
    if token in state.get("pending_approvals", {}):
        try:
            del state["pending_approvals"][token]
        except Exception:
            pass

    save_fn(state)


# ----------------------------
# Public helpers your bot will call
# ----------------------------
def ingest_telegram_actions(state: Dict[str, Any], save_fn: Callable[[Dict[str, Any]], None]) -> None:
    """
    Reads Telegram updates and records decisions from:
      - inline buttons: go:<token>, no:<token>
      - commands: /go <token>, /nogo <token>

    IMPORTANT:
    - answerCallbackQuery is best-effort and may 400 if the query is too old.
    - We always send permanent confirmation via edit + message.
    """
    if not _config_ok():
        return

    _ensure_state_defaults(state)

    last_update_id = state.get("telegram_last_update_id", 0)
    offset = last_update_id + 1 if isinstance(last_update_id, int) else None

    data = tg_get_updates(offset)
    if not data.get("ok"):
        return

    for upd in data.get("result", []):
        uid = upd.get("update_id")
        if isinstance(uid, int):
            state["telegram_last_update_id"] = uid

        # Inline button callback
        cb = upd.get("callback_query")
        if cb:
            cb_id = (cb.get("id") or "").strip()
            cb_data = (cb.get("data") or "").strip()

            msg = cb.get("message") or {}
            chat = msg.get("chat") or {}
            chat_id = str(chat.get("id", ""))

            if chat_id != TELEGRAM_CHAT_ID:
                continue

            if ":" not in cb_data:
                tg_answer_callback_query(cb_id, "Invalid action.")
                continue

            action, token = cb_data.split(":", 1)
            token = token.strip()

            if action == "go":
                _record_decision_and_signal(state, save_fn, token, "approved", source="button", callback_query_id=cb_id)
            elif action == "no":
                _record_decision_and_signal(state, save_fn, token, "denied", source="button", callback_query_id=cb_id)
            else:
                tg_answer_callback_query(cb_id, "Unknown action.")
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
        _record_decision_and_signal(state, save_fn, token, decision, source="command", callback_query_id=None)


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

    # 2) Send buttons + token as a separate normal message (store message_id for later edits)
    msg = (
        f"TOKEN: {token}\n\n"
        f"Tap: ✅ Approve / 🛑 Deny\n"
        f"Or reply: /go {token} or /nogo {token}\n\n"
        f"TTL: {TELEGRAM_APPROVAL_TTL_MIN} min"
    )
    buttons_mid = tg_send_message(msg, reply_markup=_inline_keyboard(token))

    state["pending_approvals"][token] = {
        "created_at": _utc_now_z(),
        "preview_text": preview_text,
        "kind": kind,
        "reminded_at": None,
        "buttons_message_id": buttons_mid,
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
                f"⏰ Reminder: approval still pending\n"
                f"TOKEN: {token}\n"
                f"~{int(max(0, remaining))} min remaining\n\n"
                f"Tap the buttons or reply:\n/go {token}\n/nogo {token}"
            )

    if changed:
        save_fn(state)
