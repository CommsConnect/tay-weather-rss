import os
import datetime as dt
import requests
from typing import Dict, Any, Optional, Callable

# Secrets/env
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# TTL for pending approvals (minutes) — you already added TELEGRAM_APPROVAL_TTL_MIN as a repo secret
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


def tg_send_message(text: str, reply_markup: Optional[Dict[str, Any]] = None) -> None:
    _require_config()
    payload: Dict[str, Any] = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    r = requests.post(_tg_api("sendMessage"), json=payload, timeout=30)
    r.raise_for_status()


def tg_answer_callback_query(callback_query_id: str, text: str = "") -> None:
    _require_config()
    payload: Dict[str, Any] = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    r = requests.post(_tg_api("answerCallbackQuery"), json=payload, timeout=30)
    r.raise_for_status()


def tg_get_updates(offset: Optional[int]) -> Dict[str, Any]:
    _require_config()
    params: Dict[str, Any] = {"timeout": 0}
    if offset is not None:
        params["offset"] = offset
    r = requests.get(_tg_api("getUpdates"), params=params, timeout=30)
    r.raise_for_status()
    return r.json()


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
    # token -> {"created_at": "...Z", "preview_text": "...", "kind": "warning|watch|other", "reminded_at": "...Z"|None}
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
                tg_answer_callback_query(cb_id, "Invalid action.")
                continue

            action, token = cb_data.split(":", 1)
            token = token.strip()

            if action == "go":
                state["approval_decisions"][token] = {"decision": "approved", "decided_at": _utc_now_z()}
                tg_answer_callback_query(cb_id, "Approved ✅")
            elif action == "no":
                state["approval_decisions"][token] = {"decision": "denied", "decided_at": _utc_now_z()}
                tg_answer_callback_query(cb_id, "Denied 🛑")
            else:
                tg_answer_callback_query(cb_id, "Unknown action.")
                continue

            save_fn(state)
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
        state["approval_decisions"][token] = {"decision": decision, "decided_at": _utc_now_z()}
        tg_send_message(f"{'✅' if decision=='approved' else '🛑'} {token} {decision}")
        save_fn(state)


def ensure_preview_sent(
    state: Dict[str, Any],
    save_fn: Callable[[Dict[str, Any]], None],
    token: str,
    preview_text: str,
    kind: str,
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

    msg = (
        f"{preview_text}\n\n"
        f"TOKEN: {token}\n\n"
        f"Tap: ✅ Approve / 🛑 Deny\n"
        f"Or reply: /go {token} or /nogo {token}"
    )
    tg_send_message(msg, reply_markup=_inline_keyboard(token))

    state["pending_approvals"][token] = {
        "created_at": _utc_now_z(),
        "preview_text": preview_text,
        "kind": kind,
        "reminded_at": None,
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
