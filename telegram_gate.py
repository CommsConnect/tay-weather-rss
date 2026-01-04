import os
import datetime as dt
import requests
from typing import Dict, Any, Optional

# ----------------------------
# Config (env vars)
# ----------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_APPROVAL_TTL_MIN = int(os.getenv("TELEGRAM_APPROVAL_TTL_MIN", "60"))

# ----------------------------
# Telegram helpers
# ----------------------------

def _tg_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"

def tg_send_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Telegram gate enabled but TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing")

    r = requests.post(
        _tg_api("sendMessage"),
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
        },
        timeout=30,
    )
    r.raise_for_status()

def tg_get_updates(offset: Optional[int]) -> Dict[str, Any]:
    params = {"timeout": 0}
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

def _is_expired(created_at_z: str, ttl_min: int) -> bool:
    created = _parse_z(created_at_z)
    return (dt.datetime.now(dt.timezone.utc) - created) > dt.timedelta(minutes=ttl_min)

# ----------------------------
# State helpers
# ----------------------------

def _ensure_state_defaults(state: Dict[str, Any]) -> None:
    state.setdefault("pending_approvals", {})
    state.setdefault("approval_decisions", {})
    state.setdefault("telegram_last_update_id", 0)

# ----------------------------
# Ingest /go and /nogo
# ----------------------------

def ingest_telegram_commands(state: Dict[str, Any], save_fn) -> None:
    """
    Reads Telegram updates and records:
      state["approval_decisions"][token] = {
          "decision": "approved" | "denied",
          "decided_at": ISO8601Z
      }
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
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

        state["approval_decisions"][token] = {
            "decision": decision,
            "decided_at": _utc_now_z(),
        }

        if token in state["pending_approvals"]:
            tg_send_message(f"✅ {token} approved" if decision == "approved" else f"🛑 {token} denied")
        else:
            tg_send_message(f"Recorded {decision} for {token} (not currently pending).")

    save_fn(state)

# ----------------------------
# Main gate function
# ----------------------------

def gate_or_post(
    state: Dict[str, Any],
    save_fn,
    token: str,
    preview_text: str,
    payload_for_later: Dict[str, Any],
) -> str:
    """
    Returns:
      - "approved"
      - "denied"
      - "pending"

    Behavior:
      • Sends Telegram preview once
      • Waits for /go or /nogo on later runs
      • Auto-denies on TTL expiry
    """
    _ensure_state_defaults(state)

    # Always ingest commands first
    ingest_telegram_commands(state, save_fn)

    # If decision already exists, respect it
    decision = state["approval_decisions"].get(token)
    if decision:
        dec = decision.get("decision")
        if dec in ("approved", "denied"):
            return dec

    pending = state["pending_approvals"].get(token)

    # First time seeing this token → send preview
    if not pending:
        state["pending_approvals"][token] = {
            "created_at": _utc_now_z(),
            "payload": payload_for_later,
        }
        save_fn(state)

        msg = (
            f"{preview_text}\n\n"
            f"TOKEN: {token}\n\n"
            f"Reply:\n"
            f"/go {token}\n"
            f"/nogo {token}\n\n"
            f"(expires in {TELEGRAM_APPROVAL_TTL_MIN} min)"
        )
        tg_send_message(msg)
        return "pending"

    # Pending exists → check expiry
    if _is_expired(pending["created_at"], TELEGRAM_APPROVAL_TTL_MIN):
        state["approval_decisions"][token] = {
            "decision": "denied",
            "decided_at": _utc_now_z(),
        }
        state["pending_approvals"].pop(token, None)
        save_fn(state)
        tg_send_message(f"⌛ {token} expired — treated as NO GO.")
        return "denied"

    return "pending"
