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
#     * call ensure_preview_sent(...)
#     * call wait_for_decision(...) and respect denied
#     * if remix/custom flags are present -> regenerate preview and call update_preview(...)
#
# SECURITY BEST PRACTICES INCLUDED
# - Never logs secrets
# - Optional allow-list of Telegram user IDs (TELEGRAM_ALLOWED_USER_IDS)
# - Validates callbacks/messages come from the expected chat (when enforceable)
# - Validates token format
# - Auto-heals Telegram group -> supergroup migrations (updates chat_id runtime + retries once)
# - wait_for_decision can reload state from disk each poll (load_state_fn/state_path)

from __future__ import annotations

import os
import re
import json
import time
import datetime as dt
from typing import Dict, Any, Optional, Callable, List

import requests

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()

TELEGRAM_APPROVAL_TTL_MIN = int(os.getenv("TELEGRAM_APPROVAL_TTL_MIN", "60"))
TELEGRAM_REMIND_BEFORE_MIN = int(os.getenv("TELEGRAM_REMIND_BEFORE_MIN", "5"))

# Optional security hardening:
# Comma-separated Telegram numeric user IDs allowed to press buttons / send custom text
# Example: "123456789,987654321"
_ALLOWED = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "").strip()
TELEGRAM_ALLOWED_USER_IDS: Optional[set[int]] = None
if _ALLOWED:
    ids: set[int] = set()
    for part in _ALLOWED.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except Exception:
            pass
    TELEGRAM_ALLOWED_USER_IDS = ids if ids else None


# ---------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------
TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{4,64}$")


def _utc_now_z() -> str:
    return dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"


def is_twitter_length_valid(text: str) -> bool:
    return len(text or "") <= 280


# ---------------------------------------------------------------------
# Telegram API helpers
# ---------------------------------------------------------------------
def _tg_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"


def _config_ok() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def _require_config() -> None:
    if not _config_ok():
        raise RuntimeError("Telegram enabled but TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing")


def _is_allowed_user(from_user_id: Optional[int]) -> bool:
    if TELEGRAM_ALLOWED_USER_IDS is None:
        return True
    if from_user_id is None:
        return False
    return int(from_user_id) in TELEGRAM_ALLOWED_USER_IDS


def _same_chat(chat_id_any: Any) -> bool:
    """
    Ensures the update belongs to the configured chat.
    TELEGRAM_CHAT_ID may be numeric (string) or '@channel'.

    Updates usually give numeric chat_id. If TELEGRAM_CHAT_ID is numeric, enforce exact match.
    If TELEGRAM_CHAT_ID is '@channel', we can't imply numeric id here reliably => allow.
    """
    if not TELEGRAM_CHAT_ID:
        return False
    if str(TELEGRAM_CHAT_ID).startswith("@"):
        return True
    try:
        return str(int(chat_id_any)) == str(int(TELEGRAM_CHAT_ID))
    except Exception:
        return str(chat_id_any) == str(TELEGRAM_CHAT_ID)


def _extract_migrate_to_chat_id(resp: requests.Response) -> Optional[str]:
    """
    Telegram supergroup migration error example:
      400 Bad Request: group chat was upgraded to a supergroup chat
      parameters: {"migrate_to_chat_id": -1001234567890}

    We extract migrate_to_chat_id so we can retry once with the new id.
    """
    try:
        j = resp.json()
        params = (j or {}).get("parameters") or {}
        mig = params.get("migrate_to_chat_id")
        if mig is None:
            return None
        return str(int(mig))
    except Exception:
        return None


def _tg_request(
    method: str,
    *,
    json_payload: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> requests.Response:
    """
    Unified Telegram request with auto-migration retry.
    Retries ONCE if Telegram says the group was upgraded to a supergroup.
    """
    global TELEGRAM_CHAT_ID

    url = _tg_api(method)

    def _do() -> requests.Response:
        if json_payload is not None:
            return requests.post(url, json=json_payload, timeout=timeout)
        return requests.get(url, params=params, timeout=timeout)

    r = _do()

    # Auto-heal: group -> supergroup migration
    if r.status_code == 400:
        mig = _extract_migrate_to_chat_id(r)
        if mig:
            TELEGRAM_CHAT_ID = mig
            if json_payload is not None and "chat_id" in json_payload:
                json_payload["chat_id"] = TELEGRAM_CHAT_ID
            r2 = _do()
            return r2

    return r


def _raise_tg(resp: requests.Response) -> None:
    if resp.ok:
        return
    mig = _extract_migrate_to_chat_id(resp)
    if mig:
        raise RuntimeError(
            f"Telegram chat migrated to supergroup. Update TELEGRAM_CHAT_ID to {mig}. Telegram said: {resp.text}"
        )
    raise RuntimeError(f"Telegram API error {resp.status_code}: {resp.text}")


def tg_send_message(text: str, reply_markup: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Telegram hard limits:
      - message text: 4096 chars
    We chunk to avoid 400 Bad Request.

    NOTE: reply_markup can only be attached to ONE message, so we attach it
    to the LAST chunk.
    """
    _require_config()
    text = (text or "")

    # Keep under Telegram's 4096 hard limit with some buffer.
    max_len = 4000

    chunks: List[str] = []
    while text:
        chunks.append(text[:max_len])
        text = text[max_len:]

    if not chunks:
        chunks = [""]

    last_json: Dict[str, Any] = {"ok": True, "result": None}

    for i, chunk in enumerate(chunks):
        payload: Dict[str, Any] = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
            "disable_web_page_preview": True,
        }

        # Only attach buttons on the final chunk
        if reply_markup is not None and i == (len(chunks) - 1):
            payload["reply_markup"] = reply_markup

        r = _tg_request("sendMessage", json_payload=payload, timeout=30)
        _raise_tg(r)
        last_json = r.json()

    return last_json


def tg_send_media_group(image_urls: List[str], caption: str = "") -> Dict[str, Any]:
    """
    Telegram hard limits:
      - media caption (only first item): 1024 chars
      - up to 10 photos

    Strategy:
      - If caption <= 1024: include it on the first photo
      - If caption > 1024: send media group with a SHORT caption, then send
        the full caption below via tg_send_message() (chunked).
    """
    _require_config()

    if not image_urls:
        return tg_send_message(caption) if caption else {"ok": True, "result": None}

    image_urls = image_urls[:10]
    caption = (caption or "")

    CAPTION_LIMIT = 1024
    overflow_text = ""

    caption_for_media = caption
    if len(caption) > CAPTION_LIMIT:
        # Leave room for our note
        caption_for_media = caption[: CAPTION_LIMIT - 50].rstrip() + "\n\n(Full text sent below.)"
        overflow_text = caption  # send full below after media group

    media = []
    for i, url in enumerate(image_urls):
        item = {"type": "photo", "media": url}
        if i == 0 and caption_for_media:
            item["caption"] = caption_for_media
        media.append(item)

    payload = {"chat_id": TELEGRAM_CHAT_ID, "media": media}
    r = _tg_request("sendMediaGroup", json_payload=payload, timeout=30)
    _raise_tg(r)
    out = r.json()

    if overflow_text:
        tg_send_message(overflow_text)

    return out


def tg_edit_message_text(chat_id: str, message_id: int, text: str) -> None:
    _require_config()
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    r = _tg_request("editMessageText", json_payload=payload, timeout=30)
    _raise_tg(r)


def tg_edit_message_reply_markup(chat_id: str, message_id: int, reply_markup: Optional[Dict[str, Any]]) -> None:
    _require_config()
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "reply_markup": reply_markup if reply_markup is not None else {"inline_keyboard": []},
    }
    r = _tg_request("editMessageReplyMarkup", json_payload=payload, timeout=30)
    _raise_tg(r)


def tg_answer_callback_query_safe(callback_query_id: str, text: str = "") -> None:
    try:
        payload = {"callback_query_id": callback_query_id, "text": text}
        _ = _tg_request("answerCallbackQuery", json_payload=payload, timeout=30)
    except Exception:
        pass


def tg_get_updates(offset: Optional[int]) -> Dict[str, Any]:
    _require_config()
    params: Dict[str, Any] = {"timeout": 0}
    if offset is not None:
        params["offset"] = offset
    r = _tg_request("getUpdates", params=params, timeout=30)
    _raise_tg(r)
    return r.json()


# ---------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------
def _ensure_state_defaults(state: Dict[str, Any]) -> None:
    state.setdefault("pending_approvals", {})          # token -> record
    state.setdefault("approval_decisions", {})         # token -> {decision, decided_at, ...}
    state.setdefault("telegram_last_update_id", 0)
    state.setdefault("telegram_remix_count", {})       # token -> int
    state.setdefault("telegram_custom_pending", None)  # {token, mode, created_at}
    state.setdefault("telegram_custom_text", {})       # token -> {"x": str|None, "fb": str|None}
    state.setdefault("telegram_last_reminder_at", {})  # token -> iso time (avoid spam)


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
            tg_edit_message_reply_markup(str(chat_id), int(msg_id), {"inline_keyboard": []})
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
        tg_edit_message_text(str(chat_id), int(msg_id), new_text)
    except Exception:
        pass

    _disable_buttons(state, token)


# ---------------------------------------------------------------------
# Decision + flags helpers (used by main script)
# ---------------------------------------------------------------------
def decision_for(state: Dict[str, Any], token: str) -> Optional[str]:
    _ensure_state_defaults(state)
    rec = (state.get("approval_decisions") or {}).get(token) or {}
    d = (rec.get("decision") or "").strip().lower()
    if d in ("approved", "denied"):
        return d
    return None


def is_pending(state: Dict[str, Any], token: str) -> bool:
    _ensure_state_defaults(state)
    token = (token or "").strip()
    return token in (state.get("pending_approvals") or {})


def is_expired(state: Dict[str, Any], token: str, ttl_min: Optional[int] = None) -> bool:
    """
    Returns True if the pending approval token is older than ttl_min minutes.
    Uses pending_approvals[token]["created_at"] (ISO string with trailing Z).
    If created_at is missing or unparseable, returns False (fail-open on expiry check).
    """
    _ensure_state_defaults(state)
    token = (token or "").strip()
    if not TOKEN_RE.match(token):
        return False

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


def remix_count_for(state: Dict[str, Any], token: str) -> int:
    _ensure_state_defaults(state)
    return int((state.get("telegram_remix_count") or {}).get(token) or 0)


def custom_text_for(state: Dict[str, Any], token: str) -> Dict[str, Optional[str]]:
    _ensure_state_defaults(state)
    return (state.get("telegram_custom_text") or {}).get(token) or {"x": None, "fb": None}


def clear_custom_text(state: Dict[str, Any], token: str) -> None:
    _ensure_state_defaults(state)
    (state.get("telegram_custom_text") or {}).pop(token, None)
    pc = state.get("telegram_custom_pending")
    if pc and pc.get("token") == token:
        state["telegram_custom_pending"] = None


def mark_denied(state: Dict[str, Any], token: str, reason: str = "expired") -> None:
    _ensure_state_defaults(state)
    state["approval_decisions"][token] = {
        "decision": "denied",
        "decided_at": _utc_now_z(),
        "reason": reason,
    }
    state["pending_approvals"].pop(token, None)


# ---------------------------------------------------------------------
# Main ingest loop
# ---------------------------------------------------------------------
def ingest_telegram_actions(state: Dict[str, Any], save_fn: Callable[[Dict[str, Any]], None]) -> None:
    """
    Pulls getUpdates, records button clicks and custom text into state.json.

    SECURITY:
    - Ignores updates not from TELEGRAM_CHAT_ID (where enforceable)
    - Optional allow-list via TELEGRAM_ALLOWED_USER_IDS
    """
    if not _config_ok():
        return

    _ensure_state_defaults(state)

    last_id = int(state.get("telegram_last_update_id", 0) or 0)
    data = tg_get_updates(last_id + 1 if last_id else None)
    if not data.get("ok"):
        return

    changed = False

    for upd in data.get("result", []):
        uid = upd.get("update_id")
        if isinstance(uid, int):
            state["telegram_last_update_id"] = uid
            changed = True

        # 1) BUTTON CLICKS
        cb = upd.get("callback_query")
        if cb:
            cb_id = cb.get("id", "")
            cb_data = (cb.get("data") or "").strip()

            from_user_id = (cb.get("from") or {}).get("id")
            msg = cb.get("message") or {}
            chat_id = (msg.get("chat") or {}).get("id")

            # enforce chat (when possible)
            if chat_id is not None and not _same_chat(chat_id):
                tg_answer_callback_query_safe(cb_id, text="Wrong chat.")
                continue

            # enforce allow-list (optional)
            if not _is_allowed_user(from_user_id):
                tg_answer_callback_query_safe(cb_id, text="Not authorised.")
                continue

            if ":" not in cb_data:
                continue

            action, token = cb_data.split(":", 1)
            action = action.strip().lower()
            token = token.strip()

            if not TOKEN_RE.match(token):
                tg_answer_callback_query_safe(cb_id, text="Invalid token.")
                continue

            if action == "go":
                state["approval_decisions"][token] = {"decision": "approved", "decided_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Approved ✅")
                _confirm_on_buttons_message(state, token, "✅ Approved — will post.")
                state["pending_approvals"].pop(token, None)
                changed = True

            elif action == "no":
                state["approval_decisions"][token] = {"decision": "denied", "decided_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Denied 🛑")
                _confirm_on_buttons_message(state, token, "🛑 Denied — will NOT post.")
                state["pending_approvals"].pop(token, None)
                changed = True

            elif action == "remix":
                state["telegram_remix_count"][token] = remix_count_for(state, token) + 1
                tg_answer_callback_query_safe(cb_id, text="Remixing 🔁")
                try:
                    tg_send_message("🔁 Remix requested. I’ll generate a new Care Statement + send a new preview.")
                except Exception:
                    pass
                changed = True

            elif action == "custom":
                state["telegram_custom_pending"] = {"token": token, "mode": "x", "created_at": _utc_now_z()}
                tg_answer_callback_query_safe(cb_id, text="Custom text mode ✏️")
                try:
                    tg_send_message(
                        "✏️ Custom Text:\n"
                        "Send the text for the X post.\n\n"
                        "Commands:\n"
                        "  /skip  (skip X, enter FB)\n"
                        "  /done  (cancel)\n"
                    )
                except Exception:
                    pass
                changed = True

            continue

        # 2) CUSTOM TEXT CHAT INPUT
        msg = upd.get("message") or {}
        text = (msg.get("text") or "").strip()
        if not text:
            continue

        from_user_id = (msg.get("from") or {}).get("id")
        chat_id = (msg.get("chat") or {}).get("id")

        if chat_id is not None and not _same_chat(chat_id):
            continue
        if not _is_allowed_user(from_user_id):
            continue

        pending_custom = state.get("telegram_custom_pending")
        if not pending_custom:
            continue

        token_p = (pending_custom.get("token") or "").strip()
        mode = (pending_custom.get("mode") or "x").lower().strip()

        if not TOKEN_RE.match(token_p):
            state["telegram_custom_pending"] = None
            changed = True
            continue

        if text.lower() == "/done":
            state["telegram_custom_pending"] = None
            try:
                tg_send_message("✅ Custom text cancelled.")
            except Exception:
                pass
            changed = True
            continue

        if text.lower() == "/skip" and mode == "x":
            state["telegram_custom_pending"]["mode"] = "fb"
            try:
                tg_send_message("Skipped X. Please send the Facebook custom text (or /done):")
            except Exception:
                pass
            changed = True
            continue

        if text.lower() == "/skip" and mode == "fb":
            state["telegram_custom_pending"] = None
            try:
                tg_send_message("✅ Facebook left as default. I’ll send a new preview.")
            except Exception:
                pass
            changed = True
            continue

        state.setdefault("telegram_custom_text", {}).setdefault(token_p, {"x": None, "fb": None})

        if mode == "x":
            if is_twitter_length_valid(text):
                state["telegram_custom_text"][token_p]["x"] = text
                state["telegram_custom_pending"]["mode"] = "fb"
                try:
                    tg_send_message(
                        "✅ X text saved. Now send Facebook text "
                        "(or /skip to keep default FB /done to cancel):"
                    )
                except Exception:
                    pass
            else:
                try:
                    tg_send_message(f"⚠️ Too long for X ({len(text)}/280). Try again, or /skip:")
                except Exception:
                    pass
            changed = True

        elif mode == "fb":
            state["telegram_custom_text"][token_p]["fb"] = text
            state["telegram_custom_pending"] = None
            try:
                tg_send_message("✅ Facebook text saved. I’ll send a new preview.")
            except Exception:
                pass
            changed = True

    if changed:
        save_fn(state)


# ---------------------------------------------------------------------
# Preview sending / updating
# ---------------------------------------------------------------------
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

    token = (token or "").strip()
    if not TOKEN_RE.match(token):
        raise ValueError("Invalid token format")

    # If we already have pending or decided, do not re-send initial gate
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
        "last_preview_sent_at": _utc_now_z(),
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
    token = (token or "").strip()
    if not TOKEN_RE.match(token):
        return
    if token not in (state.get("pending_approvals") or {}):
        return

    _send_preview_payload(new_preview_text, image_urls)

    state["pending_approvals"][token]["preview_text"] = new_preview_text
    state["pending_approvals"][token]["last_preview_sent_at"] = _utc_now_z()
    save_fn(state)


def _send_preview_payload(preview_text: str, image_urls: Optional[List[str]]) -> None:
    try:
        if image_urls:
            tg_send_media_group(image_urls, caption=f"{preview_text}\n\nUse buttons below to manage.")
        else:
            tg_send_message(preview_text)
    except Exception:
        try:
            tg_send_message("Error sending media, sending text only.")
        except Exception:
            pass
        tg_send_message(preview_text)


# ---------------------------------------------------------------------
# Gate wait helper (MAIN SCRIPT MUST CALL THIS)
# ---------------------------------------------------------------------
def wait_for_decision(
    st: Dict[str, Any],
    token: str,
    save_state_fn: Optional[Callable[[Dict[str, Any]], None]],
    ttl_min: int,
    poll_seconds: int = 2,
    max_wait_seconds: Optional[int] = None,
    poll_interval_seconds: Optional[int] = None,   # alias supported (older callers)
    load_state_fn: Optional[Callable[[], Dict[str, Any]]] = None,  # BEST: reload from disk each poll
    state_path: Optional[str] = None,              # alternative: reload from JSON path each poll
    ingest_each_poll: bool = False,                # optional: call ingest_telegram_actions each poll (needs save_state_fn)
    **kwargs,                                      # swallow unexpected args safely
) -> str:
    """
    Waits for Approve/Deny on a pending token.

    IMPORTANT:
    - If you want this to *actually see* Telegram decisions, provide either:
        * load_state_fn, OR
        * state_path
      so it can reload state each poll.
    - Optionally set ingest_each_poll=True to process new button clicks while waiting
      (useful in Actions where no other loop is running).
    """
    if poll_interval_seconds is not None:
        poll_seconds = int(poll_interval_seconds)

    token = (token or "").strip()
    if not TOKEN_RE.match(token):
        return "denied"

    def _reload() -> Dict[str, Any]:
        if load_state_fn is not None:
            try:
                return load_state_fn() or {}
            except Exception:
                return {}
        if state_path:
            try:
                with open(state_path, "r", encoding="utf-8") as f:
                    return json.load(f) or {}
            except Exception:
                return {}
        return st if isinstance(st, dict) else {}

    st_local = _reload()
    _ensure_state_defaults(st_local)

    pending = (st_local.get("pending_approvals") or {}).get(token)
    if not pending:
        d = decision_for(st_local, token)
        return d if d in ("approved", "denied") else "denied"

    created_at = (pending.get("created_at") or "").strip()
    try:
        created_dt = dt.datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=dt.timezone.utc)
    except Exception:
        created_dt = dt.datetime.now(dt.timezone.utc)

    hard_deadline = created_dt + dt.timedelta(minutes=int(ttl_min))

    soft_deadline = None
    if max_wait_seconds is not None:
        soft_deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=int(max_wait_seconds))

    while True:
        # optional: process new updates while we wait
        if ingest_each_poll and save_state_fn is not None:
            try:
                tmp = _reload()
                ingest_telegram_actions(tmp, save_state_fn)
            except Exception:
                pass

        st_local = _reload()
        _ensure_state_defaults(st_local)

        d = decision_for(st_local, token)
        if d in ("approved", "denied"):
            return d

        # If it disappeared, treat as denied (safe)
        pending = (st_local.get("pending_approvals") or {}).get(token)
        if not pending:
            d2 = decision_for(st_local, token)
            return d2 if d2 in ("approved", "denied") else "denied"

        now = dt.datetime.now(dt.timezone.utc)

        if now >= hard_deadline:
            try:
                mark_denied(st_local, token, reason="expired")
                if save_state_fn:
                    save_state_fn(st_local)
            except Exception:
                pass
            return "denied"

        if soft_deadline and now >= soft_deadline:
            return "denied"

        time.sleep(max(1, int(poll_seconds)))


# ---------------------------------------------------------------------
# Reminders (optional; safe + non-spammy)
# ---------------------------------------------------------------------
def maybe_send_reminders(state: Dict[str, Any], save_state_fn: Callable[[Dict[str, Any]], None]) -> None:
    """
    Sends a single reminder per token when it's close to expiring.
    Also marks expired tokens denied (safe default).
    """
    if not _config_ok():
        return

    _ensure_state_defaults(state)

    ttl_min = TELEGRAM_APPROVAL_TTL_MIN
    remind_before_min = max(1, TELEGRAM_REMIND_BEFORE_MIN)

    now = dt.datetime.now(dt.timezone.utc)
    changed = False

    for token, rec in list((state.get("pending_approvals") or {}).items()):
        created_at = (rec.get("created_at") or "").strip()
        if not created_at:
            continue

        try:
            created_dt = dt.datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            if created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=dt.timezone.utc)
        except Exception:
            continue

        hard_deadline = created_dt + dt.timedelta(minutes=int(ttl_min))
        remind_at = hard_deadline - dt.timedelta(minutes=int(remind_before_min))

        # expired -> deny
        if now >= hard_deadline:
            mark_denied(state, token, reason="expired")
            changed = True
            continue

        if now < remind_at:
            continue

        # one reminder per token
        last_rem = (state.get("telegram_last_reminder_at") or {}).get(token)
        if last_rem:
            continue

        try:
            tg_send_message(
                f"⏳ Approval pending for TOKEN: {token}\n"
                "Reminder: please Approve or Deny before expiry."
            )
            state["telegram_last_reminder_at"][token] = _utc_now_z()
            changed = True
        except Exception:
            pass

    if changed:
        save_state_fn(state)
