from telegram_gate import (
    ingest_telegram_actions,
    ensure_preview_sent,
    decision_for,
    is_expired,
    warning_auto_delay_elapsed,
)

alert_kind = classify_alert(title)   # "warning" | "watch" | "other"

token = hashlib.sha1(guid.encode("utf-8")).hexdigest()[:10]

if TELEGRAM_ENABLE_GATE:
    # Always ingest taps/commands first each run
    ingest_telegram_actions(state, save_state)

    # Always send preview (both warnings and watches)
    preview_text = (
        f"🚨 {title}\n"
        f"{social_text}\n\n"
        f"Images: {len(camera_image_urls)}"
    )
    payload = {"guid": guid, "text_hash": h}

    ensure_preview_sent(state, save_state, token, preview_text, payload)

    d = decision_for(state, token)

    if alert_kind == "watch":
        # WATCHES: must explicitly approve
        if d == "denied":
            print("Telegram: denied (watch). Skipping.")
            continue
        if d != "approved":
            print("Telegram: pending (watch). Skipping until approved.")
            continue

    elif alert_kind == "warning":
        # WARNINGS: auto-post after delay unless denied
        if d == "denied":
            print("Telegram: denied (warning). Skipping.")
            continue
        if not warning_auto_delay_elapsed(state, token):
            print("Telegram: warning preview delay not elapsed yet. Skipping this run.")
            continue
        # If approved explicitly, post now; if no decision, auto-post after delay
        # (so do nothing here)

    else:
        # OTHER: keep your current behavior (choose one)
        # safest: require explicit approve
        if d == "denied":
            print("Telegram: denied (other). Skipping.")
            continue
        if d != "approved":
            print("Telegram: pending (other). Skipping until approved.")
            continue

    # Optional: if pending is expired and still not approved for watches/other, treat as no-go
    if alert_kind in ("watch", "other") and is_expired(state, token) and d != "approved":
        print("Telegram: expired (watch/other). Skipping.")
        continue
