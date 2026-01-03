# Tay Township Weather Alerts (Automation)

This repo runs a GitHub Action every 5 minutes to:
- poll Environment and Climate Change Canada (Weather Canada) **regional ATOM alert feed**
- publish/refresh a local RSS file (`tay-weather.xml`)
- optionally post updates to **X** and a **Facebook Page**

Public-facing wording is tailored for Tay Township:
- posts say **“Tay Township area”** (instead of “Midland – Coldwater – Orr Lake”)
- “More info” links point to Tay’s **coords** location page on weather.gc.ca

## Data sources

- Regional ATOM feed (Midland–Coldwater–Orr Lake battleboard): `https://weather.gc.ca/rss/battleboard/onrm94_e.xml`
- Official alert report page: `https://weather.gc.ca/warnings/report_e.html?onrm94`
- Tay conditions page (coords): `https://weather.gc.ca/en/location/index.html?coords=44.751,-79.768`

## Configuration (GitHub Actions env)

The workflow passes these (and the script has safe defaults):

- `ALERT_FEED_URL` (default: `https://weather.gc.ca/rss/battleboard/onrm94_e.xml`)
- `TAY_COORDS_URL` (default: `https://weather.gc.ca/en/location/index.html?coords=44.751,-79.768`)
- `ENABLE_X_POSTING` (`true`/`false`)
- `ENABLE_FB_POSTING` (`true`/`false`)

### Content + media (editable)

This repo includes an Excel workbook you can edit to change:
- *Care statements* that are randomly (but deterministically) selected by **alert color + type**
- *Media rules* to choose a preloaded graphic vs. Ontario 511 cameras
- Optional *custom one-off advisory text*

File: `content_config.xlsx` (commit changes to take effect).

Local images can live in `media/` and be referenced in the workbook (e.g., `media/wind.png`).

### Optional: Telegram GO/NO-GO approval

If enabled, the bot will send a preview to Telegram and wait for you to tap **GO** before it posts to X/Facebook.

Repo variables:
- `ENABLE_TELEGRAM_APPROVAL` (`true`/`false`)
- `ENABLE_X_POSTING`, `ENABLE_FB_POSTING` (defaults are `false` unless you set these)

Repo secrets:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## Required GitHub Secrets

### X (OAuth2 refresh flow)
- `X_CLIENT_ID`
- `X_CLIENT_SECRET`
- `X_REFRESH_TOKEN`

(Optional) If you enable automatic refresh-token rotation, also set:
- `GH_PAT_ACTIONS_SECRETS` (PAT that can update Actions secrets via `gh secret set`)

### Facebook Page
- `FB_PAGE_ID`
- `FB_PAGE_ACCESS_TOKEN`

> Facebook posting is **non-fatal** (workflow stays green if FB token expires).  
> X duplicate-text is treated as a **soft skip**.

## Files

- `tay_weather_bot.py` — main script
- `tay-weather.xml` — generated RSS output (committed by Actions)
- `state.json` — dedupe/cooldown state (committed by Actions)
- `.github/workflows/weather.yml` — scheduled workflow


## Online private config + media (Google)

Set GitHub Secrets:
- GOOGLE_SERVICE_ACCOUNT_JSON
- GOOGLE_SHEET_ID
- GOOGLE_DRIVE_FOLDER_ID (optional, for Drive media)

Optional env:
- CONTENT_CONFIG_SOURCE=auto|google|local (default auto)

In Google Sheet tabs CareStatements / MediaRules / CustomText, set MediaRules.media_kind to `drive` and media_ref to a filename in the shared Drive folder (or `id:<fileId>`).
