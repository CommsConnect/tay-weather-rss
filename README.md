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
