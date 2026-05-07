# Lead sync API (FastAPI + Playwright + MongoDB)

## Prerequisites

- Python 3.11+
- MongoDB 6+ (local or Atlas)
- Network access to install PyPI packages and `playwright install chromium`

## Install

```bash
cd backend
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

Copy `.env.example` to `.env` and set at least:

- `MONGODB_URI`, `MONGODB_DB`
- `PORTAL_URL`, `PORTAL_USERNAME`, `PORTAL_PASSWORD`
- `SCRAPER_SEARCH_PAGE_URL` (full URL to the search screen after login)
- Selector overrides if the defaults do not match your portal DOM

## Run (localhost)

```bash
uvicorn main:app --reload --host localhost --port 8000
```

Open `http://localhost:8000/docs` for interactive API documentation. The Vite React app (`frontend/`) is set to `http://localhost:5173` and proxies API routes to this server.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/fetch-leads` | Body `{"selected_date":"YYYY-MM-DD"}` — queues async scrape; returns `job_id` and `status: running` |
| GET | `/jobs/{job_id}` | Job status: `running`, `completed`, or `failed` |
| GET | `/jobs` | Recent jobs |
| GET | `/leads?date=YYYY-MM-DD` | Stored leads for that `lead_date` |
| GET | `/settings/sync` | `{ "auto_sync_enabled": bool }` |
| PATCH | `/settings/sync` | Body `{"auto_sync_enabled": true\|false}` — manual vs scheduled daily fetch |

## Session persistence

Chromium is launched with `launch_persistent_context` using `PLAYWRIGHT_USER_DATA_DIR`. After a successful login, cookies and storage survive process restarts so you usually avoid re-login until the session expires.

## Scheduler

APScheduler runs a **daily** job at `AUTO_SYNC_CRON_HOUR` / `AUTO_SYNC_CRON_MINUTE` (server local time). The job **no-ops** unless `auto_sync_enabled` is true in Mongo (`app_settings` document), toggled via the API or the React admin UI.

## Celery (optional scale-out)

This project uses FastAPI `BackgroundTasks` plus Mongo-backed job rows for status. For heavier workloads, you can move `run_fetch_job` into a Celery task and keep the same REST contract.

## Legal / compliance

Only automate portals you are authorized to use. Prefer vendor APIs and written integration agreements where possible.
