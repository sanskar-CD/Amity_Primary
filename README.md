# Lead Upload & Caller Search Dashboard

## Run

1. Install dependencies:
   - `python -m pip install -r requirements.txt`
2. Start app:
   - `python app.py`
3. Open:
   - `http://localhost:5001`

## Demo Login

- Admin: `admin` / `Admin123`
- Caller: `caller` / `Caller123`

Set custom passwords with environment variables:
- `ADMIN_PASSWORD`
- `CALLER_PASSWORD`
- `APP_SECRET_KEY`
- `PORTAL_URL`
- `PORTAL_USERNAME`
- `PORTAL_PASSWORD`
- Optional: `PORTAL_LOGIN_URL`, `PORTAL_LEADS_URL`

## CSV Format

Flexible:
- Any columns/headers are allowed (nothing is strictly required).

Rules used by the system:
- Mobile column is auto-detected using common header patterns like `mobile`, `phone`, `contact`, `phone_number`.
- Lead type defaults to `Primary`.
- If a lead-type column exists (e.g. `lead_type` / contains `lead` and `type`), its value is used.
- Deduplication: if a normalized mobile number is detected and already exists, the new row is skipped (append-only).
- If mobile is missing or the mobile column doesn't exist, the row is still stored and marked with `mobile_missing=true`.

---

## FastAPI + Playwright + MongoDB (lead portal automation)

This repo also includes a **separate** stack under `backend/` and `frontend/`:

- **FastAPI** (`backend/`): `POST /fetch-leads`, `GET /leads?date=YYYY-MM-DD`, job status, auto-sync toggle.
- **Playwright** headless scraper with a **persistent browser profile** (cookies/localStorage) under `PLAYWRIGHT_USER_DATA_DIR`.
- **MongoDB** collection `leads` (flexible `data` document) with dedupe via `dedupe_key` (phone, email, or content hash).
- **React** admin UI (`frontend/`) with a date picker and job polling.

### Setup (all on localhost)

1. **MongoDB**: run on this machine (`mongodb://localhost:27017` is the default in `backend/.env.example`).
2. **Backend**:
   - `cd backend`
   - `python -m venv .venv` then activate it
   - `pip install -r requirements.txt`
   - `playwright install chromium`
   - Copy `backend/.env.example` to `backend/.env` and set portal URLs, selectors, and credentials.
   - `uvicorn main:app --reload --host localhost --port 8000`
3. **Frontend**:
   - `cd frontend`
   - `npm install`
   - `npm run dev` then open **`http://localhost:5173`** (Vite proxies API calls to `http://localhost:8000`).
   - Or set `VITE_API_URL=http://localhost:8000` and ensure `CORS_ORIGINS` includes `http://localhost:5173`.

API docs: `http://localhost:8000/docs` (also proxied from the dev server as `/docs`).

**Important:** Automating login and scraping a third-party site may violate its terms of service or applicable law unless you have explicit permission or ownership of that integration. Prefer official APIs when available.
