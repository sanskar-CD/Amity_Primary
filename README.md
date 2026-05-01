# Lead Upload & Caller Search Dashboard

## Run

1. Install dependencies:
   - `python -m pip install -r requirements.txt`
2. Start app:
   - `python app.py`
3. Open:
   - `http://localhost:5000`

## Demo Login

- Admin: `admin` / `admin123`
- Caller: `caller` / `caller123`

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
