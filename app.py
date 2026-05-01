import csv
import hashlib
import io
import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from functools import wraps
from html.parser import HTMLParser

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, flash, g, jsonify, redirect, render_template, request, session, url_for


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "leads.db")

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("APP_SECRET_KEY", "change-this-secret-in-production")
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100 MB upload cap
# Keep multipart form fields in memory only up to this limit;
# larger file uploads will be streamed to a temp file by Werkzeug.
app.config["MAX_FORM_MEMORY_SIZE"] = 1 * 1024 * 1024  # 1 MB

# Demo credentials for this implementation.
USERS = {
    "admin": {"password": os.getenv("ADMIN_PASSWORD", "admin123"), "role": "admin"},
    "caller": {"password": os.getenv("CALLER_PASSWORD", "caller123"), "role": "caller"},
}

JOB_STATUS_PENDING = "Pending"
JOB_STATUS_RUNNING = "Running"
JOB_STATUS_COMPLETED = "Completed"
JOB_STATUS_FAILED = "Failed"


def _configure_sqlite_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    # WAL allows concurrent readers while a writer is active and is the most
    # common fix for "database is locked" in web apps.
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError:
        # Some environments (or older SQLite builds) may reject this.
        pass
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")  # 30s
    # Reduce fsync overhead; acceptable for this app and improves bulk uploads.
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(
        DB_PATH,
        timeout=30,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    return _configure_sqlite_connection(conn)


def get_db():
    if "db" not in g:
        g.db = connect_db()
    return g.db


@app.teardown_appcontext
def close_db(_exception):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = connect_db()

    # If the schema already exists, migrate to the "schema-less append-only" format.
    # (Old schema lacked `raw_data` and overwrote on duplicates.)
    table_exists = (
        db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='leads'"
        ).fetchone()
        is not None
    )

    has_raw_data_col = False
    if table_exists:
        cols = db.execute("PRAGMA table_info(leads)").fetchall()
        has_raw_data_col = any(c[1] == "raw_data" for c in cols)

    if table_exists and has_raw_data_col:
        # Ensure newer columns exist on existing schema.
        cols = {c[1] for c in db.execute("PRAGMA table_info(leads)").fetchall()}
        if "lead_date" not in cols:
            db.execute("ALTER TABLE leads ADD COLUMN lead_date TEXT")
        if "source" not in cols:
            db.execute("ALTER TABLE leads ADD COLUMN source TEXT")
        if "synced_at" not in cols:
            db.execute("ALTER TABLE leads ADD COLUMN synced_at TEXT")
        if "updated_at" not in cols:
            db.execute("ALTER TABLE leads ADD COLUMN updated_at TEXT")
        db.execute("CREATE INDEX IF NOT EXISTS idx_mobile_hash ON leads(mobile_hash)")
        db.execute("CREATE TABLE IF NOT EXISTS sync_jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, selected_date TEXT NOT NULL, status TEXT NOT NULL, total_fetched INTEGER NOT NULL DEFAULT 0, inserted_count INTEGER NOT NULL DEFAULT 0, updated_count INTEGER NOT NULL DEFAULT 0, failed_count INTEGER NOT NULL DEFAULT 0, error_message TEXT, started_at TEXT, finished_at TEXT, created_by TEXT, created_at TEXT NOT NULL)")
        db.commit()
        db.close()
        return

    # Create new schema.
    db.execute("DROP TABLE IF EXISTS leads_new")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS leads_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mobile_hash TEXT UNIQUE,
            mobile_missing INTEGER NOT NULL,
            mobile_masked TEXT,
            email_masked TEXT,
            lead_type TEXT NOT NULL CHECK(lead_type IN ('Primary', 'Secondary')),
            name TEXT,
            city TEXT,
            raw_data TEXT NOT NULL,
            created_at TEXT NOT NULL,
            lead_date TEXT,
            source TEXT,
            synced_at TEXT,
            updated_at TEXT
        )
        """
    )

    if table_exists:
        # Best-effort migration of legacy data. We can't reconstruct full "raw_data" so we store `{}`.
        db.execute(
            """
            INSERT INTO leads_new (
                mobile_hash, mobile_missing, mobile_masked, email_masked,
                lead_type, name, city, raw_data, created_at, lead_date, source, synced_at, updated_at
            )
            SELECT
                mobile_hash,
                0 as mobile_missing,
                mobile_masked,
                email_masked,
                lead_type,
                name,
                city,
                '{}' as raw_data,
                uploaded_at as created_at,
                NULL as lead_date,
                'csv_upload' as source,
                NULL as synced_at,
                uploaded_at as updated_at
            FROM leads
            """
        )
        db.execute("DROP TABLE leads")

    db.execute("ALTER TABLE leads_new RENAME TO leads")
    db.execute("CREATE INDEX IF NOT EXISTS idx_mobile_hash ON leads(mobile_hash)")
    db.execute("CREATE TABLE IF NOT EXISTS sync_jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, selected_date TEXT NOT NULL, status TEXT NOT NULL, total_fetched INTEGER NOT NULL DEFAULT 0, inserted_count INTEGER NOT NULL DEFAULT 0, updated_count INTEGER NOT NULL DEFAULT 0, failed_count INTEGER NOT NULL DEFAULT 0, error_message TEXT, started_at TEXT, finished_at TEXT, created_by TEXT, created_at TEXT NOT NULL)")
    db.commit()
    db.close()


def login_required(role=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if "user" not in session:
                return redirect(url_for("login"))
            if role and session.get("role") != role:
                flash("You do not have permission to access this page.", "error")
                return redirect(url_for("dashboard"))
            return func(*args, **kwargs)

        return wrapper

    return decorator


def normalize_mobile(value):
    digits = re.sub(r"\D", "", (value or "").strip())
    if len(digits) < 10:
        return ""
    # Normalize by taking the last 10 digits (works well for many local formats).
    return digits[-10:]


def hash_mobile(mobile):
    return hashlib.sha256(mobile.encode("utf-8")).hexdigest()


def mask_mobile(mobile):
    if len(mobile) < 4:
        return "XXXX"
    return f"{mobile[:2]}XXXXXX{mobile[-2:]}"


def mask_email(email):
    if not email:
        return ""
    cleaned = email.strip()
    if "@" not in cleaned:
        return ""
    local, domain = cleaned.split("@", 1)
    if not local:
        return f"****@{domain}"
    visible = min(2, len(local))
    return f"{local[:visible]}{'*' * max(4, len(local) - visible)}@{domain}"


def normalize_lead_type(raw_value):
    value = (raw_value or "").strip().lower()
    if value == "primary":
        return "Primary"
    if value == "secondary":
        return "Secondary"
    return ""


def mask_mobile_ui(mobile_value):
    """
    UI-only masking for mobile values.
    Database/raw values remain unchanged; this only affects what the caller sees.
    """
    normalized = normalize_mobile(mobile_value)
    if not normalized:
        return "XXXX"
    # Per requirements: render a fixed masked pattern.
    return "98XXXXXX10"


def try_parse_raw_data(raw_data_text):
    if raw_data_text is None:
        return {}
    try:
        parsed = json.loads(raw_data_text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def normalize_header_key(key: str) -> str:
    # Normalize to letters/numbers/underscore for robust matching across CSVs.
    cleaned = (key or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def detect_column(fieldnames, include_substrings, exclude_substrings=None):
    """
    Detect best-matching column name using substring scoring.
    - `include_substrings`: list of tokens; presence increases score
    - `exclude_substrings`: tokens that if present, lower score (e.g. don't match email)
    """
    exclude_substrings = exclude_substrings or []
    best_col = None
    best_score = 0

    for col in fieldnames or []:
        col_norm = normalize_header_key(col)
        if any(ex in col_norm for ex in exclude_substrings):
            continue
        score = 0
        for token in include_substrings:
            tok = normalize_header_key(token)
            if not tok:
                continue
            if tok == col_norm:
                score += 6
            elif tok in col_norm:
                score += 3
        if score > best_score:
            best_score = score
            best_col = col

    return best_col if best_score > 0 else None


def detect_mobile_column(fieldnames):
    # Try common mobile identifiers.
    # Exclude keys that clearly indicate "email".
    return detect_column(
        fieldnames,
        include_substrings=[
            "mobile_number",
            "mobile",
            "phone_number",
            "phone",
            "contact",
        ],
        exclude_substrings=["email"],
    )


def detect_lead_type_column(fieldnames):
    # Look for lead/type-ish columns.
    best_col = None
    best_score = 0
    for col in fieldnames or []:
        col_norm = normalize_header_key(col)
        if "lead" in col_norm and "type" in col_norm:
            score = 6
            if col_norm in {"lead_type", "leadtype"}:
                score = 10
            if score > best_score:
                best_score = score
                best_col = col
    return best_col


def detect_email_column(fieldnames):
    return detect_column(fieldnames, include_substrings=["email", "e_mail", "email_address"], exclude_substrings=[])


def detect_name_column(fieldnames):
    return detect_column(fieldnames, include_substrings=["full_name", "contact_name", "name"], exclude_substrings=["email"])


def detect_city_column(fieldnames):
    best_col = None
    best_score = 0
    for col in fieldnames or []:
        col_norm = normalize_header_key(col)
        score = 0
        if "city" in col_norm:
            score += 6
        if "town" in col_norm:
            score += 4
        if "district" in col_norm:
            score += 2
        if score > best_score:
            best_score = score
            best_col = col
    return best_col


def create_retry_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def parse_date_yyyy_mm_dd(value):
    try:
        return datetime.strptime((value or "").strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def build_portal_urls():
    portal_url = (os.getenv("PORTAL_URL") or "").strip().rstrip("/")
    if not portal_url:
        raise RuntimeError("PORTAL_URL is not configured")
    login_url = (os.getenv("PORTAL_LOGIN_URL") or f"{portal_url}/login").strip()
    leads_url = (os.getenv("PORTAL_LEADS_URL") or f"{portal_url}/leads").strip()
    return portal_url, login_url, leads_url


class SimpleTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_tr = False
        self.in_cell = False
        self.current_row = []
        self.current_cell = []
        self.rows = []

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.in_table = True
        elif self.in_table and tag == "tr":
            self.in_tr = True
            self.current_row = []
        elif self.in_tr and tag in {"td", "th"}:
            self.in_cell = True
            self.current_cell = []

    def handle_data(self, data):
        if self.in_cell:
            self.current_cell.append(data)

    def handle_endtag(self, tag):
        if tag in {"td", "th"} and self.in_cell:
            self.in_cell = False
            self.current_row.append("".join(self.current_cell).strip())
        elif tag == "tr" and self.in_tr:
            self.in_tr = False
            if any(c.strip() for c in self.current_row):
                self.rows.append(self.current_row)
        elif tag == "table":
            self.in_table = False


def parse_records_from_response(response):
    content_type = (response.headers.get("content-type") or "").lower()
    text = response.text or ""
    if "application/json" in content_type or text.strip().startswith(("{", "[")):
        payload = response.json()
        if isinstance(payload, list):
            return payload, {"next_page": None}
        if isinstance(payload, dict):
            candidates = ["data", "results", "items", "leads", "records"]
            for key in candidates:
                val = payload.get(key)
                if isinstance(val, list):
                    pagination = {
                        "next_page": payload.get("next_page")
                        or payload.get("next")
                        or (
                            (payload.get("page", 1) + 1)
                            if payload.get("total_pages") and payload.get("page", 1) < payload.get("total_pages")
                            else None
                        )
                    }
                    return val, pagination
        raise RuntimeError("Unsupported JSON payload structure for lead records")

    if "text/csv" in content_type or ("," in text and "\n" in text):
        reader = csv.DictReader(io.StringIO(text))
        return list(reader), {"next_page": None}

    if "<table" in text.lower():
        parser = SimpleTableParser()
        parser.feed(text)
        if not parser.rows:
            return [], {"next_page": None}
        headers = parser.rows[0]
        records = []
        for row in parser.rows[1:]:
            record = {}
            for i, h in enumerate(headers):
                record[h] = row[i] if i < len(row) else ""
            records.append(record)
        return records, {"next_page": None}

    raise RuntimeError("Unsupported lead response format")


def attempt_portal_login(http_session, login_url, username, password):
    payloads = [
        {"username": username, "password": password},
        {"email": username, "password": password},
        {"userName": username, "password": password},
    ]
    for payload in payloads:
        try:
            resp = http_session.post(login_url, data=payload, timeout=30)
            if resp.status_code in {200, 201, 204, 302}:
                text = (resp.text or "").lower()
                if resp.status_code == 302 or "invalid" not in text:
                    return
        except requests.RequestException:
            continue
    raise RuntimeError("Portal login failed. Check PORTAL credentials and login endpoint.")


def fetch_portal_leads_for_date(http_session, leads_url, selected_date):
    all_records = []
    page = 1
    while True:
        params = {"date": selected_date, "page": page}
        response = http_session.get(leads_url, params=params, timeout=45)
        response.raise_for_status()
        records, pagination = parse_records_from_response(response)
        if not records:
            break
        all_records.extend(records)
        next_page = pagination.get("next_page")
        if next_page in {None, "", False}:
            if len(records) < 1:
                break
            if page > 1000:
                break
            page += 1
            continue
        if isinstance(next_page, int):
            page = next_page
            continue
        break
    return all_records


def to_clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def get_sync_jobs(limit=10):
    db = get_db()
    rows = db.execute(
        """
        SELECT id, selected_date, status, total_fetched, inserted_count, updated_count, failed_count,
               error_message, started_at, finished_at, created_by, created_at
        FROM sync_jobs
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return rows


def create_sync_job(selected_date, created_by):
    now = datetime.now(timezone.utc).isoformat()
    with connect_db() as db:
        cur = db.cursor()
        cur.execute(
            """
            INSERT INTO sync_jobs (selected_date, status, created_by, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (selected_date, JOB_STATUS_PENDING, created_by, now),
        )
        return cur.lastrowid


def update_sync_job(job_id, **kwargs):
    if not kwargs:
        return
    columns = []
    params = []
    for key, value in kwargs.items():
        columns.append(f"{key} = ?")
        params.append(value)
    params.append(job_id)
    with connect_db() as db:
        db.execute(f"UPDATE sync_jobs SET {', '.join(columns)} WHERE id = ?", params)


def merge_existing_row(existing_raw_data, incoming_raw_data):
    merged = dict(existing_raw_data)
    for key, value in incoming_raw_data.items():
        if value is None or (isinstance(value, str) and value.strip() == ""):
            if key not in merged:
                merged[key] = "N/A"
            continue
        merged[key] = value
    return merged


def process_portal_records(records, selected_date):
    inserted = 0
    updated = 0
    failed = 0
    synced_at = datetime.now(timezone.utc).isoformat()

    with connect_db() as db:
        for record in records:
            try:
                if not isinstance(record, dict):
                    failed += 1
                    continue

                raw_row = {str(k): to_clean_str(v) for k, v in record.items()}
                for k, v in list(raw_row.items()):
                    if v == "":
                        raw_row[k] = "N/A"

                fieldnames = list(raw_row.keys())
                mobile_col = detect_mobile_column(fieldnames)
                lead_type_col = detect_lead_type_column(fieldnames)
                email_col = detect_email_column(fieldnames)
                name_col = detect_name_column(fieldnames)
                city_col = detect_city_column(fieldnames)

                mobile_missing = 1
                mobile_hash = None
                mobile_masked = None
                normalized_mobile = ""
                if mobile_col:
                    normalized_mobile = normalize_mobile(raw_row.get(mobile_col))
                    if normalized_mobile:
                        mobile_missing = 0
                        mobile_hash = hash_mobile(normalized_mobile)
                        mobile_masked = mask_mobile(normalized_mobile)

                lead_type = normalize_lead_type(raw_row.get(lead_type_col) if lead_type_col else "") or "Primary"
                email_masked = mask_email(raw_row.get(email_col)) if email_col else None
                name = raw_row.get(name_col) if name_col else None
                city = raw_row.get(city_col) if city_col else None

                existing = None
                if mobile_hash:
                    existing = db.execute(
                        "SELECT id, raw_data FROM leads WHERE mobile_hash = ?",
                        (mobile_hash,),
                    ).fetchone()

                if existing:
                    existing_raw_data = try_parse_raw_data(existing["raw_data"])
                    merged_raw = merge_existing_row(existing_raw_data, raw_row)
                    db.execute(
                        """
                        UPDATE leads
                        SET mobile_missing = ?, mobile_masked = ?, email_masked = COALESCE(?, email_masked),
                            lead_type = COALESCE(?, lead_type), name = COALESCE(?, name), city = COALESCE(?, city),
                            raw_data = ?, lead_date = ?, source = ?, synced_at = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            mobile_missing,
                            mobile_masked,
                            email_masked,
                            lead_type,
                            name,
                            city,
                            json.dumps(merged_raw, ensure_ascii=False),
                            selected_date,
                            "internal_portal",
                            synced_at,
                            synced_at,
                            existing["id"],
                        ),
                    )
                    updated += 1
                    continue

                db.execute(
                    """
                    INSERT INTO leads (
                        mobile_hash, mobile_missing, mobile_masked, email_masked, lead_type, name, city,
                        raw_data, created_at, lead_date, source, synced_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        mobile_hash,
                        mobile_missing,
                        mobile_masked,
                        email_masked,
                        lead_type,
                        name,
                        city,
                        json.dumps(raw_row, ensure_ascii=False),
                        synced_at,
                        selected_date,
                        "internal_portal",
                        synced_at,
                        synced_at,
                    ),
                )
                inserted += 1
            except Exception:
                failed += 1
                continue
    return inserted, updated, failed


def run_sync_job(job_id, selected_date):
    started = datetime.now(timezone.utc).isoformat()
    update_sync_job(job_id, status=JOB_STATUS_RUNNING, started_at=started, error_message=None)
    try:
        username = (os.getenv("PORTAL_USERNAME") or "").strip()
        password = (os.getenv("PORTAL_PASSWORD") or "").strip()
        if not username or not password:
            raise RuntimeError("Portal credentials are missing in environment variables")

        _, login_url, leads_url = build_portal_urls()
        http_session = create_retry_session()
        attempt_portal_login(http_session, login_url, username, password)
        time.sleep(1)
        records = fetch_portal_leads_for_date(http_session, leads_url, selected_date)
        if not records:
            finished = datetime.now(timezone.utc).isoformat()
            update_sync_job(
                job_id,
                status=JOB_STATUS_COMPLETED,
                total_fetched=0,
                inserted_count=0,
                updated_count=0,
                failed_count=0,
                finished_at=finished,
            )
            return

        inserted, updated, failed = process_portal_records(records, selected_date)
        finished = datetime.now(timezone.utc).isoformat()
        update_sync_job(
            job_id,
            status=JOB_STATUS_COMPLETED,
            total_fetched=len(records),
            inserted_count=inserted,
            updated_count=updated,
            failed_count=failed,
            finished_at=finished,
        )
    except Exception as exc:
        finished = datetime.now(timezone.utc).isoformat()
        update_sync_job(job_id, status=JOB_STATUS_FAILED, error_message=str(exc), finished_at=finished)


@app.route("/", methods=["GET"])
def root():
    if "user" in session:
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        user = USERS.get(username)
        if not user or user["password"] != password:
            flash("Invalid credentials.", "error")
            return render_template("login.html")
        session["user"] = username
        session["role"] = user["role"]
        return redirect(url_for("dashboard"))
    return render_template("login.html")


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard", methods=["GET"])
@login_required()
def dashboard():
    role = session.get("role")
    if role == "admin":
        jobs = get_sync_jobs(limit=20)
        return render_template("admin.html", jobs=jobs)
    return render_template("caller.html", result=None)


@app.route("/admin/sync-leads", methods=["POST"])
@login_required(role="admin")
def start_sync_leads():
    selected_date = (request.form.get("selected_date") or "").strip()
    if not parse_date_yyyy_mm_dd(selected_date):
        flash("Please select a valid date in YYYY-MM-DD format.", "error")
        return redirect(url_for("dashboard"))

    job_id = create_sync_job(selected_date, session.get("user"))
    worker = threading.Thread(target=run_sync_job, args=(job_id, selected_date), daemon=True)
    worker.start()
    flash(f"Lead sync job #{job_id} queued for {selected_date}.", "success")
    return redirect(url_for("dashboard"))


@app.route("/admin/sync-jobs", methods=["GET"])
@login_required(role="admin")
def sync_jobs_api():
    jobs = get_sync_jobs(limit=20)
    payload = [
        {
            "id": row["id"],
            "selected_date": row["selected_date"],
            "status": row["status"],
            "total_fetched": row["total_fetched"],
            "inserted_count": row["inserted_count"],
            "updated_count": row["updated_count"],
            "failed_count": row["failed_count"],
            "error_message": row["error_message"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }
        for row in jobs
    ]
    return jsonify(payload)


@app.route("/admin/upload", methods=["POST"])
@login_required(role="admin")
def upload_csv():
    file = request.files.get("csv_file")
    if not file or not file.filename.lower().endswith(".csv"):
        flash("Please upload a valid .csv file.", "error")
        return redirect(url_for("dashboard"))
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()

    # Allow fully flexible CSV structure: no mandatory columns, no enforced header names.
    # We only dynamically *detect* useful columns.
    #
    # Important: do NOT load the full CSV into memory.
    stream = io.TextIOWrapper(file.stream, encoding="utf-8-sig", newline="")
    csv_reader = csv.reader(stream)

    try:
        first_row = next(csv_reader)
    except StopIteration:
        flash("CSV is empty.", "error")
        return redirect(url_for("dashboard"))

    def is_likely_header_row(cells):
        normed = [normalize_header_key(c) for c in cells if c is not None]
        header_tokens = {
            "mobile",
            "phone",
            "contact",
            "email",
            "lead_type",
            "leadtype",
            "lead",
            "type",
            "name",
            "city",
            "town",
            "district",
            "full_name",
        }
        # If any cell resembles a header token, treat it as header.
        return any(any(tok in n for tok in header_tokens) for n in normed)

    def normalize_data_cell_to_str(v):
        return "" if v is None else str(v)

    header_present = is_likely_header_row(first_row)

    if header_present:
        fieldnames = [normalize_data_cell_to_str(v) for v in first_row]

        def row_iter_gen():
            for r in csv_reader:
                # Pad/truncate rows to match header length.
                yield {
                    fieldnames[i]: (r[i] if i < len(r) else "")
                    for i in range(len(fieldnames))
                }

        row_iter = row_iter_gen()
    else:
        # Headerless CSV: generate generic column names like col1..colN
        # and treat the first row as data (streaming, no full-file buffering).
        fieldnames = [f"col{i+1}" for i in range(len(first_row))]

        def row_iter_gen():
            # First row as data
            yield {fieldnames[i]: normalize_data_cell_to_str(first_row[i]) for i in range(len(fieldnames))}
            # Remaining rows
            for r in csv_reader:
                yield {
                    fieldnames[i]: (r[i] if i < len(r) else "")
                    for i in range(len(fieldnames))
                }

        row_iter = row_iter_gen()

    mobile_col = detect_mobile_column(fieldnames)
    lead_type_col = detect_lead_type_column(fieldnames)
    email_col = detect_email_column(fieldnames)
    name_col = detect_name_column(fieldnames)
    city_col = detect_city_column(fieldnames)

    def safe_str(v):
        return "" if v is None else str(v)

    # Flags used for selective overwrites during upsert.
    # "Overwrite only fields present in the new row" is implemented by:
    # - Merging `raw_data` via json_patch (only keys present in the new upload overwrite)
    # - Conditionally updating derived columns only if the source column exists in the new upload
    has_email_col = 1 if email_col else 0
    has_lead_type_col = 1 if lead_type_col else 0
    has_name_col = 1 if name_col else 0
    has_city_col = 1 if city_col else 0

    rows_processed = 0
    batch_size = 500
    params_batch = []

    insert_sql = """
        INSERT INTO leads (
            mobile_hash, mobile_missing, mobile_masked, email_masked,
            lead_type, name, city, raw_data, created_at, lead_date, source, synced_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mobile_hash) DO UPDATE SET
            -- Merge JSON (only keys present in excluded.raw_data overwrite).
            raw_data = json_patch(leads.raw_data, excluded.raw_data),
            mobile_missing = excluded.mobile_missing,
            mobile_masked = excluded.mobile_masked,
            email_masked = CASE WHEN ? = 1 THEN excluded.email_masked ELSE leads.email_masked END,
            lead_type = CASE WHEN ? = 1 THEN excluded.lead_type ELSE leads.lead_type END,
            name = CASE WHEN ? = 1 THEN excluded.name ELSE leads.name END,
            city = CASE WHEN ? = 1 THEN excluded.city ELSE leads.city END,
            created_at = excluded.created_at,
            updated_at = excluded.created_at
    """

    for row in row_iter:
        rows_processed += 1

        raw_row = {k: safe_str(row.get(k)) for k in fieldnames}

        mobile_missing = 1
        mobile_hash = None
        mobile_masked = None

        if mobile_col:
            normalized_mobile = normalize_mobile(raw_row.get(mobile_col))
            if normalized_mobile:
                mobile_missing = 0
                mobile_hash = hash_mobile(normalized_mobile)
                mobile_masked = mask_mobile(normalized_mobile)

        # Lead type context: assume Primary for every uploaded file unless explicitly specified.
        detected_lead_type = ""
        if lead_type_col:
            detected_lead_type = normalize_lead_type(raw_row.get(lead_type_col))
        lead_type = detected_lead_type or "Primary"

        email_masked = None
        if email_col:
            email_masked = mask_email(raw_row.get(email_col))

        name = None
        if name_col:
            name_candidate = (raw_row.get(name_col) or "").strip()
            name = name_candidate or None

        city = None
        if city_col:
            city_candidate = (raw_row.get(city_col) or "").strip()
            city = city_candidate or None

        params_batch.append(
            (
                mobile_hash,
                mobile_missing,
                mobile_masked,
                email_masked,
                lead_type,
                name,
                city,
                json.dumps(raw_row, ensure_ascii=False),
                now,
                None,
                "csv_upload",
                None,
                now,
                has_email_col,
                has_lead_type_col,
                has_name_col,
                has_city_col,
            )
        )

        if len(params_batch) >= batch_size:
            db.executemany(insert_sql, params_batch)
            db.commit()
            params_batch = []

    if params_batch:
        db.executemany(insert_sql, params_batch)

    db.commit()
    flash(
        f"Upload complete. Rows processed: {rows_processed}. Duplicate mobile numbers were updated (upsert).",
        "success",
    )
    return redirect(url_for("dashboard"))


@app.route("/caller/search", methods=["POST"])
@login_required(role="caller")
def caller_search():
    mobile = normalize_mobile(request.form.get("mobile_number") or "")
    if not mobile:
        flash("Enter a valid mobile number.", "error")
        return render_template("caller.html", result=None)

    db = get_db()
    row = db.execute(
        """
        SELECT lead_type, raw_data, mobile_masked, email_masked
        FROM leads
        WHERE mobile_hash = ?
        """,
        (hash_mobile(mobile),),
    ).fetchone()

    if not row:
        return render_template("caller.html", result={"found": False})

    lead_type = row["lead_type"]
    raw_row = try_parse_raw_data(row["raw_data"])

    def to_display_value(v):
        # Treat CSV "empty" / nullish values as N/A for all columns.
        if v is None:
            return "N/A"
        if isinstance(v, str):
            stripped = v.strip()
            if stripped == "":
                return "N/A"
            if stripped.lower() in {"null", "undefined"}:
                return "N/A"
            return v
        # If some unexpected non-string type arrives, display as-is.
        return v

    # Detect sensitive fields dynamically by column name patterns.
    fieldnames = list(raw_row.keys())
    mobile_token_matches = ("mobile", "phone", "contact", "phone_number")
    email_token_matches = ("email", "email_id")

    mobile_fields = []
    email_fields = []
    for col in fieldnames:
        col_norm = normalize_header_key(col)
        if any(tok in col_norm for tok in mobile_token_matches) and "email" not in col_norm:
            mobile_fields.append(col)
        if any(tok in col_norm for tok in email_token_matches):
            email_fields.append(col)

    display_fields = {k: to_display_value(v) for k, v in raw_row.items()}

    # Always mask detected sensitive fields (UI/response formatting only).
    for col in mobile_fields:
        current = raw_row.get(col)
        if current is None:
            continue
        normalized = normalize_mobile(current)
        if not normalized:
            continue
        if display_fields.get(col) != "N/A":
            display_fields[col] = "98XXXXXX10"

    for col in email_fields:
        current = raw_row.get(col)
        if current is None:
            continue
        masked = mask_email(current)
        if not masked:
            continue
        if display_fields.get(col) != "N/A":
            display_fields[col] = masked

    # Fallback for legacy/migrated rows where raw_data isn't available.
    if not raw_row:
        if row["mobile_masked"]:
            display_fields["Mobile Number"] = "98XXXXXX10"
        if row["email_masked"]:
            display_fields["Email"] = row["email_masked"]

    return render_template(
        "caller.html",
        result={
            "found": True,
            "lead_type": lead_type,
            "fields": display_fields,
        },
    )


if __name__ == "__main__":
    init_db()
    # Disable the reloader: it spawns an extra process which can amplify SQLite locking issues.
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
