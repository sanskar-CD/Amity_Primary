import csv
import hashlib
import io
import json
import os
import random
import re
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from functools import wraps
from typing import Callable, Literal, NamedTuple
from zoneinfo import ZoneInfo
from html.parser import HTMLParser

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, flash, g, jsonify, redirect, render_template, request, session, url_for
from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "leads.db")

# Load environment variables from the repo's `.env` file (if present).
load_dotenv(os.path.join(BASE_DIR, ".env"))

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("APP_SECRET_KEY", "change-this-secret-in-production")
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100 MB upload cap
# Keep multipart form fields in memory only up to this limit;
# larger file uploads will be streamed to a temp file by Werkzeug.
app.config["MAX_FORM_MEMORY_SIZE"] = 1 * 1024 * 1024  # 1 MB

# Demo credentials for this implementation.
USERS = {
    "admin": {"password": "Admin123", "role": "admin"},
    "caller": {"password": "Caller123", "role": "caller"},
}

JOB_STATUS_PENDING = "Pending"
JOB_STATUS_RUNNING = "Running"
JOB_STATUS_COMPLETED = "Completed"
JOB_STATUS_FAILED = "Failed"


class PortalScrapeOutcome(NamedTuple):
    """Return value from Playwright scrape helpers when DB streaming may run mid-scrape."""

    records: list[dict]
    inserted: int
    updated: int
    failed: int
    streamed: bool


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
        _ensure_range_scrape_jobs_table(db)
        _ensure_sync_job_columns(db)
        _ensure_scraper_cron_state_table(db)
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
    _ensure_range_scrape_jobs_table(db)
    _ensure_sync_job_columns(db)
    _ensure_scraper_cron_state_table(db)
    db.commit()
    db.close()


def _ensure_range_scrape_jobs_table(db: sqlite3.Connection) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS range_scrape_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_from TEXT NOT NULL,
            date_to TEXT NOT NULL,
            status TEXT NOT NULL,
            days_total INTEGER NOT NULL DEFAULT 0,
            days_done INTEGER NOT NULL DEFAULT 0,
            current_date TEXT,
            total_fetched INTEGER NOT NULL DEFAULT 0,
            inserted_count INTEGER NOT NULL DEFAULT 0,
            updated_count INTEGER NOT NULL DEFAULT 0,
            failed_count INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            result_summary TEXT,
            started_at TEXT,
            finished_at TEXT,
            created_by TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    cols = {row[1] for row in db.execute("PRAGMA table_info(range_scrape_jobs)").fetchall()}
    if "result_summary" not in cols:
        db.execute("ALTER TABLE range_scrape_jobs ADD COLUMN result_summary TEXT")


def _ensure_sync_job_columns(db: sqlite3.Connection) -> None:
    cols = {row[1] for row in db.execute("PRAGMA table_info(sync_jobs)").fetchall()}
    if "result_summary" not in cols:
        db.execute("ALTER TABLE sync_jobs ADD COLUMN result_summary TEXT")


def _cron_calendar_today_iso() -> str:
    tz_name = (_env("SCRAPER_CRON_TZ", "Asia/Kolkata") or "").strip()
    try:
        return datetime.now(ZoneInfo(tz_name)).date().isoformat()
    except Exception:
        return date.today().isoformat()


def _ensure_scraper_cron_state_table(db: sqlite3.Connection) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS scraper_cron_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            active_date TEXT NOT NULL,
            pages_completed INTEGER NOT NULL DEFAULT 0,
            resume_row_key TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    cols = set([r[1] for r in db.execute("PRAGMA table_info(scraper_cron_state)").fetchall()])
    if "resume_row_key" not in cols:
        db.execute("ALTER TABLE scraper_cron_state ADD COLUMN resume_row_key TEXT")

    row = db.execute("SELECT id FROM scraper_cron_state WHERE id = 1").fetchone()
    if row is None:
        initial = (_env("SCRAPER_CRON_START_DATE", "") or "").strip() or _cron_calendar_today_iso()
        now = datetime.now(timezone.utc).isoformat()
        db.execute(
            """
            INSERT INTO scraper_cron_state (id, active_date, pages_completed, updated_at)
            VALUES (1, ?, 0, ?)
            """,
            (initial, now),
        )


def _cron_get_state() -> tuple[str, int]:
    with connect_db() as db:
        _ensure_scraper_cron_state_table(db)
        row = db.execute(
            "SELECT active_date, pages_completed FROM scraper_cron_state WHERE id = 1"
        ).fetchone()
        db.commit()
    return str(row["active_date"]), int(row["pages_completed"] or 0)


def _cron_set_pages_completed(active_date: str, pages_done: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with connect_db() as db:
        _ensure_scraper_cron_state_table(db)
        db.execute(
            """
            UPDATE scraper_cron_state
            SET pages_completed = ?, updated_at = ?
            WHERE id = 1 AND active_date = ?
            """,
            (pages_done, now, active_date),
        )
        db.commit()




def _cron_get_resume_row_key() -> str | None:
    with connect_db() as db:
        _ensure_scraper_cron_state_table(db)
        row = db.execute("SELECT resume_row_key FROM scraper_cron_state WHERE id = 1").fetchone()
        db.commit()
    if not row:
        return None
    v = row["resume_row_key"]
    return (str(v) if v else None)


def _cron_set_resume_row_key(active_date: str, key: str | None) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with connect_db() as db:
        _ensure_scraper_cron_state_table(db)
        db.execute("""
            UPDATE scraper_cron_state
            SET resume_row_key = ?, updated_at = ?
            WHERE id = 1 AND active_date = ?
            """, (key, now, active_date))
        db.commit()


def _cron_advance_active_date_after_full_day(active_date: str) -> None:
    d = parse_date_yyyy_mm_dd(active_date) or date.today()
    next_d = d + timedelta(days=1)
    now = datetime.now(timezone.utc).isoformat()
    with connect_db() as db:
        _ensure_scraper_cron_state_table(db)
        db.execute(
            """
            UPDATE scraper_cron_state
            SET active_date = ?, pages_completed = 0, updated_at = ?
            WHERE id = 1
            """,
            (next_d.isoformat(), now),
        )
        db.commit()


def _process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if sys.platform == "win32":
        try:
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True,
                text=True,
                timeout=25,
                check=False,
            )
            return str(pid) in (out.stdout or "")
        except Exception:
            return False
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    except OSError:
        return False
    return True


def _cron_lock_path() -> str:
    p = (_env("SCRAPER_CRON_LOCK_PATH", "") or "").strip()
    return p or os.path.join(BASE_DIR, "data", ".scraper_cron.lock")


def _cron_try_acquire_lock() -> bool:
    path = _cron_lock_path()
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if _process_is_running(int(data.get("pid", 0))):
                return False
        except Exception:
            pass
        try:
            os.remove(path)
        except OSError:
            return False
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"pid": os.getpid(), "started": datetime.now(timezone.utc).isoformat()},
                f,
            )
        return True
    except OSError:
        return False


def _cron_release_lock() -> None:
    path = _cron_lock_path()
    try:
        if not os.path.isfile(path):
            return
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if int(data.get("pid", 0)) == os.getpid():
            os.remove(path)
    except Exception:
        pass


def _cron_lock_status() -> dict:
    """
    Return lock info for UI/status checks.
    - locked: lock file exists and pid looks alive
    - pid: best-effort pid from lock file
    - path: lock path
    """
    path = _cron_lock_path()
    info = {"locked": False, "pid": None, "path": path}
    try:
        if not os.path.isfile(path):
            return info
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        pid = int(data.get("pid", 0) or 0)
        info["pid"] = pid if pid > 0 else None
        info["locked"] = bool(pid and _process_is_running(pid))
    except Exception:
        # If the lock file is corrupt, treat as not locked (cron will clean it up on next tick).
        info["locked"] = False
    return info


def run_scraper_cron_tick() -> None:
    """
    One Playwright scrape for the calendar day stored in ``scraper_cron_state`` (see ``SCRAPER_CRON_*`` env vars).
    Single-page mode: resumes from the last row already scraped for today's LMS day (DB marker).
    Intended to be run every 30 minutes from cron or Task Scheduler. Uses a lock file so overlapping runs exit quietly.
    """
    init_db()
    if not _cron_try_acquire_lock():
        print("[cron] Another scraper process holds the lock; skipping this run.")
        return
    started = datetime.now(timezone.utc).isoformat()
    active_date, pages_completed = _cron_get_state()
    resume_row_key = _cron_get_resume_row_key()

    # Single-page cron: always scrape *today* (and keep state pinned to today).
    today_iso = _cron_calendar_today_iso()
    if active_date != today_iso:
        # New day rollover (or state drift) -> reset marker for a clean "resume from top" baseline.
        with connect_db() as db:
            _ensure_scraper_cron_state_table(db)
            now = datetime.now(timezone.utc).isoformat()
            db.execute(
                "UPDATE scraper_cron_state SET active_date = ?, pages_completed = 0, resume_row_key = NULL, updated_at = ? WHERE id = 1",
                (today_iso, now),
            )
            db.commit()
        active_date = today_iso
        pages_completed = 0
        resume_row_key = None

    job_id = create_sync_job(active_date, "cron")
    update_sync_job(
        job_id,
        status=JOB_STATUS_RUNNING,
        started_at=started,
        error_message=None,
        result_summary=f"Cron: resuming {active_date} from last DB marker (single-page mode).",
    )

    try:
        outcome = scrape_portal_records_with_playwright(
            active_date,
            sync_job_id=job_id,
            resume_pages_completed=0,
            on_page_checkpoint=None,
            resume_until_row_key=resume_row_key,
            on_row_key_checkpoint=lambda k: _cron_set_resume_row_key(active_date, k),
        )
        finished = datetime.now(timezone.utc).isoformat()
        if outcome.streamed:
            inserted, updated, failed = outcome.inserted, outcome.updated, outcome.failed
        else:
            inserted, updated, failed = process_portal_records(outcome.records, active_date)
        update_sync_job(
            job_id,
            status=JOB_STATUS_COMPLETED,
            total_fetched=len(outcome.records),
            inserted_count=inserted,
            updated_count=updated,
            failed_count=failed,
            result_summary=(
                f"Cron: tick completed for LMS day {active_date}. "
                f"This run: {len(outcome.records)} row(s); DB {inserted} new, {updated} updated, {failed} failed."
            ),
            finished_at=finished,
        )
    except Exception as exc:
        finished = datetime.now(timezone.utc).isoformat()
        update_sync_job(job_id, status=JOB_STATUS_FAILED, error_message=str(exc), finished_at=finished)
        print(f"[cron] Scrape failed: {exc}")
    finally:
        _sync_job_progress_clear(job_id)
        _cron_release_lock()


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
    # Optional: exact header text from the portal table (see SCRAPER_MOBILE_HEADER in .env).
    force = (os.getenv("SCRAPER_MOBILE_HEADER") or "").strip()
    if force and fieldnames:
        for col in fieldnames:
            if col == force or (col or "").strip() == force:
                return col
        fl = force.lower()
        for col in fieldnames:
            if (col or "").strip().lower() == fl:
                return col
    # Try common mobile identifiers (many portals use non-English or odd labels).
    return detect_column(
        fieldnames,
        include_substrings=[
            "mobile_number",
            "mobile_no",
            "mob_no",
            "primary_mobile",
            "student_mobile",
            "father_mobile",
            "mother_mobile",
            "alternate",
            "whatsapp",
            "cell",
            "gsm",
            "tel",
            "contact_no",
            "mobile",
            "phone_number",
            "phone",
            "contact",
        ],
        exclude_substrings=["email"],
    )


def _infer_single_mobile_from_row_values(raw_row: dict) -> str:
    """
    If the portal uses an unusual header we did not match, but exactly one 10-digit
    mobile appears in the row, use it for hashing so caller search works.
    """
    hits: list[str] = []
    for v in raw_row.values():
        n = normalize_mobile(str(v))
        if n:
            hits.append(n)
    if not hits:
        return ""
    uniq = list(dict.fromkeys(hits))
    if len(uniq) == 1:
        return uniq[0]
    return ""


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


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _human_sleep() -> None:
    """
    Add jitter between actions to reduce bursty automated behavior.
    Configure via SCRAPER_MIN_DELAY_SEC / SCRAPER_MAX_DELAY_SEC (defaults: 3..8).
    """
    try:
        lo = float(_env("SCRAPER_MIN_DELAY_SEC", "3"))
        hi = float(_env("SCRAPER_MAX_DELAY_SEC", "8"))
    except Exception:
        lo, hi = 3.0, 8.0
    if lo <= 0 and hi <= 0:
        return
    if hi < lo:
        lo, hi = hi, lo
    time.sleep(random.uniform(lo, hi))


def _lms_step_sleep() -> None:
    """
    Short jitter between LMS UI steps (dates, search, pagination) — separate from
    SCRAPER_MIN_DELAY_SEC used after login. Defaults ~0.2–0.8s so scrapes finish faster.
    """
    try:
        lo = float(_env("SCRAPER_LMS_STEP_DELAY_MIN_SEC", "0.2"))
        hi = float(_env("SCRAPER_LMS_STEP_DELAY_MAX_SEC", "0.8"))
    except Exception:
        lo, hi = 0.2, 0.8
    if lo <= 0 and hi <= 0:
        return
    if hi < lo:
        lo, hi = hi, lo
    time.sleep(random.uniform(lo, hi))


_range_job_progress_last_ts: dict[int, float] = {}
_sync_job_progress_last_ts: dict[int, float] = {}


def _range_job_progress(job_id: int | None, *, min_interval_sec: float = 1.2, **kwargs) -> None:
    """Best-effort SQLite update so /scraper can show live row counts while Playwright runs."""
    if job_id is None or not kwargs:
        return
    now = time.time()
    force = bool(kwargs.pop("_force", False))
    last = _range_job_progress_last_ts.get(job_id, 0.0)
    if not force and (now - last) < min_interval_sec:
        return
    _range_job_progress_last_ts[job_id] = now
    try:
        update_range_scrape_job(job_id, **kwargs)
    except Exception:
        pass


def _sync_job_progress(job_id: int | None, *, min_interval_sec: float = 0.7, **kwargs) -> None:
    """Best-effort SQLite update so /admin can poll live sync progress while Playwright runs."""
    if job_id is None or not kwargs:
        return
    now = time.time()
    force = bool(kwargs.pop("_force", False))
    last = _sync_job_progress_last_ts.get(job_id, 0.0)
    if not force and (now - last) < min_interval_sec:
        return
    _sync_job_progress_last_ts[job_id] = now
    try:
        update_sync_job(job_id, **kwargs)
    except Exception:
        pass


def _range_job_progress_clear(job_id: int | None) -> None:
    if job_id is not None:
        _range_job_progress_last_ts.pop(job_id, None)


def _sync_job_progress_clear(job_id: int | None) -> None:
    if job_id is not None:
        _sync_job_progress_last_ts.pop(job_id, None)


def _scrape_ui_progress(
    sync_job_id: int | None,
    range_job_id: int | None,
    *,
    min_interval_sec: float = 1.2,
    **kwargs,
) -> None:
    """Update either a dashboard sync job or a range scrape job row (mutually exclusive)."""
    if sync_job_id is not None:
        _sync_job_progress(sync_job_id, min_interval_sec=min_interval_sec, **kwargs)
    elif range_job_id is not None:
        _range_job_progress(range_job_id, min_interval_sec=min_interval_sec, **kwargs)


def _format_portal_date(selected_date_yyyy_mm_dd: str) -> str:
    fmt = _env("SCRAPER_DATE_FORMAT", "YYYY-MM-DD").upper()
    try:
        d = datetime.strptime(selected_date_yyyy_mm_dd, "%Y-%m-%d").date()
    except Exception:
        return selected_date_yyyy_mm_dd
    if fmt in {"DD/MM/YYYY", "DD-MM-YYYY"}:
        sep = "/" if "/" in fmt else "-"
        return f"{d.day:02d}{sep}{d.month:02d}{sep}{d.year:04d}"
    return d.isoformat()


def _pick_selector(page, comma_separated: str):
    for sel in [p.strip() for p in (comma_separated or "").split(",") if p.strip()]:
        loc = page.locator(sel).first
        try:
            if loc.count() > 0:
                return loc
        except Exception:
            continue
    return None


def _pick_selector_any_frame(page, comma_separated: str):
    """
    Try selectors on the main frame and all iframes. Returns (locator, frame_url).
    """
    for frame in page.frames:
        for sel in [p.strip() for p in (comma_separated or "").split(",") if p.strip()]:
            loc = frame.locator(sel).first
            try:
                if loc.count() > 0:
                    return loc, frame.url
            except Exception:
                continue
    return None, ""


def _iter_selectors(comma_separated: str):
    for sel in [p.strip() for p in (comma_separated or "").split(",") if p.strip()]:
        yield sel


def _frames_main_first(page):
    """Main document first — avoids matching duplicate IDs in ad/analytics iframes."""
    mf = page.main_frame
    out = [mf]
    for fr in page.frames:
        if fr != mf:
            out.append(fr)
    return out


def _first_visible_locator_in_frames(page, frames, comma_separated: str, *, max_per_selector: int = 80):
    for frame in frames:
        for sel in _iter_selectors(comma_separated):
            try:
                loc = frame.locator(sel)
                n = loc.count()
            except Exception:
                continue
            for i in range(min(n, max_per_selector)):
                item = loc.nth(i)
                try:
                    if item.is_visible() and item.is_enabled():
                        return item, frame.url
                except Exception:
                    continue
    return None, ""


def _first_visible_locator_prefer_main_frame(page, comma_separated: str, *, max_per_selector: int = 80):
    return _first_visible_locator_in_frames(
        page, _frames_main_first(page), comma_separated, max_per_selector=max_per_selector
    )


# Amity Lead/LMS/Index — "Search Data" panel (export uses #exportFromDate in a hidden section).
_AMITY_LMS_SEARCH_FROM_FALLBACK = "#searchFromDate,input#searchFromDate,input[name='searchFromDate']"
_AMITY_LMS_SEARCH_TO_FALLBACK = "#searchToDate,input#searchToDate,input[name='searchToDate']"


def _portal_visible_date_range_pair(page, date_from_sel: str, date_to_sel: str):
    """
    First visible From/To pair. Tries .env selectors, then Amity LMS search-field IDs
    (page HTML uses searchFromDate / searchToDate, not FromDate / ToDate).
    Resolves on the main frame first so third-party iframes cannot steal #searchFromDate matches.
    """
    pairs: list[tuple[str, str]] = []
    if (date_from_sel or "").strip() and (date_to_sel or "").strip():
        pairs.append((date_from_sel.strip(), date_to_sel.strip()))
    pairs.append((_AMITY_LMS_SEARCH_FROM_FALLBACK, _AMITY_LMS_SEARCH_TO_FALLBACK))
    for fs, ts in pairs:
        a, _ = _first_visible_locator_prefer_main_frame(page, fs)
        b, _ = _first_visible_locator_prefer_main_frame(page, ts)
        if a and b:
            return a, b
    return None, None


def _first_visible_locator_any_frame(page, comma_separated: str, *, max_per_selector: int = 80):
    """
    Many portals contain multiple matching nodes (hidden templates, mobile nav, etc.).
    Pick the first locator that is actually visible and enabled.
    """
    return _first_visible_locator_in_frames(page, page.frames, comma_separated, max_per_selector=max_per_selector)


def _portal_click_search_button(page, search_btn_sel: str, timeout_ms: int) -> None:
    """
    Clicks the portal control that runs the lead search **after** dates are filled.
    (Do not confuse with 'Search Data', which opens the date panel — see SCRAPER_SEARCH_DATA_SELECTOR.)
    """
    search_btn = None
    search_btn, _url = _first_visible_locator_prefer_main_frame(page, search_btn_sel)
    if not search_btn:
        search_btn, _url = _first_visible_locator_any_frame(page, search_btn_sel)
    if not search_btn:
        for frame in page.frames:
            try:
                for pattern in (
                    re.compile(r"^search$", re.I),
                    re.compile(r"^go$", re.I),
                    re.compile(r"submit", re.I),
                ):
                    cand = frame.get_by_role("button", name=pattern)
                    n = cand.count()
                    for i in range(min(n, 40)):
                        item = cand.nth(i)
                        try:
                            if item.is_visible() and item.is_enabled():
                                search_btn = item
                                break
                        except Exception:
                            continue
                    if search_btn:
                        break
                if search_btn:
                    break
            except Exception:
                continue

    if not search_btn:
        for frame in page.frames:
            try:
                cand = frame.get_by_role("link", name=re.compile(r"search", re.I))
                for i in range(min(cand.count(), 20)):
                    item = cand.nth(i)
                    if item.is_visible() and item.is_enabled():
                        search_btn = item
                        break
            except Exception:
                continue
            if search_btn:
                break

    if not search_btn:
        out_dir = _write_debug_artifacts(page, "search_button_not_found")
        raise RuntimeError(
            "No visible control found to run the search after dates. "
            "Set SCRAPER_RESULTS_SEARCH_BUTTON_SELECTOR (and/or SCRAPER_SEARCH_BUTTON_SELECTOR) "
            "to the **visible** button on the LMS page (not a hidden template). "
            f"Debug: {out_dir}"
        )

    try:
        search_btn.scroll_into_view_if_needed(timeout=min(timeout_ms, 8000))
    except Exception:
        pass
    try:
        search_btn.wait_for(state="visible", timeout=min(timeout_ms, 8000))
    except Exception:
        pass

    click_timeout = min(timeout_ms, int(_env("SCRAPER_SEARCH_CLICK_TIMEOUT_MS", "12000")))
    last_exc: Exception | None = None
    for attempt in range(1, 5):
        try:
            if attempt == 1:
                search_btn.click(timeout=click_timeout, no_wait_after=True)
            elif attempt == 2:
                search_btn.click(timeout=click_timeout, force=True, no_wait_after=True)
            elif attempt == 3:
                # JS click bypasses actionability checks (good when overlays confuse Playwright).
                search_btn.evaluate("(el) => el && el.click && el.click()")
            else:
                # Dispatch a mouse click event.
                search_btn.evaluate(
                    """(el) => {
                      if (!el) return;
                      const ev = new MouseEvent('click', { bubbles: true, cancelable: true, view: window });
                      el.dispatchEvent(ev);
                    }"""
                )
            return
        except Exception as exc:
            last_exc = exc
            try:
                time.sleep(0.4)
            except Exception:
                pass

    out_dir = _write_debug_artifacts(page, "search_click_failed")
    raise RuntimeError(
        "Found the Search button, but could not click it successfully (actionability/overlay issue). "
        f"Try adjusting SCRAPER_RESULTS_SEARCH_BUTTON_SELECTOR. Debug: {out_dir}. Last error: {last_exc}"
    )


def _portal_open_search_data_panel(page, cfg: dict, timeout_ms: int) -> None:
    """Click 'Search Data' (or equivalent) so the date / calendar UI appears."""
    sels = str(cfg.get("search_data_sel") or "").strip()
    if not sels:
        return
    btn, _ = _first_visible_locator_prefer_main_frame(page, sels)
    if not btn:
        btn, _ = _first_visible_locator_any_frame(page, sels)
    if btn:
        btn.scroll_into_view_if_needed(timeout=min(timeout_ms, 10000))
        btn.click(timeout=min(timeout_ms, 30000))
        time.sleep(0.6)
        return
    for frame in page.frames:
        try:
            loc = frame.get_by_text("Search Data", exact=False)
            for i in range(min(loc.count(), 25)):
                item = loc.nth(i)
                try:
                    if item.is_visible() and item.is_enabled():
                        item.scroll_into_view_if_needed(timeout=10000)
                        item.click(timeout=30000)
                        time.sleep(0.6)
                        return
                except Exception:
                    continue
        except Exception:
            continue
    if _env("SCRAPER_REQUIRE_SEARCH_DATA_CLICK", "false").lower() in {"1", "true", "yes"}:
        out_dir = _write_debug_artifacts(page, "search_data_not_found")
        raise RuntimeError(
            "Could not click 'Search Data' to open the date panel. "
            f"Tune SCRAPER_SEARCH_DATA_SELECTOR. Debug: {out_dir}"
        )


def _portal_wait_for_calendar_or_date_inputs(page, cfg: dict, timeout_ms: int) -> None:
    """Wait until calendar overlay or date inputs are visible (AJAX UI)."""
    cal_sel = str(cfg.get("calendar_wait_sel") or "").strip()
    date_sel = str(cfg["date_sel"])
    date_from_sel = _env("SCRAPER_DATE_FROM_SELECTOR", "").strip()
    date_to_sel = _env("SCRAPER_DATE_TO_SELECTOR", "").strip()
    wait_ms = int(_env("SCRAPER_CALENDAR_WAIT_MS", "45000"))
    deadline = time.time() + min(wait_ms, max(timeout_ms, 5000)) / 1000.0
    poll = float(_env("SCRAPER_RESULTS_POLL_SEC", "0.5"))
    while time.time() < deadline:
        if cal_sel:
            loc = page.locator(cal_sel).first
            try:
                if loc.count() > 0 and loc.is_visible():
                    return
            except Exception:
                pass
        if date_from_sel and date_to_sel:
            a, b = _portal_visible_date_range_pair(page, date_from_sel, date_to_sel)
            if a and b:
                return
        else:
            d, _ = _pick_selector_any_frame(page, date_sel)
            if d:
                try:
                    if d.is_visible():
                        return
                except Exception:
                    pass
        time.sleep(poll)


def _write_debug_artifacts(page, label: str) -> str:
    """
    Writes screenshot + HTML to `data/playwright_debug/` and returns the folder path.
    """
    debug_dir = os.path.join(BASE_DIR, "data", "playwright_debug")
    os.makedirs(debug_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(debug_dir, f"{ts}_{label}")
    os.makedirs(out_dir, exist_ok=True)

    # Best effort: don't let debug writing hide the real error.
    try:
        page.screenshot(path=os.path.join(out_dir, "page.png"), full_page=True)
    except Exception:
        pass
    try:
        html = page.content()
        with open(os.path.join(out_dir, "page.html"), "w", encoding="utf-8") as f:
            f.write(html if isinstance(html, str) else str(html))
    except Exception as exc:
        try:
            with open(os.path.join(out_dir, "page.html"), "w", encoding="utf-8") as f:
                f.write(f"<!-- page.content() failed: {exc!r} -->\n")
        except Exception:
            pass
    try:
        with open(os.path.join(out_dir, "meta.txt"), "w", encoding="utf-8") as f:
            f.write(f"url={page.url}\n")
            f.write(f"title={page.title()}\n")
            f.write("frames:\n")
            for fr in page.frames:
                f.write(f"- {fr.url}\n")
    except Exception:
        pass
    return out_dir


_DEFAULT_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


def _playwright_persistent_kwargs(user_data_dir: str, headless: bool) -> dict:
    """
    Shared Chromium launch options for portal scraping.

    - Realistic User-Agent: many WAFs throttle the default HeadlessChrome UA.
    - Viewport: Amity LMS layouts assume a desktop width.
    - --no-sandbox: required on some Linux/Docker hosts when not using user namespaces.
    """
    ua = (_env("PLAYWRIGHT_USER_AGENT", "") or "").strip() or _DEFAULT_CHROME_UA
    vw = int(_env("PLAYWRIGHT_VIEWPORT_WIDTH", "1366"))
    vh = int(_env("PLAYWRIGHT_VIEWPORT_HEIGHT", "900"))
    return {
        "user_data_dir": user_data_dir,
        "headless": headless,
        "timezone_id": _env("PLAYWRIGHT_TIMEZONE_ID", "Asia/Kolkata"),
        "user_agent": ua,
        "viewport": {"width": vw, "height": vh},
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    }


def _portal_build_config() -> dict[str, object]:
    portal_url = _env("PORTAL_URL").rstrip("/")
    explicit_login_url = _env("PORTAL_LOGIN_URL")
    login_candidates: list[str] = []
    if explicit_login_url:
        login_candidates = [explicit_login_url]
    elif portal_url:
        login_candidates = [
            f"{portal_url}/Account/Login",
            f"{portal_url}/Login",
            f"{portal_url}/login",
            portal_url,
        ]
    search_url = _env("SCRAPER_SEARCH_PAGE_URL", portal_url)

    username = _env("PORTAL_USERNAME")
    password = _env("PORTAL_PASSWORD")
    if not username or not password:
        raise RuntimeError("Portal credentials are missing in environment variables")
    if not login_candidates:
        raise RuntimeError("PORTAL_URL or PORTAL_LOGIN_URL must be set")
    if not search_url:
        raise RuntimeError("SCRAPER_SEARCH_PAGE_URL (or PORTAL_URL) must be set")

    user_data_dir = _env("PLAYWRIGHT_USER_DATA_DIR", os.path.join(BASE_DIR, "data", "playwright_profile"))
    headless = _env("SCRAPER_HEADLESS", "true").lower() not in {"0", "false", "no"}
    timeout_ms = int(_env("SCRAPER_TIMEOUT_MS", "60000"))
    post_login_wait_ms = int(_env("SCRAPER_POST_LOGIN_WAIT_MS", "1500"))

    return {
        "login_candidates": login_candidates,
        "search_url": search_url,
        "username": username,
        "password": password,
        "user_data_dir": user_data_dir,
        "headless": headless,
        "timeout_ms": timeout_ms,
        "post_login_wait_ms": post_login_wait_ms,
        # Amity portal uses text field name=UserName (not type=email); defaults must match without a full .env.
        "email_sel": _env(
            "SCRAPER_EMAIL_SELECTOR",
            'input[name="UserName"],#UserName,input[type="email"],input[name="email"],input[name="Email"],#email',
        ),
        "pass_sel": _env(
            "SCRAPER_PASSWORD_SELECTOR",
            'input[name="Password"],#Password,input[type="password"],input[name="password"],#password',
        ),
        "submit_sel": _env(
            "SCRAPER_LOGIN_SUBMIT_SELECTOR",
            'button[type="submit"],input[type="submit"],button:has-text("Login"),button:has-text("Sign in")',
        ),
        "date_sel": _env(
            "SCRAPER_DATE_INPUT_SELECTOR",
            'input[type="date"],input[name*="date" i],input[id*="date" i],input[placeholder*="dd" i],input[placeholder*="mm" i]',
        ),
        "search_btn_sel": _env(
            "SCRAPER_SEARCH_BUTTON_SELECTOR",
            'input[value="Search"],input[type="submit"][value*="Search" i],button:has-text("Search"),a:has-text("Search")',
        ),
        # Button/link that opens the date search panel (often "Search Data" on Amity LMS).
        "search_data_sel": _env(
            "SCRAPER_SEARCH_DATA_SELECTOR",
            'button:has-text("Search Data"),a:has-text("Search Data"),input[value*="Search Data" i]',
        ),
        # Optional: visible calendar / popup (wait before typing dates).
        "calendar_wait_sel": _env("SCRAPER_CALENDAR_VISIBLE_SELECTOR", "").strip(),
        # Submit search after dates filled (defaults to same as search_btn_sel if unset).
        "results_search_btn_sel": (_env("SCRAPER_RESULTS_SEARCH_BUTTON_SELECTOR", "").strip() or None),
        # Amity LMS posts HTML into #divLMSPartial; do not use bare "table" (modal table is first in DOM).
        "table_sel": _env("SCRAPER_RESULTS_TABLE_SELECTOR", "#divLMSPartial table"),
        "next_sel": _env("SCRAPER_NEXT_PAGE_SELECTOR", 'a:has-text("Next"),button:has-text("Next"),.pagination .next'),
    }


def _portal_login_on_page(page, cfg: dict) -> None:
    email_sel = str(cfg["email_sel"])
    pass_sel = str(cfg["pass_sel"])
    submit_sel = str(cfg["submit_sel"])
    username = str(cfg["username"])
    password = str(cfg["password"])
    timeout_ms = int(cfg["timeout_ms"])
    post_login_wait_ms = int(cfg["post_login_wait_ms"])
    login_candidates = cfg["login_candidates"]
    assert isinstance(login_candidates, list)

    logged_in = False
    for login_url in login_candidates:
        page.goto(str(login_url), wait_until="domcontentloaded")
        _human_sleep()
        email_loc = _pick_selector(page, email_sel)
        pass_loc = _pick_selector(page, pass_sel)
        if not (email_loc and pass_loc):
            continue
        email_loc.fill(username)
        pass_loc.fill(password)
        submit_loc = _pick_selector(page, submit_sel)
        if submit_loc:
            submit_loc.click()
        else:
            pass_loc.press("Enter")
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except PlaywrightTimeoutError:
            pass
        time.sleep(post_login_wait_ms / 1000.0)
        _human_sleep()
        email_after = _pick_selector(page, email_sel)
        pass_after = _pick_selector(page, pass_sel)
        if not (email_after and pass_after):
            logged_in = True
            break

    if not logged_in:
        raise RuntimeError(
            "Portal login failed. Set PORTAL_LOGIN_URL to your exact login page "
            "and verify SCRAPER_EMAIL_SELECTOR / SCRAPER_PASSWORD_SELECTOR in .env."
        )


def _portal_largest_data_table_on_main(page, *, min_tr: int = 2, max_tables: int = 30):
    """
    Fallback when #divLMSPartial is empty: pick the visible <table> on the main frame
    with the most body rows (skips tiny chrome tables).
    """
    mf = page.main_frame
    best = None
    best_score = 0
    try:
        n = min(mf.locator("table").count(), max_tables)
    except Exception:
        n = 0
    for i in range(n):
        t = mf.locator("table").nth(i)
        try:
            if not t.is_visible():
                continue
            trc = t.locator("tr").count()
            if trc < min_tr:
                continue
            score = trc * 10
            try:
                head = (t.inner_text(timeout=2500) or "")[:800].lower()
                if any(k in head for k in ("mobile", "phone", "student", "email", "lead", "source")):
                    score += 200
            except Exception:
                pass
            if score > best_score:
                best_score = score
                best = t
        except Exception:
            continue
    return best


def _portal_active_results_table(page, table_sel: str):
    """
    Resolve the leads grid table. Amity injects rows under #divLMSPartial; the first
    document <table> is often an empty modal summary table, so .first is wrong.
    """
    mf = page.main_frame
    try:
        wrap = mf.locator("#divLMSPartial").first
        if wrap.count() > 0:
            try:
                wrap.scroll_into_view_if_needed(timeout=8000)
            except Exception:
                pass
        pl = mf.locator("#divLMSPartial table")
        n = pl.count()
        for i in range(n):
            t = pl.nth(i)
            try:
                if not t.is_visible():
                    continue
                if t.locator("tr").count() >= 2:
                    return t
            except Exception:
                continue
    except Exception:
        pass

    for sel in _iter_selectors((table_sel or "").strip()):
        try:
            loc = mf.locator(sel).first
            if loc.count() == 0:
                continue
            if not loc.is_visible():
                continue
            if loc.locator("tr").count() >= 2:
                return loc
        except Exception:
            continue

    return _portal_largest_data_table_on_main(page)


def _portal_wait_for_search_results(
    page,
    cfg: dict,
    *,
    sync_job_id: int | None = None,
    range_job_id: int | None = None,
) -> None:
    """
    After clicking Search, wait until the results grid looks populated (not just the
    first empty paint). Polls the table until enough data rows exist or timeout.
    """
    table_sel = str(cfg["table_sel"])
    timeout_ms = int(_env("SCRAPER_RESULTS_WAIT_MS", "120000"))
    poll_sec = float(_env("SCRAPER_RESULTS_POLL_SEC", "0.5"))
    min_data_rows = int(_env("SCRAPER_MIN_DATA_ROWS", "1"))
    loading_sel = _env("SCRAPER_LOADING_SELECTOR", "").strip()
    started = time.time()
    deadline = started + max(timeout_ms, 5000) / 1000.0
    last_ui_ts = 0.0

    while time.time() < deadline:
        now = time.time()
        if sync_job_id is not None or range_job_id is not None:
            # Keep the dashboard "alive" while we wait (otherwise it looks stuck).
            if now - last_ui_ts >= 2.5:
                last_ui_ts = now
                try:
                    elapsed = int(now - started)
                    _scrape_ui_progress(
                        sync_job_id,
                        range_job_id,
                        min_interval_sec=0.0,
                        result_summary=f"LMS: submitting search; waiting for lead grid… ({elapsed}s)",
                    )
                except Exception:
                    pass

        if loading_sel:
            try:
                loc = page.locator(loading_sel).first
                if loc.count() > 0 and loc.is_visible():
                    time.sleep(poll_sec)
                    continue
            except Exception:
                pass

        table = _portal_active_results_table(page, table_sel)
        if table is None:
            time.sleep(poll_sec)
            continue

        # Fast path: evaluate table content in one browser round-trip (much faster than per-row inner_text).
        try:
            filled, n = table.evaluate(
                """(tbl) => {
                  const rows = Array.from(tbl.querySelectorAll('tr'));
                  const n = rows.length;
                  if (n < 2) return [0, n];
                  let filled = 0;
                  const rePhone = /\\d{10}/;
                  const reEmpty = /\\b(no data|no records|no leads|loading)\\b/i;
                  for (let ri = 1; ri < n; ri++) {
                    const t = (rows[ri].innerText || '').trim();
                    if (!t) continue;
                    if (reEmpty.test(t)) { filled = 0; break; }
                    if (rePhone.test(t) || t.length > 12) filled++;
                  }
                  return [filled, n];
                }"""
            )
        except Exception:
            rows = table.locator("tr")
            n = rows.count()
            if n < 2:
                time.sleep(poll_sec)
                continue

            filled = 0
            for ri in range(1, n):
                try:
                    text = rows.nth(ri).inner_text()
                except Exception:
                    text = ""
                t = (text or "").strip()
                if not t:
                    continue
                if re.search(r"\d{10}", t) or len(t) > 12:
                    filled += 1
                elif re.search(r"(?i)\b(no data|no records|no leads|loading)\b", t):
                    filled = 0
                    break

        if filled >= min_data_rows:
            time.sleep(float(_env("SCRAPER_POST_RESULTS_SETTLE_SEC", "0.75")))
            return

        time.sleep(poll_sec)

    out_dir = _write_debug_artifacts(page, "results_wait_timeout")
    raise RuntimeError(
        "Timed out waiting for the LMS lead grid to populate after Search. "
        f"Check portal availability/auth/captcha/selectors. Debug saved to: {out_dir}"
    )


def _commit_portal_date_input(locator) -> None:
    """
    Amity LMS uses Bootstrap datepicker + jQuery; btnSearch reads $("#searchFromDate").val().
    Sync the widget and fire change so the main document input is non-empty after automation.
    """
    try:
        locator.evaluate(
            """(el) => {
                try {
                    var doc = el && el.ownerDocument;
                    var w = doc && doc.defaultView;
                    var $ = w && w.jQuery;
                    if ($) {
                        var $e = $(el);
                        if ($e.data && $e.data('datepicker')) {
                            try { $e.datepicker('update'); } catch (e0) {}
                        }
                        try { $e.trigger('change'); } catch (e1) {}
                    }
                    try { el.dispatchEvent(new Event('input', { bubbles: true })); } catch (e2) {}
                    try { el.dispatchEvent(new Event('change', { bubbles: true })); } catch (e3) {}
                } catch (e) {}
            }"""
        )
    except Exception:
        pass


def _force_set_date_input_value(locator, value: str) -> None:
    """
    Force-set an LMS date input that uses jQuery/Bootstrap datepicker.
    Sets DOM value and asks datepicker to update, then triggers events.
    """
    try:
        locator.evaluate(
            """(el, val) => {
              if (!el) return;
              try { el.removeAttribute('readonly'); } catch(e) {}
              try { el.value = val; } catch(e) {}
              try {
                const w = el.ownerDocument && el.ownerDocument.defaultView;
                const $ = w && w.jQuery;
                if ($) {
                  const $e = $(el);
                  try { $e.val(val); } catch(e0) {}
                  try { $e.datepicker('update', val); } catch(e1) {}
                  try { $e.trigger('input'); } catch(e2) {}
                  try { $e.trigger('change'); } catch(e3) {}
                }
              } catch(e) {}
              try { el.dispatchEvent(new Event('input', { bubbles: true })); } catch(e) {}
              try { el.dispatchEvent(new Event('change', { bubbles: true })); } catch(e) {}
              try { el.blur(); } catch(e) {}
            }""",
            value,
        )
    except Exception:
        pass
    _commit_portal_date_input(locator)


def _set_date_field(locator, portal_date: str, page) -> None:
    """
    Robustly set a date field (works for readonly/custom widgets):
    - click/focus
    - try fill
    - try JS set + dispatch events
    - fallback: Ctrl+A, Backspace, type
    Always commits Bootstrap/jQuery datepicker so .val() matches for portal validation.
    """
    try:
        locator.scroll_into_view_if_needed()
    except Exception:
        pass
    try:
        locator.click()
    except Exception:
        pass

    # 1) normal fill
    try:
        locator.fill(portal_date)
        _commit_portal_date_input(locator)
        return
    except Exception:
        pass

    # 2) JS set value + fire events
    try:
        _force_set_date_input_value(locator, portal_date)
        return
    except Exception:
        pass

    # 3) keyboard overwrite
    try:
        locator.click()
        page.keyboard.press("Control+A")
        page.keyboard.press("Backspace")
        page.keyboard.type(portal_date)
        _commit_portal_date_input(locator)
    except Exception:
        try:
            page.keyboard.type(portal_date)
            _commit_portal_date_input(locator)
        except Exception:
            pass


def _block_heavy_portal_resources_enabled() -> bool:
    # Default off: blocking images/fonts can break AJAX-heavy LMS grids on slow / remote links.
    return _env("SCRAPER_BLOCK_HEAVY_RESOURCES", "false").lower() in {"1", "true", "yes", "on"}


def _install_portal_route_blocking(context) -> None:
    """Optional image/font/media blocking — disable if the portal stops loading the grid."""
    if not _block_heavy_portal_resources_enabled():
        return
    try:

        def _route_handler(route, request):
            rt = request.resource_type
            if rt in ("image", "font", "media"):
                route.abort()
            else:
                route.continue_()

        context.route("**/*", _route_handler)
    except Exception:
        pass


_PORTAL_TABLE_ROWS_BULK_JS = """(tbl) => {
  const rows = Array.from(tbl.querySelectorAll('tr'));
  if (rows.length < 2) return [];
  const headerCells = Array.from(rows[0].querySelectorAll('th,td'));
  const headers = headerCells.map((c, i) => {
    const t = (c && c.innerText ? c.innerText : '').trim();
    return t || `col_${i}`;
  });
  const out = [];
  for (let ri = 1; ri < rows.length; ri++) {
    const cells = Array.from(rows[ri].querySelectorAll('td,th'));
    if (!cells.length) continue;
    const obj = {};
    for (let ci = 0; ci < cells.length; ci++) {
      const key = headers[ci] ?? `col_${ci}`;
      const val = ((cells[ci] && cells[ci].innerText) ? cells[ci].innerText : '').trim();
      obj[key] = val;
    }
    if (Object.values(obj).some(v => v)) out.push(obj);
  }
  return out;
}"""


def _portal_row_highlight_enabled(cfg: dict) -> bool:
    """
    When True, rows are read via the DOM one-by-one with a visible highlight (needs SCRAPER_HEADLESS=false to see).
    SCRAPER_HIGHLIGHT_ROWS: auto (default: on only when not headless), true, false.
    """
    v = (_env("SCRAPER_HIGHLIGHT_ROWS", "auto") or "auto").strip().lower()
    headless = bool(cfg.get("headless", True))
    if v in {"0", "false", "no", "off"}:
        return False
    if v in {"1", "true", "yes", "on"}:
        return True
    return not headless


def _portal_clear_row_highlights(table) -> None:
    try:
        table.evaluate(
            """(tbl) => {
              tbl.querySelectorAll('tr').forEach((tr) => {
                tr.style.outline = '';
                tr.style.backgroundColor = '';
                tr.classList.remove('amity-scrape-row');
              });
            }"""
        )
    except Exception:
        pass


def _portal_extract_page_data_rows(_page, table, cfg: dict, *, highlight: bool) -> list[dict]:
    """Read data rows from the results table; optionally highlight each row in the browser while reading."""
    rows = table.locator("tr")
    try:
        n = rows.count()
    except Exception:
        return []
    if n < 2:
        return []

    if not highlight:
        try:
            page_records = table.evaluate(_PORTAL_TABLE_ROWS_BULK_JS)
            if isinstance(page_records, list) and len(page_records) > 0:
                return page_records
        except Exception:
            pass

    try:
        header_cells = rows.nth(0).locator("th, td")
        hc = header_cells.count()
    except Exception:
        return []
    headers: list[str] = []
    for i in range(hc):
        try:
            headers.append(header_cells.nth(i).inner_text().strip() or f"col_{i}")
        except Exception:
            headers.append(f"col_{i}")

    try:
        pause = float(_env("SCRAPER_ROW_HIGHLIGHT_PAUSE_SEC", "0.055"))
    except Exception:
        pause = 0.055
    out: list[dict] = []
    prev_highlight_ri: int | None = None
    for ri in range(1, n):
        row = rows.nth(ri)
        if highlight:
            try:
                if prev_highlight_ri is not None:
                    rows.nth(prev_highlight_ri).evaluate(
                        """(tr) => {
                          tr.style.outline = '';
                          tr.style.backgroundColor = '';
                          tr.classList.remove('amity-scrape-row');
                        }"""
                    )
                row.evaluate(
                    """(tr) => {
                      tr.classList.add('amity-scrape-row');
                      tr.style.outline = '3px solid #ea580c';
                      tr.style.backgroundColor = 'rgba(254, 236, 179, 0.92)';
                    }"""
                )
                row.scroll_into_view_if_needed(timeout=4000)
                time.sleep(pause)
                prev_highlight_ri = ri
            except Exception:
                pass
        try:
            cells = row.locator("td, th")
            cc = cells.count()
        except Exception:
            continue
        if cc == 0:
            continue
        record: dict = {}
        for ci in range(cc):
            key = headers[ci] if ci < len(headers) else f"col_{ci}"
            try:
                record[key] = cells.nth(ci).inner_text().strip()
            except Exception:
                record[key] = ""
        if any(v for v in record.values()):
            out.append(record)

    if highlight:
        try:
            if prev_highlight_ri is not None:
                rows.nth(prev_highlight_ri).evaluate(
                    """(tr) => {
                      tr.style.outline = '';
                      tr.style.backgroundColor = '';
                      tr.classList.remove('amity-scrape-row');
                    }"""
                )
        except Exception:
            pass
        _portal_clear_row_highlights(table)
    return out


def _portal_advance_to_next_results_page(page, cfg: dict) -> bool:
    """Click the LMS grid \"next page\" control; return False if there is no next page."""
    next_sel = str(cfg["next_sel"])
    timeout_ms = int(cfg["timeout_ms"])
    if not next_sel:
        return False
    next_btn, _ = _first_visible_locator_any_frame(page, next_sel)
    if not next_btn:
        return False
    try:
        if next_btn.is_disabled():
            return False
    except Exception:
        pass
    try:
        next_btn.click(timeout=5000)
    except Exception:
        try:
            next_btn.click(force=True, timeout=5000)
        except Exception:
            return False
    if _env("SCRAPER_WAIT_NETWORKIDLE_AFTER_SEARCH", "false").lower() in {"1", "true", "yes"}:
        try:
            page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 20000))
        except PlaywrightTimeoutError:
            pass
    try:
        time.sleep(float(_env("SCRAPER_PAGE_TURN_PAUSE_SEC", "0.35")))
    except Exception:
        time.sleep(0.35)
    _portal_wait_for_search_results(page, cfg, sync_job_id=sync_job_id, range_job_id=jid)
    _lms_step_sleep()
    return True


def _portal_scrape_leads_flow(
    page,
    cfg: dict,
    start_yyyy_mm_dd: str,
    end_yyyy_mm_dd: str | None,
    *,
    sync_job_id: int | None = None,
    range_count_base: tuple[int, int, int] | None = None,
    range_rows_base: int = 0,
    resume_pages_completed: int = 0,
    on_page_checkpoint: Callable[[int], None] | None = None,
    resume_until_row_key: str | None = None,
    on_row_key_checkpoint: Callable[[str | None], None] | None = None,
) -> PortalScrapeOutcome:
    """
    LMS flow: open LMS URL → (optional) click 'Search Data' → wait for date UI →
    fill From/To or single date → click results Search → wait for rows → scrape table (+ pages).
    """
    search_url = str(cfg["search_url"])
    date_sel = str(cfg["date_sel"])
    date_from_sel = _env("SCRAPER_DATE_FROM_SELECTOR", "").strip()
    date_to_sel = _env("SCRAPER_DATE_TO_SELECTOR", "").strip()
    table_sel = str(cfg["table_sel"])
    next_sel = str(cfg["next_sel"])
    timeout_ms = int(cfg["timeout_ms"])
    results_sel = str(cfg.get("results_search_btn_sel") or cfg["search_btn_sel"])
    jid = cfg.get("range_job_id")
    jid = jid if isinstance(jid, int) else None
    persist = sync_job_id is not None or jid is not None
    stream_counts = {"inserted": 0, "updated": 0, "failed": 0}
    lead_date_for_db = start_yyyy_mm_dd
    source_tag = "portal_scraper" if jid else "internal_portal"
    rcb_i, rcb_u, rcb_f = range_count_base if range_count_base is not None else (0, 0, 0)

    page.goto(search_url, wait_until="domcontentloaded")
    time.sleep(0.5)
    _lms_step_sleep()
    _scrape_ui_progress(
        sync_job_id,
        jid,
        _force=True,
        result_summary="LMS: page loaded; opening search / date controls…",
    )
    title_lower = (page.title() or "").lower()
    if "resource cannot be found" in title_lower or "server error" in title_lower:
        out_dir = _write_debug_artifacts(page, "bad_search_url_or_auth")
        raise RuntimeError(
            "Portal returned an error page when opening SCRAPER_SEARCH_PAGE_URL. "
            "This is usually a wrong URL or missing access after login. "
            f"Set PORTAL_LOGIN_URL explicitly and verify SCRAPER_SEARCH_PAGE_URL. (debug saved to: {out_dir})"
        )

    if _env("SCRAPER_CLICK_SEARCH_DATA_FIRST", "true").lower() in {"1", "true", "yes"}:
        _portal_open_search_data_panel(page, cfg, timeout_ms)
        _portal_wait_for_calendar_or_date_inputs(page, cfg, timeout_ms)
        _lms_step_sleep()
        _scrape_ui_progress(sync_job_id, jid, result_summary="LMS: date panel ready; filling From/To…")

    end_iso = end_yyyy_mm_dd or start_yyyy_mm_dd
    portal_from = _format_portal_date(start_yyyy_mm_dd)
    portal_to = _format_portal_date(end_iso)

    if date_from_sel and date_to_sel:
        from_loc, to_loc = _portal_visible_date_range_pair(page, date_from_sel, date_to_sel)
        if not from_loc or not to_loc:
            out_dir = _write_debug_artifacts(page, "date_range_input_not_found")
            raise RuntimeError(
                "From/To date inputs not found (no visible pair). "
                "On Amity LMS/Index use #searchFromDate and #searchToDate — set SCRAPER_DATE_FROM_SELECTOR / "
                "SCRAPER_DATE_TO_SELECTOR in .env, ensure the Search Data panel is open and Type = Search Data is selected. "
                f"(debug saved to: {out_dir})"
            )
        # Prefer a single JS action that sets BOTH fields and datepicker state (the portal reads jQuery .val()).
        vf = ""
        vt = ""
        try:
            vf, vt = page.evaluate(
                """(vals) => {
                  const [fromV, toV] = vals;
                  const f = document.querySelector('#searchFromDate');
                  const t = document.querySelector('#searchToDate');
                  const setVal = (el, v) => {
                    if (!el) return '';
                    try { el.removeAttribute('readonly'); } catch(e) {}
                    try { el.value = v; } catch(e) {}
                    try { el.dispatchEvent(new Event('input', { bubbles: true })); } catch(e) {}
                    try { el.dispatchEvent(new Event('change', { bubbles: true })); } catch(e) {}
                    return (el.value || '').trim();
                  };
                  const parseDMY = (s) => {
                    const parts = String(s||'').split('/');
                    if (parts.length !== 3) return null;
                    const d = Number(parts[0]); const m = Number(parts[1]); const y = Number(parts[2]);
                    if (!d || !m || !y) return null;
                    return new Date(y, m - 1, d);
                  };
                  const $ = window.jQuery;
                  if ($ && f && $(f).datepicker) {
                    const dFrom = parseDMY(fromV);
                    try { $(f).datepicker('setDate', dFrom || fromV); } catch(e0) {}
                    try { $(f).datepicker('update', dFrom || fromV); } catch(e1) {}
                    // Mirror LMS changeDate handler so To constraints are configured.
                    try {
                      const d = dFrom || parseDMY(fromV);
                      if (d) {
                        const minDate = new Date(d.valueOf());
                        const maxDate = new Date(d.valueOf()); maxDate.setDate(maxDate.getDate() + 7);
                        try { $('#searchToDate').datepicker('setStartDate', minDate); } catch(e2) {}
                        try { $('#searchToDate').datepicker('setEndDate', maxDate); } catch(e3) {}
                      }
                    } catch(e4) {}
                  } else {
                    setVal(f, fromV);
                  }
                  if ($ && t && $(t).datepicker) {
                    const dTo = parseDMY(toV);
                    try { $(t).datepicker('setDate', dTo || toV); } catch(e5) {}
                    try { $(t).datepicker('update', dTo || toV); } catch(e6) {}
                  } else {
                    setVal(t, toV);
                  }
                  const outF = (f && (f.value || '')).trim();
                  const outT = (t && (t.value || '')).trim();
                  return [outF, outT];
                }""",
                [portal_from, portal_to],
            )
        except Exception:
            pass

        if not vf or not vt:
            # Fallback to locator-based setters if JS couldn't access elements for some reason.
            _set_date_field(from_loc, portal_from, page)
            _lms_step_sleep()
            _set_date_field(to_loc, portal_to, page)
            _lms_step_sleep()
            try:
                vf = (from_loc.input_value() or "").strip()
                vt = (to_loc.input_value() or "").strip()
            except Exception:
                pass

        if not vf or not vt:
            out_dir = _write_debug_artifacts(page, "date_range_not_set")
            raise RuntimeError(
                "Could not set Search From/To dates on LMS (inputs remain empty). "
                f"Debug saved to: {out_dir}"
            )
    else:
        if end_iso != start_yyyy_mm_dd:
            out_dir = _write_debug_artifacts(page, "range_needs_from_to")
            raise RuntimeError(
                "Different start and end dates need separate From/To fields on the portal. "
                "Set SCRAPER_DATE_FROM_SELECTOR and SCRAPER_DATE_TO_SELECTOR in .env "
                f"(or use a single day). Debug: {out_dir}"
            )
        date_loc, _frame_url = _pick_selector_any_frame(page, date_sel)
        if not date_loc:
            out_dir = _write_debug_artifacts(page, "date_input_not_found")
            raise RuntimeError(
                "Date input not found. Update SCRAPER_DATE_INPUT_SELECTOR in .env. "
                f"(debug saved to: {out_dir})"
            )
        _set_date_field(date_loc, portal_from, page)
    _lms_step_sleep()
    _scrape_ui_progress(sync_job_id, jid, result_summary="LMS: submitting search; waiting for lead grid…")

    _portal_click_search_button(page, results_sel, timeout_ms)
    if _env("SCRAPER_WAIT_NETWORKIDLE_AFTER_SEARCH", "false").lower() in {"1", "true", "yes"}:
        try:
            page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 20000))
        except PlaywrightTimeoutError:
            pass
    else:
        try:
            time.sleep(float(_env("SCRAPER_POST_SEARCH_PAUSE_SEC", "0.45")))
        except Exception:
            time.sleep(0.45)
    _portal_wait_for_search_results(page, cfg)
    _lms_step_sleep()
    _scrape_ui_progress(
        sync_job_id,
        jid,
        _force=True,
        total_fetched=range_rows_base,
        inserted_count=rcb_i,
        updated_count=rcb_u,
        failed_count=rcb_f,
        result_summary="LMS: grid detected; scraping rows from the table…",
    )

    try:
        page.main_frame.locator("#divLMSPartial").first.scroll_into_view_if_needed(timeout=8000)
    except Exception:
        pass

    # Used by single-page resume mode to stop after reaching the stored DB marker.
    stop_after_this_page = False

    skipped = 0
    for _ in range(resume_pages_completed):
        t = _portal_active_results_table(page, table_sel)
        if t is None:
            break
        if stop_after_this_page:
            break

        if not _portal_advance_to_next_results_page(page, cfg):
            break
        skipped += 1
    if resume_pages_completed > 0 and skipped != resume_pages_completed:
        if on_page_checkpoint is not None:
            on_page_checkpoint(0)
        raise RuntimeError(
            f"LMS pagination resume failed ({skipped}/{resume_pages_completed} page skips). "
            "Checkpoint was reset to 0; fix the portal or run again to scrape this date from the first page."
        )

    all_records: list[dict] = []
    page_num = 0
    while True:
        table = _portal_active_results_table(page, table_sel)
        if table is None:
            break
        try:
            _portal_clear_row_highlights(table)
        except Exception:
            pass
        highlight = _portal_row_highlight_enabled(cfg)
        page_batch = _portal_extract_page_data_rows(page, table, cfg, highlight=highlight)

        stop_after_this_page = False
        # Single-page resume marker: track the *last/bottom* row on the page.
        # Next cron tick will scrape only rows *after* this marker (new rows appended below).
        current_bottom_key = _stable_portal_row_key(page_batch[-1]) if page_batch else None
        if on_row_key_checkpoint is not None:
            on_row_key_checkpoint(current_bottom_key)

        if resume_until_row_key and page_batch:
            cutoff = None
            for j, r in enumerate(page_batch):
                if _stable_portal_row_key(r) == resume_until_row_key:
                    cutoff = j
                    break
            if cutoff is not None:
                page_batch = page_batch[cutoff + 1 :]
                stop_after_this_page = True


        _persist_portal_rows_incremental(
            page_batch,
            all_records,
            persist=persist,
            sync_job_id=sync_job_id,
            range_job_id=jid,
            lead_date=lead_date_for_db,
            source=source_tag,
            counts=stream_counts,
            range_count_base=range_count_base,
            range_rows_base=range_rows_base,
        )

        page_num += 1
        if on_page_checkpoint is not None:
            on_page_checkpoint(resume_pages_completed + page_num)
        if not persist:
            _scrape_ui_progress(
                sync_job_id,
                jid,
                _force=True,
                total_fetched=len(all_records),
                result_summary=f"LMS: scraped {len(all_records)} row(s) so far (page {page_num}).",
            )

        if not _portal_advance_to_next_results_page(page, cfg):
            break

    return PortalScrapeOutcome(
        records=all_records,
        inserted=stream_counts["inserted"],
        updated=stream_counts["updated"],
        failed=stream_counts["failed"],
        streamed=persist,
    )


def _portal_extract_table_for_date(
    page,
    cfg: dict,
    selected_date: str,
    *,
    range_count_base: tuple[int, int, int] | None = None,
    range_rows_base: int = 0,
) -> PortalScrapeOutcome:
    """One calendar day on the portal (same From and To when only one field exists)."""
    return _portal_scrape_leads_flow(
        page,
        cfg,
        selected_date,
        None,
        sync_job_id=None,
        range_count_base=range_count_base,
        range_rows_base=range_rows_base,
    )


def scrape_portal_records_range_playwright(
    date_from: str, date_to: str, *, range_job_id: int | None = None
) -> PortalScrapeOutcome:
    """Login once, run one LMS search for [date_from .. date_to], return all scraped rows."""
    cfg: dict = _portal_build_config()
    if range_job_id is not None:
        cfg = {**cfg, "range_job_id": range_job_id}
    user_data_dir = str(cfg["user_data_dir"])
    headless = bool(cfg["headless"])
    timeout_ms = int(cfg["timeout_ms"])

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            **_playwright_persistent_kwargs(user_data_dir, headless)
        )
        try:
            _install_portal_route_blocking(context)

            page = context.pages[0] if context.pages else context.new_page()
            page.set_default_timeout(timeout_ms)
            _portal_login_on_page(page, cfg)
            return _portal_scrape_leads_flow(page, cfg, date_from, date_to)
        finally:
            context.close()




def _stable_portal_row_key(row: dict) -> str:
    payload = json.dumps(row or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def scrape_portal_records_with_playwright(
    selected_date: str,
    *,
    sync_job_id: int | None = None,
    resume_pages_completed: int = 0,
    on_page_checkpoint: Callable[[int], None] | None = None,
    resume_until_row_key: str | None = None,
    on_row_key_checkpoint: Callable[[str | None], None] | None = None,
) -> PortalScrapeOutcome:
    """
    Use a real browser session to login + scrape the results table for one day.
    ``resume_pages_completed`` / ``on_page_checkpoint`` support the scheduled cron runner
    (skip already-persisted pages, then continue).
    """
    cfg: dict = _portal_build_config()
    user_data_dir = str(cfg["user_data_dir"])
    headless = bool(cfg["headless"])
    timeout_ms = int(cfg["timeout_ms"])

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            **_playwright_persistent_kwargs(user_data_dir, headless)
        )
        try:
            _install_portal_route_blocking(context)

            page = context.pages[0] if context.pages else context.new_page()
            page.set_default_timeout(timeout_ms)
            _portal_login_on_page(page, cfg)
            return _portal_scrape_leads_flow(
                page,
                cfg,
                selected_date,
                None,
                sync_job_id=sync_job_id,
                resume_pages_completed=resume_pages_completed,
                on_page_checkpoint=on_page_checkpoint,
                resume_until_row_key=resume_until_row_key,
                on_row_key_checkpoint=on_row_key_checkpoint,
            )
        finally:
            context.close()


def _daterange_inclusive_iso(date_from: str, date_to: str) -> list[str]:
    a = parse_date_yyyy_mm_dd(date_from)
    b = parse_date_yyyy_mm_dd(date_to)
    if not a or not b:
        raise ValueError("invalid date range")
    if b < a:
        raise ValueError("date_to must be on or after date_from")
    out: list[str] = []
    cur: date = a
    while cur <= b:
        out.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return out


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
    _ensure_sync_job_columns(db)
    rows = db.execute(
        """
        SELECT id, selected_date, status, total_fetched, inserted_count, updated_count, failed_count,
               error_message, result_summary, started_at, finished_at, created_by, created_at
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
        _ensure_sync_job_columns(db)
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
        _ensure_sync_job_columns(db)
        db.execute(f"UPDATE sync_jobs SET {', '.join(columns)} WHERE id = ?", params)


def create_range_scrape_job(date_from, date_to, created_by):
    now = datetime.now(timezone.utc).isoformat()
    with connect_db() as db:
        _ensure_range_scrape_jobs_table(db)
        cur = db.cursor()
        cur.execute(
            """
            INSERT INTO range_scrape_jobs (date_from, date_to, status, created_by, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (date_from, date_to, JOB_STATUS_PENDING, created_by, now),
        )
        return cur.lastrowid


def update_range_scrape_job(job_id, **kwargs):
    if not kwargs:
        return
    columns = []
    params = []
    for key, value in kwargs.items():
        columns.append(f"{key} = ?")
        params.append(value)
    params.append(job_id)
    with connect_db() as db:
        _ensure_range_scrape_jobs_table(db)
        db.execute(f"UPDATE range_scrape_jobs SET {', '.join(columns)} WHERE id = ?", params)


def get_range_scrape_jobs(limit=30):
    db = get_db()
    _ensure_range_scrape_jobs_table(db)
    db.commit()
    rows = db.execute(
        """
        SELECT id, date_from, date_to, status, days_total, days_done, current_date,
               total_fetched, inserted_count, updated_count, failed_count,
               error_message, result_summary, started_at, finished_at, created_by, created_at
        FROM range_scrape_jobs
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return rows


def merge_existing_row(existing_raw_data, incoming_raw_data):
    merged = dict(existing_raw_data)
    for key, value in incoming_raw_data.items():
        if value is None or (isinstance(value, str) and value.strip() == ""):
            if key not in merged:
                merged[key] = "N/A"
            continue
        merged[key] = value
    return merged


def _process_one_portal_record(
    db: sqlite3.Connection,
    record: dict,
    selected_date: str,
    source: str,
) -> Literal["inserted", "updated", "failed"]:
    """Insert or update a single scraped row. Caller must commit (e.g. ``with connect_db()``)."""
    try:
        if not isinstance(record, dict):
            return "failed"

        synced_at = datetime.now(timezone.utc).isoformat()
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

        if not normalized_mobile and _env("SCRAPER_INFER_MOBILE", "true").lower() in {
            "1",
            "true",
            "yes",
        }:
            inferred = _infer_single_mobile_from_row_values(raw_row)
            if inferred:
                normalized_mobile = inferred
                mobile_missing = 0
                mobile_hash = hash_mobile(inferred)
                mobile_masked = mask_mobile(inferred)

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
                    source,
                    synced_at,
                    synced_at,
                    existing["id"],
                ),
            )
            return "updated"

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
                source,
                synced_at,
                synced_at,
            ),
        )
        return "inserted"
    except Exception:
        return "failed"


def process_portal_records(records, selected_date, source="internal_portal"):
    inserted = 0
    updated = 0
    failed = 0
    with connect_db() as db:
        for record in records:
            o = _process_one_portal_record(db, record, selected_date, source)
            if o == "inserted":
                inserted += 1
            elif o == "updated":
                updated += 1
            else:
                failed += 1
    return inserted, updated, failed


def _persist_portal_rows_incremental(
    rows: list[dict] | None,
    all_records: list[dict],
    *,
    persist: bool,
    sync_job_id: int | None,
    range_job_id: int | None,
    lead_date: str,
    source: str,
    counts: dict[str, int],
    range_count_base: tuple[int, int, int] | None = None,
    range_rows_base: int = 0,
) -> None:
    """Append rows to ``all_records`` and optionally persist each row + update job progress."""
    if not rows:
        return
    bi, bu, bf = range_count_base if range_count_base is not None else (0, 0, 0)
    for rec in rows:
        all_records.append(rec)
        if not persist:
            continue
        with connect_db() as db:
            o = _process_one_portal_record(db, rec, lead_date, source)
        if o == "inserted":
            counts["inserted"] += 1
        elif o == "updated":
            counts["updated"] += 1
        else:
            counts["failed"] += 1
        n = len(all_records)
        display_tf = range_rows_base + n
        disp_i = bi + counts["inserted"]
        disp_u = bu + counts["updated"]
        disp_f = bf + counts["failed"]
        summary = (
            f"Scraping… {display_tf} row(s) from portal so far; "
            f"DB (cumulative): {disp_i} new, {disp_u} updated, {disp_f} failed."
        )
        force = n == 1 or n % 25 == 0
        _scrape_ui_progress(
            sync_job_id,
            range_job_id,
            min_interval_sec=0.65,
            total_fetched=display_tf,
            inserted_count=disp_i,
            updated_count=disp_u,
            failed_count=disp_f,
            result_summary=summary,
            _force=force,
        )


def run_sync_job(job_id, selected_date):
    started = datetime.now(timezone.utc).isoformat()
    update_sync_job(job_id, status=JOB_STATUS_RUNNING, started_at=started, error_message=None)
    try:
        outcome = scrape_portal_records_with_playwright(selected_date, sync_job_id=job_id)
        if not outcome.records:
            finished = datetime.now(timezone.utc).isoformat()
            update_sync_job(
                job_id,
                status=JOB_STATUS_COMPLETED,
                total_fetched=0,
                inserted_count=0,
                updated_count=0,
                failed_count=0,
                result_summary=None,
                finished_at=finished,
            )
            return

        if outcome.streamed:
            inserted, updated, failed = outcome.inserted, outcome.updated, outcome.failed
        else:
            inserted, updated, failed = process_portal_records(outcome.records, selected_date)
        finished = datetime.now(timezone.utc).isoformat()
        update_sync_job(
            job_id,
            status=JOB_STATUS_COMPLETED,
            total_fetched=len(outcome.records),
            inserted_count=inserted,
            updated_count=updated,
            failed_count=failed,
            result_summary=(
                f"Done: {len(outcome.records)} row(s) from portal; "
                f"{inserted} new, {updated} updated, {failed} failed in DB."
            ),
            finished_at=finished,
        )
    except Exception as exc:
        finished = datetime.now(timezone.utc).isoformat()
        update_sync_job(job_id, status=JOB_STATUS_FAILED, error_message=str(exc), finished_at=finished)
    finally:
        _sync_job_progress_clear(job_id)


def run_range_scrape_job(job_id, date_from, date_to):
    started = datetime.now(timezone.utc).isoformat()
    update_range_scrape_job(
        job_id, status=JOB_STATUS_RUNNING, started_at=started, error_message=None, result_summary=None
    )
    try:
        days = _daterange_inclusive_iso(date_from, date_to)
        max_days = int(_env("SCRAPER_MAX_RANGE_DAYS", "31"))
        if len(days) > max_days:
            raise RuntimeError(
                f"Range has {len(days)} days; limit is {max_days}. "
                "Increase SCRAPER_MAX_RANGE_DAYS in .env if you need a wider window."
            )

        one_shot = _env("SCRAPER_RANGE_ONE_PORTAL_SEARCH", "true").lower() in {"1", "true", "yes"}
        has_from_to = bool(_env("SCRAPER_DATE_FROM_SELECTOR", "").strip() and _env("SCRAPER_DATE_TO_SELECTOR", "").strip())
        same_day = date_from == date_to

        # One browser session: open LMS → Search Data → dates (From/To = dashboard range) → Search → wait → scrape → caller DB.
        if one_shot and (same_day or has_from_to):
            update_range_scrape_job(
                job_id,
                days_total=1,
                days_done=0,
                current_date=f"{date_from}–{date_to}",
                result_summary="Running: login + LMS (watch Rows scraped update live)…",
            )
            outcome = scrape_portal_records_range_playwright(date_from, date_to, range_job_id=job_id)
            total_fetched = len(outcome.records)
            inserted = updated = failed_rows = 0
            if outcome.records:
                if outcome.streamed:
                    inserted, updated, failed_rows = outcome.inserted, outcome.updated, outcome.failed
                else:
                    _range_job_progress(
                        job_id,
                        _force=True,
                        total_fetched=total_fetched,
                        result_summary=f"LMS: scraped {total_fetched} row(s); writing to database…",
                    )
                    inserted, updated, failed_rows = process_portal_records(
                        outcome.records, date_from, source="portal_scraper"
                    )
            finished = datetime.now(timezone.utc).isoformat()
            summary = (
                f"Scraped {total_fetched} row(s) from the portal; "
                f"database: {inserted} inserted, {updated} updated, {failed_rows} failed to persist."
            )
            update_range_scrape_job(
                job_id,
                status=JOB_STATUS_COMPLETED,
                days_done=1,
                current_date=None,
                total_fetched=total_fetched,
                inserted_count=inserted,
                updated_count=updated,
                failed_count=failed_rows,
                finished_at=finished,
                result_summary=summary,
            )
            _range_job_progress_clear(job_id)
            return

        update_range_scrape_job(job_id, days_total=len(days), days_done=0)

        cfg: dict = _portal_build_config()
        cfg = {**cfg, "range_job_id": job_id}
        user_data_dir = str(cfg["user_data_dir"])
        headless = bool(cfg["headless"])
        timeout_ms = int(cfg["timeout_ms"])

        total_fetched = 0
        inserted = 0
        updated = 0
        failed_rows = 0

        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                **_playwright_persistent_kwargs(user_data_dir, headless)
            )
            try:
                _install_portal_route_blocking(context)
                page = context.pages[0] if context.pages else context.new_page()
                page.set_default_timeout(timeout_ms)
                _portal_login_on_page(page, cfg)
                for i, day in enumerate(days):
                    update_range_scrape_job(job_id, days_done=i, current_date=day)
                    outcome = _portal_extract_table_for_date(
                        page,
                        cfg,
                        day,
                        range_count_base=(inserted, updated, failed_rows),
                        range_rows_base=total_fetched,
                    )
                    total_fetched += len(outcome.records)
                    if outcome.streamed:
                        inserted += outcome.inserted
                        updated += outcome.updated
                        failed_rows += outcome.failed
                    else:
                        ins, upd, fail = process_portal_records(
                            outcome.records, day, source="portal_scraper"
                        )
                        inserted += ins
                        updated += upd
                        failed_rows += fail
                    if i < len(days) - 1:
                        _human_sleep()
            finally:
                context.close()

        finished = datetime.now(timezone.utc).isoformat()
        summary = (
            f"Scraped {total_fetched} row(s) from the portal; "
            f"database: {inserted} inserted, {updated} updated, {failed_rows} failed to persist."
        )
        update_range_scrape_job(
            job_id,
            status=JOB_STATUS_COMPLETED,
            days_done=len(days),
            current_date=None,
            total_fetched=total_fetched,
            inserted_count=inserted,
            updated_count=updated,
            failed_count=failed_rows,
            finished_at=finished,
            result_summary=summary,
        )
        _range_job_progress_clear(job_id)
    except Exception as exc:
        finished = datetime.now(timezone.utc).isoformat()
        update_range_scrape_job(
            job_id,
            status=JOB_STATUS_FAILED,
            error_message=str(exc),
            finished_at=finished,
            result_summary=None,
        )
        _range_job_progress_clear(job_id)


@app.route("/", methods=["GET"])
def root():
    if "user" in session:
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username_raw = (request.form.get("username") or "").strip()
        username = username_raw.lower()
        password = (request.form.get("password") or "").strip()
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
            "result_summary": row["result_summary"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }
        for row in jobs
    ]
    return jsonify(payload)


@app.route("/scraper", methods=["GET"])
@login_required(role="admin")
def scraper_dashboard():
    jobs = get_range_scrape_jobs(limit=30)
    # Render a snapshot server-side so the table works even if polling fails.
    active_date, pages_completed = _cron_get_state()
    lock = _cron_lock_status()
    sync_jobs = get_sync_jobs(limit=30)
    cron_jobs = [row for row in sync_jobs if ((row["created_by"] or "") == "cron")]
    latest = cron_jobs[0] if cron_jobs else None

    def _job_row_to_dict(row):
        if not row:
            return None
        return {
            "id": row["id"],
            "selected_date": row["selected_date"],
            "status": row["status"],
            "total_fetched": row["total_fetched"],
            "inserted_count": row["inserted_count"],
            "updated_count": row["updated_count"],
            "failed_count": row["failed_count"],
            "error_message": row["error_message"],
            "result_summary": row["result_summary"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    return render_template(
        "scraper.html",
        jobs=jobs,
        cron_snapshot={
            "active_date": active_date,
            "pages_completed": pages_completed,
            "lock": lock,
            "latest_cron_job": _job_row_to_dict(latest),
        },
    )


@app.route("/scraper/start", methods=["POST"])
@login_required(role="admin")
def scraper_start_range():
    date_from = (request.form.get("date_from") or "").strip()
    date_to = (request.form.get("date_to") or "").strip()
    d0 = parse_date_yyyy_mm_dd(date_from)
    d1 = parse_date_yyyy_mm_dd(date_to)
    if not d0 or not d1:
        flash("Use YYYY-MM-DD for both dates.", "error")
        return redirect(url_for("scraper_dashboard"))
    if d1 < d0:
        flash("End date must be on or after start date.", "error")
        return redirect(url_for("scraper_dashboard"))
    try:
        span = len(_daterange_inclusive_iso(date_from, date_to))
        max_days = int(_env("SCRAPER_MAX_RANGE_DAYS", "31"))
        if span > max_days:
            flash(f"Range too large ({span} days). Max is {max_days} (SCRAPER_MAX_RANGE_DAYS).", "error")
            return redirect(url_for("scraper_dashboard"))
    except ValueError:
        flash("Invalid date range.", "error")
        return redirect(url_for("scraper_dashboard"))

    job_id = create_range_scrape_job(date_from, date_to, session.get("user"))
    worker = threading.Thread(target=run_range_scrape_job, args=(job_id, date_from, date_to), daemon=True)
    worker.start()
    flash(
        f"Range scrape job #{job_id} queued ({date_from} … {date_to}). "
        "Refresh the table below (or wait for auto-refresh) to see Rows scraped and Summary when it finishes.",
        "success",
    )
    return redirect(url_for("scraper_dashboard"))


@app.route("/scraper/jobs", methods=["GET"])
@login_required(role="admin")
def range_scrape_jobs_api():
    jobs = get_range_scrape_jobs(limit=30)
    payload = [
        {
            "id": row["id"],
            "date_from": row["date_from"],
            "date_to": row["date_to"],
            "status": row["status"],
            "days_total": row["days_total"],
            "days_done": row["days_done"],
            "current_date": row["current_date"],
            "total_fetched": row["total_fetched"],
            "inserted_count": row["inserted_count"],
            "updated_count": row["updated_count"],
            "failed_count": row["failed_count"],
            "error_message": row["error_message"],
            "result_summary": row["result_summary"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }
        for row in jobs
    ]
    return jsonify(payload)


@app.route("/scraper/cron-status", methods=["GET"])
@login_required(role="admin")
def scraper_cron_status_api():
    """
    Dashboard API: show cron runner state + latest cron sync job.
    """
    active_date, pages_completed = _cron_get_state()
    lock = _cron_lock_status()
    jobs = get_sync_jobs(limit=30)
    cron_jobs = [row for row in jobs if ((row["created_by"] or "") == "cron")]
    latest = cron_jobs[0] if cron_jobs else None

    def _job_row_to_dict(row):
        if not row:
            return None
        return {
            "id": row["id"],
            "selected_date": row["selected_date"],
            "status": row["status"],
            "total_fetched": row["total_fetched"],
            "inserted_count": row["inserted_count"],
            "updated_count": row["updated_count"],
            "failed_count": row["failed_count"],
            "error_message": row["error_message"],
            "result_summary": row["result_summary"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    return jsonify(
        {
            "active_date": active_date,
            "pages_completed": pages_completed,
            "lock": lock,
            "latest_cron_job": _job_row_to_dict(latest),
        }
    )


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
    raw_stream = file.stream
    # Some deployment setups hand us a stream-like object that doesn't fully
    # implement the io.IOBase interface (e.g., missing `.readable()`), which
    # breaks `io.TextIOWrapper`. Normalize it to a compatible binary stream.
    if not hasattr(raw_stream, "readable"):
        raw_stream = getattr(raw_stream, "_file", raw_stream)
    if not hasattr(raw_stream, "readable"):
        raw_stream = io.BytesIO(file.read())
    stream = io.TextIOWrapper(raw_stream, encoding="utf-8-sig", newline="")
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
    # Single local URL for admin + caller + scraper UI: http://localhost:5001 (override with FLASK_PORT or PORT in .env).
    _port = int(os.getenv("FLASK_PORT") or os.getenv("PORT") or "5001")
    # Disable the reloader: it spawns an extra process which can amplify SQLite locking issues.
    # Listen on localhost only (use a reverse proxy if you need LAN/public access).
    print(f"[Flask] http://localhost:{_port}/  (login -> dashboard / scraper)")
    # Debug mode can spawn extra behavior and makes it easier to accidentally run multiple servers.
    # Keep it off for a stable polling + cron dashboard experience.
    app.run(host="localhost", port=_port, debug=False, use_reloader=False)
