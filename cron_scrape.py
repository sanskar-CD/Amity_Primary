#!/usr/bin/env python3
"""
Scheduled portal scrape entrypoint.

Windows: run ``powershell -ExecutionPolicy Bypass -File setup_scraper_schedule.ps1`` once
to register a Task Scheduler job every 30 minutes (default; configurable).

Linux/macOS cron (every 30 minutes):

    */30 * * * * cd /path/to/Amity_Primary && /path/to/python cron_scrape.py >> cron_scrape.log 2>&1
"""
from __future__ import annotations

import traceback
import os
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(ROOT, ".env"))


def _env_yes(name: str, default: str = "true") -> bool:
    v = (os.environ.get(name) or default).strip().lower()
    return v in ("1", "true", "yes", "on")


# Background runs usually have no interactive desktop — default to headless for cron only.
if _env_yes("SCRAPER_CRON_USE_HEADLESS", "true"):
    os.environ["SCRAPER_HEADLESS"] = "true"

import app  # noqa: E402


def main() -> None:
    os.chdir(ROOT)
    app.init_db()
    app.run_scraper_cron_tick()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        # Task Scheduler doesn't reliably surface stdout/stderr. Always append a traceback to a log file.
        log_path = os.environ.get("SCRAPER_CRON_LOG_PATH") or os.path.join(ROOT, "cron_scrape.log")
        ts = datetime.now(timezone.utc).isoformat()
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"\n[{ts}] cron_scrape.py failed\n")
                f.write(traceback.format_exc())
                f.write("\n")
        except Exception:
            # As a last resort, print to stderr (may still be captured in some environments).
            traceback.print_exc()
        raise
