"""Orchestrates scrape + Mongo insert + job status updates."""

from __future__ import annotations

import logging
import time
from typing import Any

from config import get_settings
from db.client import get_database
from db.jobs import update_job
from db.leads import ensure_lead_indexes, try_insert_one_lead
from scraper.runner import scrape_with_retries

logger = logging.getLogger(__name__)


async def run_fetch_job(job_id: str, selected_date: str) -> None:
    settings = get_settings()
    db = get_database(settings)
    await ensure_lead_indexes(db)
    await update_job(
        db,
        job_id,
        result_summary="Starting browser scrape (rows save to DB as each page is read)…",
    )

    batch_seen: set[str] = set()
    inserted = 0
    skipped = 0
    failed = 0
    total_fetched = 0
    last_flush = 0.0

    async def on_attempt_begin() -> None:
        nonlocal batch_seen
        batch_seen = set()

    async def on_page_rows(rows: list[dict[str, Any]]) -> None:
        nonlocal inserted, skipped, failed, total_fetched, last_flush
        for r in rows:
            outcome = await try_insert_one_lead(
                db, r, selected_date, batch_seen=batch_seen
            )
            total_fetched += 1
            if outcome == "inserted":
                inserted += 1
            elif outcome == "skipped":
                skipped += 1
            else:
                failed += 1
            now = time.monotonic()
            force = total_fetched == 1 or total_fetched % 25 == 0 or (now - last_flush) >= 0.8
            if force:
                last_flush = now
                summary = (
                    f"Scraping… {total_fetched} row(s) from portal; "
                    f"DB: {inserted} new, {skipped} skipped/dup, {failed} failed."
                )
                await update_job(
                    db,
                    job_id,
                    total_fetched=total_fetched,
                    inserted_count=inserted,
                    skipped_duplicates=skipped + failed,
                    result_summary=summary,
                )

    try:
        records = await scrape_with_retries(
            settings,
            selected_date,
            on_page_rows=on_page_rows,
            on_attempt_begin=on_attempt_begin,
        )
        summary_done = (
            f"Done: {len(records)} row(s) from portal; "
            f"{inserted} new in DB, {skipped} skipped/dup, {failed} failed."
        )
        await update_job(
            db,
            job_id,
            status="completed",
            total_fetched=len(records),
            inserted_count=inserted,
            skipped_duplicates=skipped + failed,
            result_summary=summary_done,
        )
        logger.info(
            "Job %s completed: fetched=%s inserted=%s skipped_or_failed=%s",
            job_id,
            len(records),
            inserted,
            skipped + failed,
        )
    except Exception as exc:
        logger.exception("Job %s failed", job_id)
        await update_job(
            db,
            job_id,
            status="failed",
            error_message=str(exc),
            result_summary="Failed (see error_message).",
        )
