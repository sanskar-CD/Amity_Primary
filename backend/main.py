"""
FastAPI entrypoint: lead fetch API + Mongo + background scrape jobs.
Run from `backend/`: uvicorn main:app --reload --port 8000
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import date, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import router
from config import get_settings
from db.client import close_mongo, get_database
from db.jobs import create_job
from db.settings_store import get_settings_doc
from services.pipeline import run_fetch_job
from utils.logging_config import setup_logging

logger = logging.getLogger(__name__)
_scheduler: AsyncIOScheduler | None = None


async def _scheduled_daily_fetch() -> None:
    settings = get_settings()
    db = get_database(settings)
    doc = await get_settings_doc(db)
    if not doc.get("auto_sync_enabled"):
        logger.debug("Auto sync disabled; skipping scheduled fetch")
        return
    d = date.today()
    if settings.auto_sync_use_yesterday:
        d = d - timedelta(days=1)
    selected = d.isoformat()
    job_id = await create_job(db, selected)
    logger.info("Scheduled fetch started job_id=%s date=%s", job_id, selected)
    await run_fetch_job(job_id, selected)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    get_settings.cache_clear()
    s = get_settings()
    db = get_database(s)
    from db.jobs import ensure_job_indexes
    from db.leads import ensure_lead_indexes

    await ensure_job_indexes(db)
    await ensure_lead_indexes(db)

    global _scheduler
    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(
        _scheduled_daily_fetch,
        CronTrigger(hour=s.auto_sync_cron_hour, minute=s.auto_sync_cron_minute),
        id="daily_lead_fetch",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info(
        "Scheduler started: daily job at %02d:%02d (auto when enabled in Mongo)",
        s.auto_sync_cron_hour,
        s.auto_sync_cron_minute,
    )

    yield

    if _scheduler:
        _scheduler.shutdown(wait=False)
    await close_mongo()


app = FastAPI(title="Lead Sync API", lifespan=lifespan)
s = get_settings()
origins = [o.strip() for o in s.cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "lead-sync-api", "docs": "/docs"}
