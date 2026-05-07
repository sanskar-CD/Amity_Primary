from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from config import get_settings
from db.client import get_database
from db.jobs import create_job, get_job, list_jobs
from db.leads import find_leads_by_date
from db.settings_store import get_settings_doc, set_auto_sync_enabled
from services.pipeline import run_fetch_job

router = APIRouter()


class FetchLeadsBody(BaseModel):
    selected_date: str = Field(..., description="YYYY-MM-DD")


class JobQueuedResponse(BaseModel):
    job_id: str
    status: str


class AutoSyncBody(BaseModel):
    auto_sync_enabled: bool


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_iso_date(value: str) -> None:
    if not DATE_RE.match(value):
        raise HTTPException(status_code=400, detail="selected_date must be YYYY-MM-DD")
    date.fromisoformat(value)


@router.post("/fetch-leads", response_model=JobQueuedResponse)
async def fetch_leads(body: FetchLeadsBody, background_tasks: BackgroundTasks) -> JobQueuedResponse:
    _parse_iso_date(body.selected_date)
    settings = get_settings()
    db = get_database(settings)
    job_id = await create_job(db, body.selected_date)
    background_tasks.add_task(run_fetch_job, job_id, body.selected_date)
    return JobQueuedResponse(job_id=job_id, status="running")


@router.get("/jobs/{job_id}")
async def job_status(job_id: str) -> dict[str, Any]:
    settings = get_settings()
    db = get_database(settings)
    doc = await get_job(db, job_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Job not found")
    return doc


@router.get("/jobs")
async def jobs_list(limit: int = Query(50, ge=1, le=200)) -> list[dict[str, Any]]:
    settings = get_settings()
    db = get_database(settings)
    return await list_jobs(db, limit=limit)


@router.get("/leads")
async def leads_by_date(
    lead_date: str = Query(..., alias="date", description="YYYY-MM-DD"),
    limit: int = Query(2000, ge=1, le=10000),
    skip: int = Query(0, ge=0),
) -> dict[str, Any]:
    _parse_iso_date(lead_date)
    settings = get_settings()
    db = get_database(settings)
    items = await find_leads_by_date(db, lead_date, limit=limit, skip=skip)
    return {"date": lead_date, "count": len(items), "leads": items}


@router.get("/settings/sync")
async def get_sync_settings() -> dict[str, Any]:
    settings = get_settings()
    db = get_database(settings)
    doc = await get_settings_doc(db)
    return {"auto_sync_enabled": doc.get("auto_sync_enabled", False)}


@router.patch("/settings/sync")
async def patch_sync_settings(body: AutoSyncBody) -> dict[str, Any]:
    settings = get_settings()
    db = get_database(settings)
    doc = await set_auto_sync_enabled(db, body.auto_sync_enabled)
    return {"auto_sync_enabled": doc.get("auto_sync_enabled", False)}


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
