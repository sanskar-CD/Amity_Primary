from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase

JOBS_COLLECTION = "scrape_jobs"


async def ensure_job_indexes(db: AsyncIOMotorDatabase) -> None:
    await db[JOBS_COLLECTION].create_index("job_id", unique=True)
    await db[JOBS_COLLECTION].create_index([("created_at", -1)])


async def create_job(db: AsyncIOMotorDatabase, selected_date: str) -> str:
    await ensure_job_indexes(db)
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    doc = {
        "job_id": job_id,
        "status": "running",
        "selected_date": selected_date,
        "created_at": now,
        "finished_at": None,
        "error_message": None,
        "result_summary": None,
        "total_fetched": 0,
        "inserted_count": 0,
        "skipped_duplicates": 0,
    }
    await db[JOBS_COLLECTION].insert_one(doc)
    return job_id


async def update_job(
    db: AsyncIOMotorDatabase,
    job_id: str,
    *,
    status: str | None = None,
    error_message: str | None = None,
    result_summary: str | None = None,
    total_fetched: int | None = None,
    inserted_count: int | None = None,
    skipped_duplicates: int | None = None,
) -> None:
    patch: dict[str, Any] = {}
    if status is not None:
        patch["status"] = status
    if error_message is not None:
        patch["error_message"] = error_message
    if result_summary is not None:
        patch["result_summary"] = result_summary
    if total_fetched is not None:
        patch["total_fetched"] = total_fetched
    if inserted_count is not None:
        patch["inserted_count"] = inserted_count
    if skipped_duplicates is not None:
        patch["skipped_duplicates"] = skipped_duplicates
    if status == "completed":
        patch["error_message"] = None
    if status in ("completed", "failed"):
        patch["finished_at"] = datetime.now(timezone.utc)
    if patch:
        await db[JOBS_COLLECTION].update_one({"job_id": job_id}, {"$set": patch})


async def get_job(db: AsyncIOMotorDatabase, job_id: str) -> dict[str, Any] | None:
    doc = await db[JOBS_COLLECTION].find_one({"job_id": job_id})
    if not doc:
        return None
    doc.pop("_id", None)
    if isinstance(doc.get("created_at"), datetime):
        doc["created_at"] = doc["created_at"].isoformat()
    if isinstance(doc.get("finished_at"), datetime):
        doc["finished_at"] = doc["finished_at"].isoformat()
    return doc


async def list_jobs(db: AsyncIOMotorDatabase, limit: int = 50) -> list[dict[str, Any]]:
    cursor = db[JOBS_COLLECTION].find().sort("created_at", -1).limit(limit)
    out: list[dict[str, Any]] = []
    async for doc in cursor:
        doc.pop("_id", None)
        if isinstance(doc.get("created_at"), datetime):
            doc["created_at"] = doc["created_at"].isoformat()
        if isinstance(doc.get("finished_at"), datetime):
            doc["finished_at"] = doc["finished_at"].isoformat()
        out.append(doc)
    return out
