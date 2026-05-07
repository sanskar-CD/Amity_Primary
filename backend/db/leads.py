from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from motor.motor_asyncio import AsyncIOMotorDatabase

from utils.dedupe import compute_dedupe_key

LEADS_COLLECTION = "leads"


async def ensure_lead_indexes(db: AsyncIOMotorDatabase) -> None:
    await db[LEADS_COLLECTION].create_index("dedupe_key")
    await db[LEADS_COLLECTION].create_index("lead_date")
    await db[LEADS_COLLECTION].create_index([("lead_date", 1), ("dedupe_key", 1)])


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {str(k): ("" if v is None else str(v).strip()) for k, v in row.items()}


async def try_insert_one_lead(
    db: AsyncIOMotorDatabase,
    raw: dict[str, Any],
    selected_date: str,
    *,
    source: str = "playwright_portal",
    batch_seen: set[str],
) -> Literal["inserted", "skipped", "failed"]:
    """
    Persist a single scraped row if it is new (dedupe_key not already in DB or this scrape batch).
    ``batch_seen`` must be the same set for the whole job so duplicate rows within one scrape are skipped once.
    """
    if not isinstance(raw, dict):
        return "failed"
    row = _normalize_row(raw)
    dk = compute_dedupe_key(row)
    if dk in batch_seen:
        return "skipped"
    batch_seen.add(dk)
    coll = db[LEADS_COLLECTION]
    existing = await coll.find_one({"dedupe_key": dk}, {"_id": 1})
    if existing is not None:
        return "skipped"
    now = datetime.now(timezone.utc)
    await coll.insert_one(
        {
            "dedupe_key": dk,
            "lead_date": selected_date,
            "source": source,
            "data": row,
            "created_at": now,
        }
    )
    return "inserted"


async def insert_new_leads(
    db: AsyncIOMotorDatabase,
    records: list[dict[str, Any]],
    selected_date: str,
    *,
    source: str = "playwright_portal",
) -> tuple[int, int, int]:
    """
    Append only new leads (dedupe by dedupe_key globally in collection).
    Returns (inserted, skipped_duplicates, failed_parse).
    """
    await ensure_lead_indexes(db)
    coll = db[LEADS_COLLECTION]
    inserted = 0
    skipped = 0
    failed = 0
    pending_keys: list[str] = []
    key_to_row: dict[str, dict[str, Any]] = {}

    for raw in records:
        if not isinstance(raw, dict):
            failed += 1
            continue
        row = _normalize_row(raw)
        dk = compute_dedupe_key(row)
        if dk in key_to_row:
            skipped += 1
            continue
        key_to_row[dk] = row
        pending_keys.append(dk)

    if not pending_keys:
        return 0, skipped, failed

    existing = await coll.find({"dedupe_key": {"$in": pending_keys}}, {"dedupe_key": 1}).to_list(
        length=len(pending_keys)
    )
    existing_set = {e["dedupe_key"] for e in existing}

    now = datetime.now(timezone.utc)
    docs: list[dict[str, Any]] = []
    for dk, row in key_to_row.items():
        if dk in existing_set:
            skipped += 1
            continue
        existing_set.add(dk)
        docs.append(
            {
                "dedupe_key": dk,
                "lead_date": selected_date,
                "source": source,
                "data": row,
                "created_at": now,
            }
        )

    if docs:
        await coll.insert_many(docs)
        inserted = len(docs)

    return inserted, skipped, failed


async def find_leads_by_date(
    db: AsyncIOMotorDatabase,
    lead_date: str,
    *,
    limit: int = 5000,
    skip: int = 0,
) -> list[dict[str, Any]]:
    cursor = (
        db[LEADS_COLLECTION].find({"lead_date": lead_date}).skip(skip).limit(limit).sort("created_at", -1)
    )
    out: list[dict[str, Any]] = []
    async for doc in cursor:
        doc.pop("_id", None)
        if isinstance(doc.get("created_at"), datetime):
            doc["created_at"] = doc["created_at"].isoformat()
        out.append(doc)
    return out
