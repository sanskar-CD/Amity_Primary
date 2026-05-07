"""Persisted admin toggles (manual vs auto sync)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase

SETTINGS_DOC_ID = "app"
SETTINGS_COLLECTION = "app_settings"


async def get_settings_doc(db: AsyncIOMotorDatabase) -> dict[str, Any]:
    doc = await db[SETTINGS_COLLECTION].find_one({"_id": SETTINGS_DOC_ID})
    if doc:
        doc.pop("_id", None)
        return doc
    now = datetime.now(timezone.utc).isoformat()
    await db[SETTINGS_COLLECTION].update_one(
        {"_id": SETTINGS_DOC_ID},
        {"$setOnInsert": {"_id": SETTINGS_DOC_ID, "auto_sync_enabled": False, "updated_at": now}},
        upsert=True,
    )
    doc = await db[SETTINGS_COLLECTION].find_one({"_id": SETTINGS_DOC_ID})
    if not doc:
        return {"auto_sync_enabled": False, "updated_at": now}
    doc.pop("_id", None)
    return doc


async def set_auto_sync_enabled(db: AsyncIOMotorDatabase, enabled: bool) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    await db[SETTINGS_COLLECTION].replace_one(
        {"_id": SETTINGS_DOC_ID},
        {"_id": SETTINGS_DOC_ID, "auto_sync_enabled": enabled, "updated_at": now},
        upsert=True,
    )
    return await get_settings_doc(db)
