from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from config import Settings, get_settings

_client: AsyncIOMotorClient | None = None


def get_mongo_client(settings: Settings | None = None) -> AsyncIOMotorClient:
    global _client
    if _client is None:
        s = settings or get_settings()
        _client = AsyncIOMotorClient(s.mongodb_uri)
    return _client


def get_database(settings: Settings | None = None) -> AsyncIOMotorDatabase:
    s = settings or get_settings()
    return get_mongo_client(s)[s.mongodb_db]


async def close_mongo() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None
