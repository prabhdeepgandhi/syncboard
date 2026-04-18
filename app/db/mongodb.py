from motor.motor_asyncio import AsyncIOMotorClient
from app.core.config import settings

client: AsyncIOMotorClient = None


async def connect_db():
    global client
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[settings.database_name]
    await create_indexes(db)
    print(f"Connected to MongoDB: {settings.database_name}")


async def close_db():
    global client
    if client:
        client.close()


def get_db():
    return client[settings.database_name]


async def create_indexes(db):
    # Compound index for sub-100ms reads on workspace home views
    await db.workspaces.create_index([("owner_id", 1), ("last_modified", -1)])
    await db.workspaces.create_index([("members.user_id", 1)])

    # Materialized path index for hierarchy traversal
    await db.nodes.create_index([("path", 1)])
    await db.nodes.create_index([("workspace_id", 1), ("parent_id", 1)])
    await db.nodes.create_index([("owner_id", 1), ("last_modified", -1)])

    # TTL index: auto-purge soft-deleted items after 30 days
    await db.nodes.create_index(
        [("deleted_at", 1)],
        expireAfterSeconds=30 * 24 * 60 * 60,
        sparse=True,
    )

    # Text index for fuzzy search across titles and descriptions
    await db.nodes.create_index(
        [("title", "text"), ("description", "text")],
        name="nodes_text_search",
    )

    # Activity log: compound index for bucket pattern queries
    await db.activity_logs.create_index([("workspace_id", 1), ("date", -1)])

    # Version index for optimistic locking conflict detection
    await db.nodes.create_index([("_id", 1), ("version", 1)])

    print("Indexes created.")
