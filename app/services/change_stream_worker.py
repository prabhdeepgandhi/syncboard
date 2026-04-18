"""
Change Stream Worker: listens to MongoDB change events on the nodes collection
and publishes notification events to Redis pub/sub.

Run as background task on app startup (or as standalone process).
"""
import asyncio
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


async def start_change_stream_worker(db, redis_client=None):
    """
    Watches the nodes collection change stream.
    On insert/update/delete, publishes a JSON event to Redis channel
    'syncboard:notifications:{workspace_id}'.
    Falls back to logging if Redis is unavailable.
    """
    try:
        pipeline = [
            {
                "$match": {
                    "operationType": {"$in": ["insert", "update", "delete", "replace"]}
                }
            }
        ]
        async with db.nodes.watch(pipeline, full_document="updateLookup") as stream:
            logger.info("Change stream worker started.")
            async for change in stream:
                await _handle_change(change, redis_client)
    except Exception as e:
        logger.error(f"Change stream error: {e}")


async def _handle_change(change: dict, redis_client=None):
    op = change.get("operationType")
    doc = change.get("fullDocument") or {}
    workspace_id = doc.get("workspace_id", "unknown")

    event = {
        "type": op,
        "node_id": str(change.get("documentKey", {}).get("_id", "")),
        "workspace_id": workspace_id,
        "title": doc.get("title", ""),
        "node_type": doc.get("node_type", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    channel = f"syncboard:notifications:{workspace_id}"
    payload = json.dumps(event)

    if redis_client:
        try:
            await redis_client.publish(channel, payload)
        except Exception as e:
            logger.warning(f"Redis publish failed: {e}. Event: {payload}")
    else:
        logger.info(f"[ChangeStream] {channel}: {payload}")
