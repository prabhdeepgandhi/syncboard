"""
Outlier Pattern: handles "Mega-Workspaces" that exceed MongoDB's 16MB document limit.

When a workspace document grows too large (tracked via overflow_refs), its
excess child references are split into linked overflow documents, keeping the
main workspace document under the limit.

This is a reference implementation — in production, document size would be
monitored via Change Streams or periodic jobs. Here we expose an explicit
check-and-split endpoint for demonstration.
"""
from bson import ObjectId
from datetime import datetime, timezone
from app.db.mongodb import get_db

# Threshold: trigger split when a workspace has more than this many
# direct root-level nodes (proxy for document size growth risk).
OVERFLOW_THRESHOLD = 500


async def check_and_split_workspace(workspace_id: str, user_id: str) -> dict:
    """
    Check if workspace needs splitting (outlier pattern).
    If root node count exceeds threshold, create an overflow shard document
    and link it back to the main workspace.
    """
    db = get_db()
    ws = await db.workspaces.find_one({"_id": ObjectId(workspace_id)})
    if not ws:
        raise ValueError("Workspace not found")

    member = next((m for m in ws.get("members", []) if m["user_id"] == user_id), None)
    if not member or member["role"] != "owner":
        raise PermissionError("Only owner can trigger outlier split")

    root_count = await db.nodes.count_documents(
        {"workspace_id": workspace_id, "parent_id": None, "is_deleted": False}
    )

    if root_count <= OVERFLOW_THRESHOLD:
        return {
            "workspace_id": workspace_id,
            "root_node_count": root_count,
            "needs_split": False,
            "overflow_shards": ws.get("overflow_refs", []),
        }

    # Find oldest root nodes to move into overflow shard
    cursor = (
        db.nodes.find(
            {"workspace_id": workspace_id, "parent_id": None, "is_deleted": False}
        )
        .sort("created_at", 1)
        .limit(root_count - OVERFLOW_THRESHOLD)
    )
    overflow_node_ids = [str(doc["_id"]) async for doc in cursor]

    # Create overflow shard document
    shard_doc = {
        "workspace_id": workspace_id,
        "type": "overflow_shard",
        "node_ids": overflow_node_ids,
        "created_at": datetime.now(timezone.utc),
    }
    shard_result = await db.workspace_shards.insert_one(shard_doc)
    shard_id = str(shard_result.inserted_id)

    # Update workspace to reference shard
    await db.workspaces.update_one(
        {"_id": ObjectId(workspace_id)},
        {
            "$push": {"overflow_refs": shard_id},
            "$set": {"is_overflow": True, "last_modified": datetime.now(timezone.utc)},
        },
    )

    return {
        "workspace_id": workspace_id,
        "root_node_count": root_count,
        "needs_split": True,
        "shard_id": shard_id,
        "nodes_in_shard": len(overflow_node_ids),
        "overflow_shards": ws.get("overflow_refs", []) + [shard_id],
    }
