from datetime import datetime, timezone
from typing import Optional
from bson import ObjectId
from pymongo import ReturnDocument
from app.db.mongodb import get_db
from app.schemas.node import NodeCreate, NodeUpdate, NodeMoveRequest
from app.schemas.workspace import Role


def _now():
    return datetime.now(timezone.utc)


def _fmt(doc: dict) -> dict:
    return {
        "id": str(doc["_id"]),
        "workspace_id": doc["workspace_id"],
        "node_type": doc["node_type"],
        "title": doc["title"],
        "description": doc.get("description"),
        "parent_id": doc.get("parent_id"),
        "path": doc.get("path", ""),
        "custom_fields": doc.get("custom_fields", {}),
        "version": doc.get("version", 1),
        "created_by": doc["created_by"],
        "created_at": doc["created_at"].isoformat(),
        "last_modified": doc["last_modified"].isoformat(),
        "is_deleted": doc.get("is_deleted", False),
    }


async def _assert_workspace_access(db, workspace_id: str, user_id: str, write: bool = False):
    ws = await db.workspaces.find_one({"_id": ObjectId(workspace_id)})
    if not ws:
        raise ValueError("Workspace not found")
    member = next((m for m in ws.get("members", []) if m["user_id"] == user_id), None)
    if not member:
        raise PermissionError("Access denied")
    if write and member["role"] == Role.viewer:
        raise PermissionError("Viewers cannot modify content")
    return ws


async def _build_path(db, parent_id: Optional[str], workspace_id: str) -> str:
    if not parent_id:
        return f"/{workspace_id}"
    parent = await db.nodes.find_one({"_id": ObjectId(parent_id)})
    if not parent:
        raise ValueError("Parent node not found")
    return f"{parent['path']}/{parent_id}"


async def create_node(workspace_id: str, data: NodeCreate, user_id: str) -> dict:
    db = get_db()
    await _assert_workspace_access(db, workspace_id, user_id, write=True)

    path = await _build_path(db, data.parent_id, workspace_id)
    now = _now()
    doc = {
        "workspace_id": workspace_id,
        "node_type": data.node_type,
        "title": data.title,
        "description": data.description,
        "parent_id": data.parent_id,
        "path": path,
        "custom_fields": data.custom_fields,
        "version": 1,
        "created_by": user_id,
        "created_at": now,
        "last_modified": now,
        "is_deleted": False,
    }
    result = await db.nodes.insert_one(doc)
    doc["_id"] = result.inserted_id
    await _log_activity(db, workspace_id, user_id, "create", str(result.inserted_id), data.title)
    return _fmt(doc)


async def get_node(workspace_id: str, node_id: str, user_id: str) -> dict:
    db = get_db()
    await _assert_workspace_access(db, workspace_id, user_id)
    doc = await db.nodes.find_one(
        {"_id": ObjectId(node_id), "workspace_id": workspace_id, "is_deleted": False}
    )
    if not doc:
        raise ValueError("Node not found")
    return _fmt(doc)


async def list_children(workspace_id: str, parent_id: Optional[str], user_id: str) -> list[dict]:
    db = get_db()
    await _assert_workspace_access(db, workspace_id, user_id)
    query = {
        "workspace_id": workspace_id,
        "is_deleted": False,
        "parent_id": parent_id,
    }
    cursor = db.nodes.find(query).sort("last_modified", -1)
    return [_fmt(doc) async for doc in cursor]


async def update_node(workspace_id: str, node_id: str, data: NodeUpdate, user_id: str) -> dict:
    db = get_db()
    await _assert_workspace_access(db, workspace_id, user_id, write=True)

    updates: dict = {"last_modified": _now(), "version": data.version + 1}
    if data.title is not None:
        updates["title"] = data.title
    if data.description is not None:
        updates["description"] = data.description
    if data.custom_fields is not None:
        # Merge custom fields (dynamic schema support)
        updates["$set_custom"] = data.custom_fields

    set_payload = {k: v for k, v in updates.items() if k != "$set_custom"}
    if data.custom_fields is not None:
        for k, v in data.custom_fields.items():
            set_payload[f"custom_fields.{k}"] = v

    # Optimistic locking: only update if version matches
    doc = await db.nodes.find_one_and_update(
        {
            "_id": ObjectId(node_id),
            "workspace_id": workspace_id,
            "version": data.version,
            "is_deleted": False,
        },
        {"$set": set_payload},
        return_document=ReturnDocument.AFTER,
    )
    if not doc:
        raise ValueError("Version conflict or node not found — fetch latest and retry")
    await _log_activity(db, workspace_id, user_id, "update", node_id, doc["title"])
    return _fmt(doc)


async def soft_delete_node(workspace_id: str, node_id: str, user_id: str):
    db = get_db()
    await _assert_workspace_access(db, workspace_id, user_id, write=True)
    now = _now()
    result = await db.nodes.update_one(
        {"_id": ObjectId(node_id), "workspace_id": workspace_id, "is_deleted": False},
        {"$set": {"is_deleted": True, "deleted_at": now, "last_modified": now}},
    )
    if result.matched_count == 0:
        raise ValueError("Node not found")
    # Also soft-delete all descendants (materialized path prefix match)
    node = await db.nodes.find_one({"_id": ObjectId(node_id)})
    if node:
        child_path_prefix = f"{node['path']}/{node_id}"
        await db.nodes.update_many(
            {"path": {"$regex": f"^{child_path_prefix}"}, "is_deleted": False},
            {"$set": {"is_deleted": True, "deleted_at": now, "last_modified": now}},
        )
    await _log_activity(db, workspace_id, user_id, "delete", node_id, "")


async def move_node(workspace_id: str, node_id: str, data: NodeMoveRequest, user_id: str) -> dict:
    """
    Move node + all descendants atomically via multi-document transaction.
    Handles optimistic locking via version field.
    """
    db = get_db()
    await _assert_workspace_access(db, workspace_id, user_id, write=True)

    mongo_client = db.client
    async with await mongo_client.start_session() as session:
        async with session.start_transaction():
            # Optimistic lock check
            node = await db.nodes.find_one(
                {"_id": ObjectId(node_id), "workspace_id": workspace_id, "version": data.version},
                session=session,
            )
            if not node:
                raise ValueError("Version conflict or node not found")

            old_path = node["path"]
            new_path = await _build_path(db, data.new_parent_id, workspace_id)

            # Update node itself
            await db.nodes.update_one(
                {"_id": ObjectId(node_id)},
                {
                    "$set": {
                        "parent_id": data.new_parent_id,
                        "path": new_path,
                        "last_modified": _now(),
                        "version": data.version + 1,
                    }
                },
                session=session,
            )

            # Update all descendants: replace old path prefix with new path
            old_child_prefix = f"{old_path}/{node_id}"
            new_child_prefix = f"{new_path}/{node_id}"

            descendants = db.nodes.find(
                {"path": {"$regex": f"^{old_child_prefix}"}},
                session=session,
            )
            async for desc in descendants:
                updated_path = desc["path"].replace(old_child_prefix, new_child_prefix, 1)
                await db.nodes.update_one(
                    {"_id": desc["_id"]},
                    {"$set": {"path": updated_path, "last_modified": _now()}},
                    session=session,
                )

    updated = await db.nodes.find_one({"_id": ObjectId(node_id)})
    await _log_activity(db, workspace_id, user_id, "move", node_id, node["title"])
    return _fmt(updated)


async def search_nodes(workspace_id: str, query: str, user_id: str) -> list[dict]:
    """Text search across titles and descriptions using MongoDB text index."""
    db = get_db()
    await _assert_workspace_access(db, workspace_id, user_id)
    cursor = db.nodes.find(
        {
            "$text": {"$search": query},
            "workspace_id": workspace_id,
            "is_deleted": False,
        },
        {"score": {"$meta": "textScore"}},
    ).sort([("score", {"$meta": "textScore"})])
    return [_fmt(doc) async for doc in cursor]


async def _log_activity(db, workspace_id: str, user_id: str, action: str, node_id: str, title: str):
    """
    Bucket Pattern: group activity logs by day into single document.
    Avoids small-document overhead for high-frequency writes.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    event = {
        "user_id": user_id,
        "action": action,
        "node_id": node_id,
        "title": title,
        "timestamp": _now(),
    }
    await db.activity_logs.update_one(
        {"workspace_id": workspace_id, "date": today},
        {
            "$push": {"events": event},
            "$inc": {"event_count": 1},
            "$setOnInsert": {"workspace_id": workspace_id, "date": today},
        },
        upsert=True,
    )
