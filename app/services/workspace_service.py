from datetime import datetime, timezone
from bson import ObjectId
from app.db.mongodb import get_db
from app.schemas.workspace import WorkspaceCreate, WorkspaceUpdate, MemberAdd, Role


def _now():
    return datetime.now(timezone.utc)


def _fmt(doc: dict) -> dict:
    return {
        "id": str(doc["_id"]),
        "name": doc["name"],
        "owner_id": doc["owner_id"],
        "members": doc.get("members", []),
        "metadata": doc.get("metadata", {}),
        "created_at": doc["created_at"].isoformat(),
        "last_modified": doc["last_modified"].isoformat(),
    }


async def create_workspace(data: WorkspaceCreate, user_id: str) -> dict:
    db = get_db()
    now = _now()
    doc = {
        "name": data.name,
        "owner_id": user_id,
        "members": [{"user_id": user_id, "role": Role.owner}],
        "metadata": data.metadata,
        "created_at": now,
        "last_modified": now,
        # Outlier pattern: tracks if split into linked documents
        "is_overflow": False,
        "overflow_refs": [],
    }
    result = await db.workspaces.insert_one(doc)
    doc["_id"] = result.inserted_id
    return _fmt(doc)


async def get_workspace(workspace_id: str, user_id: str) -> dict:
    db = get_db()
    doc = await db.workspaces.find_one({"_id": ObjectId(workspace_id)})
    if not doc:
        raise ValueError("Workspace not found")
    _assert_member(doc, user_id)
    return _fmt(doc)


async def list_workspaces(user_id: str) -> list[dict]:
    db = get_db()
    cursor = db.workspaces.find({"members.user_id": user_id}).sort("last_modified", -1)
    return [_fmt(doc) async for doc in cursor]


async def update_workspace(workspace_id: str, data: WorkspaceUpdate, user_id: str) -> dict:
    db = get_db()
    doc = await db.workspaces.find_one({"_id": ObjectId(workspace_id)})
    if not doc:
        raise ValueError("Workspace not found")
    _assert_role(doc, user_id, [Role.owner, Role.editor])

    updates: dict = {"last_modified": _now()}
    if data.name is not None:
        updates["name"] = data.name
    if data.metadata is not None:
        updates["metadata"] = data.metadata

    await db.workspaces.update_one({"_id": ObjectId(workspace_id)}, {"$set": updates})
    doc.update(updates)
    return _fmt(doc)


async def add_member(workspace_id: str, data: MemberAdd, user_id: str) -> dict:
    db = get_db()
    doc = await db.workspaces.find_one({"_id": ObjectId(workspace_id)})
    if not doc:
        raise ValueError("Workspace not found")
    _assert_role(doc, user_id, [Role.owner])

    # Remove existing member entry if present, then add updated
    await db.workspaces.update_one(
        {"_id": ObjectId(workspace_id)},
        {
            "$pull": {"members": {"user_id": data.user_id}},
        },
    )
    await db.workspaces.update_one(
        {"_id": ObjectId(workspace_id)},
        {
            "$push": {"members": {"user_id": data.user_id, "role": data.role}},
            "$set": {"last_modified": _now()},
        },
    )
    doc = await db.workspaces.find_one({"_id": ObjectId(workspace_id)})
    return _fmt(doc)


async def delete_workspace(workspace_id: str, user_id: str):
    db = get_db()
    doc = await db.workspaces.find_one({"_id": ObjectId(workspace_id)})
    if not doc:
        raise ValueError("Workspace not found")
    _assert_role(doc, user_id, [Role.owner])
    await db.workspaces.delete_one({"_id": ObjectId(workspace_id)})


def _get_member(doc: dict, user_id: str) -> dict | None:
    for m in doc.get("members", []):
        if m["user_id"] == user_id:
            return m
    return None


def _assert_member(doc: dict, user_id: str):
    if not _get_member(doc, user_id):
        raise PermissionError("Access denied")


def _assert_role(doc: dict, user_id: str, allowed: list[Role]):
    member = _get_member(doc, user_id)
    if not member or member["role"] not in allowed:
        raise PermissionError("Insufficient permissions")
