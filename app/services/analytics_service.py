from datetime import datetime, timezone
from bson import ObjectId
from app.db.mongodb import get_db


async def workspace_summary(workspace_id: str, user_id: str) -> dict:
    """
    Aggregation pipeline: counts total tasks, completed tasks, overdue items
    in a single DB call.
    """
    db = get_db()
    ws = await db.workspaces.find_one({"_id": ObjectId(workspace_id)})
    if not ws:
        raise ValueError("Workspace not found")
    member = next((m for m in ws.get("members", []) if m["user_id"] == user_id), None)
    if not member:
        raise PermissionError("Access denied")

    now = datetime.now(timezone.utc)
    pipeline = [
        {
            "$match": {
                "workspace_id": workspace_id,
                "is_deleted": False,
            }
        },
        {
            "$facet": {
                "total_by_type": [
                    {"$group": {"_id": "$node_type", "count": {"$sum": 1}}}
                ],
                "completed_tasks": [
                    {
                        "$match": {
                            "node_type": "task",
                            "custom_fields.status": "completed",
                        }
                    },
                    {"$count": "count"},
                ],
                "overdue_tasks": [
                    {
                        "$match": {
                            "node_type": "task",
                            "custom_fields.due_date": {"$lt": now.isoformat()},
                            "custom_fields.status": {"$ne": "completed"},
                        }
                    },
                    {"$count": "count"},
                ],
            }
        },
    ]

    result = await db.nodes.aggregate(pipeline).to_list(1)
    data = result[0] if result else {}

    totals = {item["_id"]: item["count"] for item in data.get("total_by_type", [])}
    completed = data.get("completed_tasks", [{}])[0].get("count", 0)
    overdue = data.get("overdue_tasks", [{}])[0].get("count", 0)

    return {
        "workspace_id": workspace_id,
        "total_folders": totals.get("folder", 0),
        "total_docs": totals.get("doc", 0),
        "total_tasks": totals.get("task", 0),
        "completed_tasks": completed,
        "overdue_tasks": overdue,
    }


async def recent_activity(workspace_id: str, user_id: str, limit: int = 10) -> list[dict]:
    """
    Aggregation pipeline on bucket-pattern activity_logs collection.
    Returns last N changes across all daily buckets.
    """
    db = get_db()
    ws = await db.workspaces.find_one({"_id": ObjectId(workspace_id)})
    if not ws:
        raise ValueError("Workspace not found")
    member = next((m for m in ws.get("members", []) if m["user_id"] == user_id), None)
    if not member:
        raise PermissionError("Access denied")

    pipeline = [
        {"$match": {"workspace_id": workspace_id}},
        {"$sort": {"date": -1}},
        {"$limit": 7},  # last 7 daily buckets
        {"$unwind": "$events"},
        {"$replaceRoot": {"newRoot": "$events"}},
        {"$sort": {"timestamp": -1}},
        {"$limit": limit},
    ]
    events = await db.activity_logs.aggregate(pipeline).to_list(limit)
    return [
        {
            "user_id": e["user_id"],
            "action": e["action"],
            "node_id": e["node_id"],
            "title": e.get("title", ""),
            "timestamp": e["timestamp"].isoformat(),
        }
        for e in events
    ]


async def who_is_online(workspace_id: str, user_id: str) -> list[dict]:
    """
    Returns users with activity in the last 5 minutes using activity_logs.
    Change Streams hook (see change_stream_worker) pushes heartbeats to Redis;
    this falls back to activity log if Redis unavailable.
    """
    db = get_db()
    ws = await db.workspaces.find_one({"_id": ObjectId(workspace_id)})
    if not ws:
        raise ValueError("Workspace not found")
    member = next((m for m in ws.get("members", []) if m["user_id"] == user_id), None)
    if not member:
        raise PermissionError("Access denied")

    from datetime import timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=5)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    pipeline = [
        {"$match": {"workspace_id": workspace_id, "date": today}},
        {"$unwind": "$events"},
        {"$match": {"events.timestamp": {"$gte": cutoff}}},
        {"$group": {"_id": "$events.user_id", "last_seen": {"$max": "$events.timestamp"}}},
    ]
    result = await db.activity_logs.aggregate(pipeline).to_list(100)
    return [
        {"user_id": r["_id"], "last_seen": r["last_seen"].isoformat()}
        for r in result
    ]
