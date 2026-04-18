"""Tests for analytics aggregation pipeline and bucket-pattern activity log."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from bson import ObjectId
from datetime import datetime, timezone


def make_oid():
    return str(ObjectId())


@pytest.mark.asyncio
async def test_workspace_summary_structure():
    """workspace_summary returns correct keys."""
    from app.services.analytics_service import workspace_summary

    user_id = make_oid()
    ws_id = make_oid()

    ws_doc = {
        "_id": ObjectId(ws_id),
        "members": [{"user_id": user_id, "role": "owner"}],
    }

    agg_result = [
        {
            "total_by_type": [
                {"_id": "task", "count": 5},
                {"_id": "folder", "count": 2},
            ],
            "completed_tasks": [{"count": 3}],
            "overdue_tasks": [{"count": 1}],
        }
    ]

    mock_cursor = MagicMock()
    mock_cursor.to_list = AsyncMock(return_value=agg_result)

    mock_db = MagicMock()
    mock_db.workspaces.find_one = AsyncMock(return_value=ws_doc)
    mock_db.nodes.aggregate = MagicMock(return_value=mock_cursor)

    with patch("app.services.analytics_service.get_db", return_value=mock_db):
        result = await workspace_summary(ws_id, user_id)

    assert result["total_tasks"] == 5
    assert result["completed_tasks"] == 3
    assert result["overdue_tasks"] == 1
    assert result["total_folders"] == 2


@pytest.mark.asyncio
async def test_recent_activity_returns_events():
    """recent_activity unwraps bucket events and returns last N."""
    from app.services.analytics_service import recent_activity

    user_id = make_oid()
    ws_id = make_oid()

    ws_doc = {"_id": ObjectId(ws_id), "members": [{"user_id": user_id, "role": "owner"}]}
    events = [
        {
            "user_id": user_id,
            "action": "create",
            "node_id": make_oid(),
            "title": "My Task",
            "timestamp": datetime.now(timezone.utc),
        }
    ]

    mock_cursor = MagicMock()
    mock_cursor.to_list = AsyncMock(return_value=events)

    mock_db = MagicMock()
    mock_db.workspaces.find_one = AsyncMock(return_value=ws_doc)
    mock_db.activity_logs.aggregate = MagicMock(return_value=mock_cursor)

    with patch("app.services.analytics_service.get_db", return_value=mock_db):
        result = await recent_activity(ws_id, user_id, limit=10)

    assert len(result) == 1
    assert result[0]["action"] == "create"
    assert result[0]["title"] == "My Task"
