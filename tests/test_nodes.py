"""
Unit tests for node service logic (no live MongoDB needed).
Tests: optimistic locking conflict, soft delete, path building.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from bson import ObjectId
from datetime import datetime, timezone


def make_oid():
    return ObjectId()


def make_workspace_doc(user_id: str) -> dict:
    return {
        "_id": make_oid(),
        "name": "Test WS",
        "owner_id": user_id,
        "members": [{"user_id": user_id, "role": "owner"}],
        "metadata": {},
        "created_at": datetime.now(timezone.utc),
        "last_modified": datetime.now(timezone.utc),
    }


def make_node_doc(workspace_id: str, user_id: str, parent_id=None, title="Node") -> dict:
    oid = make_oid()
    return {
        "_id": oid,
        "workspace_id": workspace_id,
        "node_type": "task",
        "title": title,
        "description": None,
        "parent_id": parent_id,
        "path": f"/{workspace_id}",
        "custom_fields": {},
        "version": 1,
        "created_by": user_id,
        "created_at": datetime.now(timezone.utc),
        "last_modified": datetime.now(timezone.utc),
        "is_deleted": False,
    }


@pytest.mark.asyncio
async def test_optimistic_lock_conflict():
    """
    find_one_and_update returns None when version mismatches.
    Service should raise ValueError.
    """
    from app.services.node_service import update_node
    from app.schemas.node import NodeUpdate

    user_id = str(make_oid())
    ws_id = str(make_oid())
    node_id = str(make_oid())
    ws_doc = make_workspace_doc(user_id)

    mock_nodes = MagicMock()
    mock_nodes.find_one = AsyncMock(return_value=ws_doc)
    mock_nodes.find_one_and_update = AsyncMock(return_value=None)  # version mismatch

    mock_ws = MagicMock()
    mock_ws.find_one = AsyncMock(return_value=ws_doc)

    mock_db = MagicMock()
    mock_db.workspaces = mock_ws
    mock_db.nodes = mock_nodes
    mock_db.activity_logs = MagicMock()
    mock_db.activity_logs.update_one = AsyncMock()

    with patch("app.services.node_service.get_db", return_value=mock_db):
        data = NodeUpdate(title="Updated", version=1)
        with pytest.raises(ValueError, match="Version conflict"):
            await update_node(ws_id, node_id, data, user_id)


@pytest.mark.asyncio
async def test_viewer_cannot_write():
    """Viewers should get PermissionError on write ops."""
    from app.services.node_service import create_node
    from app.schemas.node import NodeCreate, NodeType

    user_id = str(make_oid())
    ws_id = str(make_oid())

    ws_doc = {
        "_id": make_oid(),
        "name": "WS",
        "owner_id": "other",
        "members": [{"user_id": user_id, "role": "viewer"}],
    }

    mock_db = MagicMock()
    mock_db.workspaces.find_one = AsyncMock(return_value=ws_doc)

    with patch("app.services.node_service.get_db", return_value=mock_db):
        data = NodeCreate(title="Task", node_type=NodeType.task)
        with pytest.raises(PermissionError):
            await create_node(ws_id, data, user_id)


@pytest.mark.asyncio
async def test_soft_delete_marks_deleted_at():
    """soft_delete_node must set is_deleted=True and deleted_at."""
    from app.services.node_service import soft_delete_node

    user_id = str(make_oid())
    ws_id = str(make_oid())
    node_id = str(make_oid())
    ws_doc = make_workspace_doc(user_id)
    node_doc = make_node_doc(ws_id, user_id)

    update_result = MagicMock()
    update_result.matched_count = 1

    mock_db = MagicMock()
    mock_db.workspaces.find_one = AsyncMock(return_value=ws_doc)
    mock_db.nodes.update_one = AsyncMock(return_value=update_result)
    mock_db.nodes.update_many = AsyncMock()
    mock_db.nodes.find_one = AsyncMock(return_value=node_doc)
    mock_db.activity_logs.update_one = AsyncMock()

    with patch("app.services.node_service.get_db", return_value=mock_db):
        await soft_delete_node(ws_id, node_id, user_id)

    # Verify update_one was called with is_deleted=True and deleted_at set
    call_args = mock_db.nodes.update_one.call_args
    set_payload = call_args[0][1]["$set"]
    assert set_payload["is_deleted"] is True
    assert "deleted_at" in set_payload
