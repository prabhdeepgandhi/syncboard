from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from app.core.deps import get_current_user
from app.schemas.node import NodeCreate, NodeUpdate, NodeMoveRequest, NodeOut
from app.services import node_service

router = APIRouter(prefix="/workspaces/{workspace_id}/nodes", tags=["nodes"])


def _handle(e: Exception):
    if isinstance(e, PermissionError):
        raise HTTPException(403, str(e))
    raise HTTPException(400, str(e))


@router.post("", response_model=NodeOut, status_code=201)
async def create(workspace_id: str, data: NodeCreate, user=Depends(get_current_user)):
    try:
        return await node_service.create_node(workspace_id, data, user["id"])
    except Exception as e:
        _handle(e)


@router.get("", response_model=list[NodeOut])
async def list_children(
    workspace_id: str,
    parent_id: Optional[str] = Query(None),
    user=Depends(get_current_user),
):
    try:
        return await node_service.list_children(workspace_id, parent_id, user["id"])
    except Exception as e:
        _handle(e)


@router.get("/search", response_model=list[NodeOut])
async def search(
    workspace_id: str,
    q: str = Query(..., min_length=1),
    user=Depends(get_current_user),
):
    try:
        return await node_service.search_nodes(workspace_id, q, user["id"])
    except Exception as e:
        _handle(e)


@router.get("/{node_id}", response_model=NodeOut)
async def get(workspace_id: str, node_id: str, user=Depends(get_current_user)):
    try:
        return await node_service.get_node(workspace_id, node_id, user["id"])
    except Exception as e:
        _handle(e)


@router.patch("/{node_id}", response_model=NodeOut)
async def update(workspace_id: str, node_id: str, data: NodeUpdate, user=Depends(get_current_user)):
    try:
        return await node_service.update_node(workspace_id, node_id, data, user["id"])
    except Exception as e:
        _handle(e)


@router.delete("/{node_id}", status_code=204)
async def delete(workspace_id: str, node_id: str, user=Depends(get_current_user)):
    try:
        await node_service.soft_delete_node(workspace_id, node_id, user["id"])
    except Exception as e:
        _handle(e)


@router.post("/{node_id}/move", response_model=NodeOut)
async def move(
    workspace_id: str, node_id: str, data: NodeMoveRequest, user=Depends(get_current_user)
):
    try:
        return await node_service.move_node(workspace_id, node_id, data, user["id"])
    except Exception as e:
        _handle(e)
