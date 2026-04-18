from fastapi import APIRouter, Depends, HTTPException
from app.core.deps import get_current_user
from app.schemas.workspace import WorkspaceCreate, WorkspaceUpdate, MemberAdd, WorkspaceOut
from app.services import workspace_service, outlier_service

router = APIRouter(prefix="/workspaces", tags=["workspaces"])


def _handle(e: Exception):
    if isinstance(e, PermissionError):
        raise HTTPException(403, str(e))
    raise HTTPException(400, str(e))


@router.post("", response_model=WorkspaceOut, status_code=201)
async def create(data: WorkspaceCreate, user=Depends(get_current_user)):
    try:
        return await workspace_service.create_workspace(data, user["id"])
    except Exception as e:
        _handle(e)


@router.get("", response_model=list[WorkspaceOut])
async def list_all(user=Depends(get_current_user)):
    return await workspace_service.list_workspaces(user["id"])


@router.get("/{workspace_id}", response_model=WorkspaceOut)
async def get(workspace_id: str, user=Depends(get_current_user)):
    try:
        return await workspace_service.get_workspace(workspace_id, user["id"])
    except Exception as e:
        _handle(e)


@router.patch("/{workspace_id}", response_model=WorkspaceOut)
async def update(workspace_id: str, data: WorkspaceUpdate, user=Depends(get_current_user)):
    try:
        return await workspace_service.update_workspace(workspace_id, data, user["id"])
    except Exception as e:
        _handle(e)


@router.post("/{workspace_id}/members", response_model=WorkspaceOut)
async def add_member(workspace_id: str, data: MemberAdd, user=Depends(get_current_user)):
    try:
        return await workspace_service.add_member(workspace_id, data, user["id"])
    except Exception as e:
        _handle(e)


@router.delete("/{workspace_id}", status_code=204)
async def delete(workspace_id: str, user=Depends(get_current_user)):
    try:
        await workspace_service.delete_workspace(workspace_id, user["id"])
    except Exception as e:
        _handle(e)


@router.post("/{workspace_id}/split-overflow")
async def split_overflow(workspace_id: str, user=Depends(get_current_user)):
    """Outlier pattern: split oversized workspace into linked shard documents."""
    try:
        return await outlier_service.check_and_split_workspace(workspace_id, user["id"])
    except Exception as e:
        _handle(e)
