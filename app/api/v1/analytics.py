from fastapi import APIRouter, Depends, HTTPException, Query
from app.core.deps import get_current_user
from app.services import analytics_service

router = APIRouter(prefix="/workspaces/{workspace_id}/analytics", tags=["analytics"])


def _handle(e: Exception):
    if isinstance(e, PermissionError):
        raise HTTPException(403, str(e))
    raise HTTPException(400, str(e))


@router.get("/summary")
async def summary(workspace_id: str, user=Depends(get_current_user)):
    try:
        return await analytics_service.workspace_summary(workspace_id, user["id"])
    except Exception as e:
        _handle(e)


@router.get("/activity")
async def activity(
    workspace_id: str,
    limit: int = Query(10, ge=1, le=100),
    user=Depends(get_current_user),
):
    try:
        return await analytics_service.recent_activity(workspace_id, user["id"], limit)
    except Exception as e:
        _handle(e)


@router.get("/online")
async def online(workspace_id: str, user=Depends(get_current_user)):
    try:
        return await analytics_service.who_is_online(workspace_id, user["id"])
    except Exception as e:
        _handle(e)
