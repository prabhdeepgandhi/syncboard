from typing import Any, Optional
from pydantic import BaseModel
from enum import Enum


class Role(str, Enum):
    owner = "owner"
    editor = "editor"
    viewer = "viewer"


class WorkspaceCreate(BaseModel):
    name: str
    metadata: dict[str, Any] = {}


class WorkspaceUpdate(BaseModel):
    name: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


class MemberAdd(BaseModel):
    user_id: str
    role: Role = Role.viewer


class WorkspaceOut(BaseModel):
    id: str
    name: str
    owner_id: str
    members: list[dict]
    metadata: dict[str, Any]
    created_at: str
    last_modified: str
