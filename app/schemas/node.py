from typing import Any, Optional
from pydantic import BaseModel
from enum import Enum


class NodeType(str, Enum):
    folder = "folder"
    doc = "doc"
    task = "task"


class NodeCreate(BaseModel):
    title: str
    node_type: NodeType
    parent_id: Optional[str] = None
    description: Optional[str] = None
    # Zero-schema: arbitrary user-defined fields
    custom_fields: dict[str, Any] = {}


class NodeUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    custom_fields: Optional[dict[str, Any]] = None
    # Optimistic locking: client must send current version
    version: int


class NodeMoveRequest(BaseModel):
    new_parent_id: Optional[str] = None  # None = root of workspace
    version: int


class NodeOut(BaseModel):
    id: str
    workspace_id: str
    node_type: str
    title: str
    description: Optional[str]
    parent_id: Optional[str]
    path: str
    custom_fields: dict[str, Any]
    version: int
    created_by: str
    created_at: str
    last_modified: str
    is_deleted: bool
