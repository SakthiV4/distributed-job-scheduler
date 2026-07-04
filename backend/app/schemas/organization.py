from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime


class OrganizationResponse(BaseModel):
    id: UUID
    name: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class OrganizationUpdate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
