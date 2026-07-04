import uuid
from datetime import datetime
from pydantic import BaseModel
from app.schemas.job import JobResponse

class DLQJobResponse(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    failure_reason: str
    moved_at: datetime
    job: JobResponse

    model_config = {"from_attributes": True}
