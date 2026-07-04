from pydantic import BaseModel
from typing import Generic, TypeVar

from app.schemas.project import ProjectResponse
from app.schemas.queue import QueueResponse
from app.schemas.job import JobResponse
from app.schemas.dlq import DLQJobResponse

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    total: int
    page: int
    page_size: int
    items: list[T]


class PaginatedProjectsResponse(PaginatedResponse[ProjectResponse]):
    pass


class PaginatedQueuesResponse(PaginatedResponse[QueueResponse]):
    pass


class PaginatedJobsResponse(PaginatedResponse[JobResponse]):
    pass


class PaginatedDLQResponse(PaginatedResponse[DLQJobResponse]):
    pass
