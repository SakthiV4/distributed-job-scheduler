from typing import Any, TypeVar, Generic, Sequence
from uuid import UUID
import math

from sqlalchemy import select, func, Select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.base import Base

T = TypeVar("T", bound=Base)


class PaginatedResult(Generic[T]):
    def __init__(self, items: Sequence[T], total: int, page: int, page_size: int):
        self.items = items
        self.total = total
        self.page = page
        self.page_size = page_size
        self.total_pages = math.ceil(total / page_size) if page_size > 0 else 0

    def to_dict(self, schema_fn) -> dict[str, Any]:
        return {
            "items": [schema_fn(item) for item in self.items],
            "total": self.total,
            "page": self.page,
            "page_size": self.page_size,
            "total_pages": self.total_pages,
        }


async def paginate(
    db: AsyncSession,
    query: Select,
    page: int = 1,
    page_size: int = 20,
) -> PaginatedResult:
    page = max(1, page)
    page_size = min(max(1, page_size), 100)

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Fetch page
    offset = (page - 1) * page_size
    paginated_query = query.offset(offset).limit(page_size)
    result = await db.execute(paginated_query)
    items = result.scalars().all()

    return PaginatedResult(items=items, total=total, page=page, page_size=page_size)
