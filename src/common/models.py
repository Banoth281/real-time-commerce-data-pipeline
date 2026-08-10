from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class OrderEvent(BaseModel):
    event_id: UUID
    order_id: UUID
    customer_id: UUID
    product_id: str = Field(min_length=1, max_length=40)
    category: str = Field(min_length=1, max_length=40)
    quantity: int = Field(gt=0, le=100)
    unit_price: Decimal = Field(ge=0, decimal_places=2)
    country: str = Field(pattern=r"^[A-Z]{2}$")
    event_time: datetime

    @field_validator("event_time")
    @classmethod
    def timezone_required(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("event_time must include a timezone")
        if value > datetime.now(timezone.utc):
            raise ValueError("event_time cannot be in the future")
        return value

    @property
    def total_amount(self) -> Decimal:
        return (self.unit_price * self.quantity).quantize(Decimal("0.01"))

