from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from src.common.models import OrderEvent


def valid_event(**overrides):
    data = {
        "event_id": uuid4(),
        "order_id": uuid4(),
        "customer_id": uuid4(),
        "product_id": "laptop",
        "category": "Electronics",
        "quantity": 2,
        "unit_price": "899.99",
        "country": "GB",
        "event_time": datetime.now(timezone.utc) - timedelta(seconds=1),
    }
    data.update(overrides)
    return OrderEvent(**data)


def test_total_amount_is_calculated():
    assert valid_event().total_amount == Decimal("1799.98")


@pytest.mark.parametrize("quantity", [0, -1, 101])
def test_invalid_quantity_is_rejected(quantity):
    with pytest.raises(ValidationError):
        valid_event(quantity=quantity)


def test_future_event_is_rejected():
    with pytest.raises(ValidationError):
        valid_event(event_time=datetime.now(timezone.utc) + timedelta(minutes=1))

