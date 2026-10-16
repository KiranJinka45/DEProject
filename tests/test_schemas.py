"""Unit tests for Pydantic V2 data contracts and defensive schema enforcement."""

import datetime
from decimal import Decimal
import pytest
from pydantic import ValidationError
from src.ingestion.schemas import OrderContract, OrderItemContract, CustomerContract, ProductContract


def test_valid_order_contract():
    order_data = {
        "order_id": "ORD-1001",
        "customer_id": "CUST-500",
        "order_status": "COMPLETED",
        "order_timestamp": "2024-05-15T14:30:00Z",
        "total_amount": 199.99,
        "currency": "USD",
        "shipping_address": "742 Evergreen Terrace, Springfield",
        "payment_method": "credit_card",
    }
    contract = OrderContract(**order_data)
    assert contract.order_id == "ORD-1001"
    assert contract.order_status == "completed"  # Lowercased by validator
    assert contract.total_amount == Decimal("199.99")


def test_invalid_order_status():
    order_data = {
        "order_id": "ORD-1002",
        "customer_id": "CUST-500",
        "order_status": "UNKNOWN_GARBAGE_STATUS",
        "order_timestamp": "2024-05-15T14:30:00Z",
        "total_amount": 150.00,
    }
    with pytest.raises(ValidationError) as exc_info:
        OrderContract(**order_data)
    assert "Invalid order status" in str(exc_info.value)


def test_order_item_contract_subtotal_and_quantity():
    item_data = {
        "order_item_id": "ITEM-901",
        "order_id": "ORD-1001",
        "product_id": "PROD-20",
        "quantity": 2,
        "unit_price": 50.00,
        "discount_amount": 10.00,
        "subtotal": 90.00,
    }
    contract = OrderItemContract(**item_data)
    assert contract.quantity == 2
    assert contract.subtotal == Decimal("90.00")

    # Quantity cannot be <= 0
    item_data["quantity"] = 0
    with pytest.raises(ValidationError):
        OrderItemContract(**item_data)
