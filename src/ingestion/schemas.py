"""Data Contracts and Schema Specifications using Pydantic V2.

Defines schemas for Orders, Order Items, Customers, Products, Marketing Spend,
and Clickstream Events to validate raw inputs before bronze persistence.
"""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator


class OrderContract(BaseModel):
    """Data contract for e-commerce transactions."""
    model_config = ConfigDict(strict=False, extra="allow")

    order_id: str = Field(..., min_length=1, description="Primary business identifier for order")
    customer_id: str = Field(..., min_length=1, description="Customer reference ID")
    order_status: str = Field(..., description="Pending, Processing, Completed, Cancelled, Refunded")
    order_timestamp: datetime.datetime = Field(..., description="Timestamp in ISO 8601 UTC")
    total_amount: Decimal = Field(..., ge=0, description="Gross order amount")
    currency: str = Field(default="USD", max_length=3, description="ISO 4217 Currency code")
    shipping_address: Optional[str] = Field(default=None)
    payment_method: Optional[str] = Field(default="credit_card")

    @field_validator("order_status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        valid_statuses = {"pending", "processing", "completed", "shipped", "delivered", "cancelled", "refunded"}
        if value.lower() not in valid_statuses:
            raise ValueError(f"Invalid order status '{value}'. Allowed: {valid_statuses}")
        return value.lower()


class OrderItemContract(BaseModel):
    """Data contract for individual order line items."""
    model_config = ConfigDict(strict=False, extra="allow")

    order_item_id: str = Field(..., min_length=1)
    order_id: str = Field(..., min_length=1)
    product_id: str = Field(..., min_length=1)
    quantity: int = Field(..., gt=0)
    unit_price: Decimal = Field(..., ge=0)
    discount_amount: Decimal = Field(default=Decimal("0.00"), ge=0)
    subtotal: Decimal = Field(..., ge=0)


class CustomerContract(BaseModel):
    """Data contract for customer profile data."""
    model_config = ConfigDict(strict=False, extra="allow")

    customer_id: str = Field(..., min_length=1)
    email: str = Field(..., min_length=3)
    first_name: str = Field(...)
    last_name: str = Field(...)
    country_code: str = Field(default="US", max_length=2)
    city: Optional[str] = None
    signup_timestamp: datetime.datetime
    updated_at: datetime.datetime


class ProductContract(BaseModel):
    """Data contract for product catalog items."""
    model_config = ConfigDict(strict=False, extra="allow")

    product_id: str = Field(..., min_length=1)
    sku: str = Field(..., min_length=1)
    product_name: str = Field(..., min_length=1)
    category: str = Field(...)
    subcategory: Optional[str] = None
    cost_price: Decimal = Field(..., ge=0)
    retail_price: Decimal = Field(..., ge=0)
    is_active: bool = Field(default=True)


class MarketingSpendContract(BaseModel):
    """Data contract for marketing ad spend API reports."""
    model_config = ConfigDict(strict=False, extra="allow")

    campaign_id: str = Field(..., min_length=1)
    channel: str = Field(...)  # Google, Meta, TikTok, Email, Affiliate
    spend_date: datetime.date = Field(...)
    spend_amount: Decimal = Field(..., ge=0)
    impressions: int = Field(default=0, ge=0)
    clicks: int = Field(default=0, ge=0)
    conversions: int = Field(default=0, ge=0)


class ClickstreamEventContract(BaseModel):
    """Data contract for front-end clickstream telemetry."""
    model_config = ConfigDict(strict=False, extra="allow")

    event_id: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)
    user_id: Optional[str] = None
    event_type: str = Field(...)  # page_view, view_item, add_to_cart, checkout_step, purchase
    event_timestamp: datetime.datetime
    page_url: str
    device_category: str = Field(default="desktop")  # mobile, desktop, tablet
