from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Literal["RUB", "USD", "EUR"]
    description: Annotated[str, Field(min_length=1, max_length=1000)]
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: Annotated[HttpUrl, Field(max_length=2048)]

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, amount: Decimal) -> Decimal:
        return amount.quantize(Decimal("0.01"))


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    payment_id: UUID = Field(validation_alias="id")
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, JsonValue] = Field(validation_alias="details")
    webhook_url: str
    processed_at: datetime | None
