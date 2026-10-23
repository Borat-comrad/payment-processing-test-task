import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

from app.db import Outbox, Payment


async def test_concurrent_duplicates_create_one_payment_and_event(api, sessions, payment_body):
    responses = await asyncio.gather(
        *[
            api.post("/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "same"})
            for _ in range(12)
        ]
    )
    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1
    payment_id = responses[0].json()["payment_id"]
    detail = (await api.get(f"/api/v1/payments/{payment_id}")).json()
    assert detail["amount"] == "125.50"
    assert detail["metadata"] == {"order": 42}
    assert detail["processed_at"] is None


async def test_idempotency_compares_normalized_body_and_rejects_changes(api, payment_body):
    headers = {"Idempotency-Key": "normalization"}
    payment_body["amount"] = "125.5"
    first = await api.post("/api/v1/payments", json=payment_body, headers=headers)
    payment_body["amount"] = "125.50"
    duplicate = await api.post("/api/v1/payments", json=payment_body, headers=headers)
    assert first.status_code == duplicate.status_code == 202
    assert first.json()["payment_id"] == duplicate.json()["payment_id"]
    for field, value in [("amount", "126.00"), ("webhook_url", "http://other.test/hook")]:
        changed = await api.post(
            "/api/v1/payments", json={**payment_body, field: value}, headers=headers
        )
        assert changed.status_code == 409


@pytest.mark.parametrize("amount", ["0", "-1", "1.001", "10000000000000000", "NaN", "Infinity"])
async def test_invalid_amount_never_reaches_database(api, sessions, payment_body, amount):
    response = await api.post(
        "/api/v1/payments",
        json={**payment_body, "amount": amount},
        headers={"Idempotency-Key": "invalid"},
    )
    assert response.status_code == 422
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0


async def test_auth_validation_and_missing_payment(api, payment_body):
    assert (await api.get("/openapi.json", headers={"X-API-Key": "wrong"})).status_code == 401
    assert (await api.post("/api/v1/payments", json=payment_body)).status_code == 422
    assert (await api.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await api.get("/api/v1/payments/not-a-uuid")).status_code == 422
    for patch in [{"currency": "GBP"}, {"webhook_url": "file:///secret"}, {"metadata": []}]:
        response = await api.post(
            "/api/v1/payments", json={**payment_body, **patch}, headers={"Idempotency-Key": "bad"}
        )
        assert response.status_code == 422


async def test_outbox_insert_failure_rolls_back_payment(api, sessions, payment_body):
    async with sessions.begin() as session:
        await session.execute(text("ALTER TABLE outbox ADD CONSTRAINT fail_insert CHECK (false)"))
    response = await api.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "rollback"}
    )
    assert response.status_code == 503
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
