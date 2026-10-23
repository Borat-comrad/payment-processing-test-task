import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from app.db import Outbox, Payment
from app.processing import process_event, record_failure


@pytest.mark.parametrize("success", [True, False])
async def test_payment_outcome_and_duplicate_delivery(sessions, pending, success):
    received = []

    def receiver(request):
        received.append(json.loads(request.content))
        return httpx.Response(204)

    gateway = AsyncMock(return_value=success)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        await asyncio.gather(
            process_event(sessions, pending[1], client, gateway),
            process_event(sessions, pending[1], client, gateway),
        )
    assert gateway.await_count == 1
    assert len(received) == 1
    assert received[0]["status"] == ("succeeded" if success else "failed")
    assert received[0]["amount"] == "125.50"
    async with sessions() as session:
        payment = await session.get(Payment, pending[0])
        assert payment.webhook_delivered_at is not None
        assert (await session.get(Outbox, pending[1])).handled_at is not None


async def test_three_webhook_failures_schedule_backoff_and_dead_letter(sessions, pending):
    gateway = AsyncMock(return_value=True)
    calls = []

    def receiver(request):
        calls.append(request)
        return httpx.Response(503)

    event_id = pending[1]
    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        for attempt in (1, 2, 3):
            before = datetime.now(UTC)
            await process_event(sessions, event_id, client, gateway)
            await process_event(sessions, event_id, client, gateway)
            async with sessions() as session:
                current = await session.get(Outbox, event_id)
                assert current.handled_at is not None
                following = await session.scalar(select(Outbox).where(Outbox.handled_at.is_(None)))
                if attempt < 3:
                    delay = 2 ** (attempt - 1)
                    assert following.kind == "process"
                    assert following.attempt == attempt + 1
                    assert delay <= (following.available_at - before).total_seconds() < delay + 2
                    event_id = following.id
                else:
                    assert following.kind == "dead"
                    assert following.attempt == 3
                    assert following.error == "Webhook HTTP 503"
    assert len(calls) == 3
    assert gateway.await_count == 1
    async with sessions() as session:
        payment = await session.get(Payment, pending[0])
        assert payment.status == "succeeded"
        assert payment.webhook_delivered_at is None


async def test_retry_after_timeout_delivers_without_reprocessing(sessions, pending):
    gateway = AsyncMock(return_value=False)
    calls = 0

    def receiver(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadTimeout("lost response")
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        await process_event(sessions, pending[1], client, gateway)
        async with sessions() as session:
            retry = await session.scalar(select(Outbox).where(Outbox.attempt == 2))
        await process_event(sessions, retry.id, client, gateway)
    assert calls == 2
    assert gateway.await_count == 1
    async with sessions() as session:
        payment = await session.get(Payment, pending[0])
        assert payment.status == "failed"
        assert payment.webhook_delivered_at is not None


async def test_crash_after_payment_commit_resumes_webhook(sessions, pending):
    gateway = AsyncMock(return_value=True)

    def interrupted(request):
        raise asyncio.CancelledError()

    async with httpx.AsyncClient(transport=httpx.MockTransport(interrupted)) as client:
        with pytest.raises(asyncio.CancelledError):
            await process_event(sessions, pending[1], client, gateway)
    async with sessions() as session:
        assert (await session.get(Payment, pending[0])).status == "succeeded"
        assert (await session.get(Outbox, pending[1])).handled_at is None
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(204))
    ) as client:
        await process_event(sessions, pending[1], client, gateway)
    assert gateway.await_count == 1


async def test_record_failure_is_atomic_and_duplicate_safe(sessions, pending):
    await asyncio.gather(
        record_failure(sessions, pending[1], "GatewayUnavailable"),
        record_failure(sessions, pending[1], "GatewayUnavailable"),
    )
    async with sessions() as session:
        events = list(await session.scalars(select(Outbox).order_by(Outbox.attempt)))
        assert [event.attempt for event in events] == [1, 2]
        assert events[0].handled_at is not None
        assert events[1].handled_at is None


async def test_failed_retry_insert_keeps_original_event_unhandled(sessions, pending):
    async with sessions.begin() as session:
        await session.execute(
            text("ALTER TABLE outbox ADD CONSTRAINT no_retry CHECK (attempt = 1)")
        )
    with pytest.raises(IntegrityError):
        await record_failure(sessions, pending[1], "Webhook timeout")
    async with sessions() as session:
        events = list(await session.scalars(select(Outbox)))
        assert len(events) == 1
        assert events[0].handled_at is None
