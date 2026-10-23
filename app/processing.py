import asyncio
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db import Outbox, Payment

Gateway = Callable[[], Awaitable[bool]]


class InvalidEvent(Exception):
    pass


async def simulate_payment() -> bool:
    await asyncio.sleep(random.uniform(2, 5))
    return random.random() < 0.9


def schedule_failure(session: AsyncSession, event: Outbox, reason: str) -> None:
    now = datetime.now(UTC)
    event.handled_at = now
    event.error = reason[:200]
    last_attempt = event.attempt == 3
    session.add(
        Outbox(
            payment_id=event.payment_id,
            kind="dead" if last_attempt else "process",
            attempt=event.attempt if last_attempt else event.attempt + 1,
            available_at=now if last_attempt else now + timedelta(seconds=2 ** (event.attempt - 1)),
            error=reason[:200],
        )
    )


async def lock_event(session: AsyncSession, event_id: UUID) -> Outbox:
    event = await session.scalar(select(Outbox).where(Outbox.id == event_id).with_for_update())
    if event is None or event.kind != "process":
        raise InvalidEvent("Unknown processing event")
    return event


async def record_failure(sessions: async_sessionmaker, event_id: UUID, reason: str) -> None:
    async with sessions.begin() as session:
        event = await lock_event(session, event_id)
        if event.handled_at is None:
            schedule_failure(session, event, reason)


async def process_event(
    sessions: async_sessionmaker,
    event_id: UUID,
    client: httpx.AsyncClient,
    gateway: Gateway = simulate_payment,
) -> None:
    async with sessions.begin() as session:
        event = await lock_event(session, event_id)
        if event.handled_at is not None:
            return
        payment = await session.get(Payment, event.payment_id, with_for_update=True)
        if payment.status == "pending":
            payment.status = "succeeded" if await gateway() else "failed"
            payment.processed_at = datetime.now(UTC)

    # Commit the payment result before HTTP: delivery failure must not repeat the charge.
    async with sessions.begin() as session:
        event = await lock_event(session, event_id)
        if event.handled_at is not None:
            return
        payment = await session.get(Payment, event.payment_id, with_for_update=True)
        if payment.webhook_delivered_at is not None:
            event.handled_at = datetime.now(UTC)
            return
        payload = {
            "payment_id": str(payment.id),
            "amount": str(payment.amount),
            "currency": payment.currency,
            "status": payment.status,
            "processed_at": payment.processed_at.isoformat(),
        }
        try:
            # Bound the whole request, including a peer that keeps trickling response bytes.
            async with asyncio.timeout(5):
                async with client.stream("POST", payment.webhook_url, json=payload) as response:
                    response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            schedule_failure(session, event, f"Webhook HTTP {exc.response.status_code}")
        except (httpx.HTTPError, TimeoutError) as exc:
            schedule_failure(session, event, f"Webhook {type(exc).__name__}")
        else:
            payment.webhook_delivered_at = datetime.now(UTC)
            event.handled_at = payment.webhook_delivered_at
