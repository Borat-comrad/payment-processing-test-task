import asyncio
import logging
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.annotations import RabbitMessage
from faststream.rabbit.schemas import Channel, QueueType
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import Settings
from app.db import Outbox, open_database
from app.processing import InvalidEvent, process_event, record_failure

logger = logging.getLogger(__name__)
EXCHANGE = RabbitExchange("payments", durable=True)
DEAD_QUEUE = RabbitQueue("payments.dlq", durable=True, queue_type=QueueType.QUORUM)
WORK_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    queue_type=QueueType.QUORUM,
    arguments={
        "x-dead-letter-exchange": "payments",
        "x-dead-letter-routing-key": "payments.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)


def make_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=15,
    )


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(EXCHANGE)
    for schema in (DEAD_QUEUE, WORK_QUEUE):
        queue = await broker.declare_queue(schema)
        await queue.bind(exchange, routing_key=schema.name)


async def publish_one(sessions: async_sessionmaker, broker: RabbitBroker) -> bool:
    async with sessions.begin() as session:
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= datetime.now(UTC))
            .order_by(Outbox.available_at, Outbox.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if event is None:
            return False
        await broker.publish(
            {
                "event_id": str(event.id),
                "payment_id": str(event.payment_id),
                "attempt": event.attempt,
                "error": event.error,
            },
            exchange=EXCHANGE,
            routing_key=DEAD_QUEUE.name if event.kind == "dead" else WORK_QUEUE.name,
            persist=True,
            mandatory=True,
            message_id=str(event.id),
            timeout=5,
        )
        event.published_at = datetime.now(UTC)
    return True


async def relay_outbox(sessions: async_sessionmaker, broker: RabbitBroker) -> None:
    while True:
        try:
            published = await publish_one(sessions, broker)
        except Exception as exc:
            logger.error("Outbox publish failed; retained for retry: %s", type(exc).__name__)
            await asyncio.sleep(1)
        else:
            if not published:
                await asyncio.sleep(0.25)


def create_worker(settings: Settings | None = None) -> FastStream:
    settings = settings or Settings()
    engine, sessions = open_database(settings.database_url)
    broker = make_broker(settings.rabbitmq_url)
    client = httpx.AsyncClient(timeout=5, follow_redirects=False, trust_env=False)
    app = FastStream(broker)
    relay: asyncio.Task | None = None

    @broker.subscriber(WORK_QUEUE, EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
    async def consume(body: Any, message: RabbitMessage) -> None:
        try:
            event_id = UUID(body["event_id"])
        except (KeyError, TypeError, ValueError, AttributeError):
            await message.reject()
            return
        delay = 1
        while True:
            try:
                try:
                    await process_event(sessions, event_id, client)
                except (InvalidEvent, SQLAlchemyError, OSError):
                    raise
                except Exception as exc:
                    logger.error("Processing event %s failed: %s", event_id, type(exc).__name__)
                    await record_failure(sessions, event_id, type(exc).__name__)
            except InvalidEvent:
                await message.reject()
                return
            except (SQLAlchemyError, OSError) as exc:
                # Hold the unacked delivery until DB recovery; requeue loops exhaust broker limits.
                logger.error("Database unavailable for event %s: %s", event_id, type(exc).__name__)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10)
            else:
                await message.ack()
                return

    @app.after_startup
    async def start_relay():
        nonlocal relay
        await declare_topology(broker)
        relay = asyncio.create_task(relay_outbox(sessions, broker))

    @app.on_shutdown
    async def stop_relay():
        if relay is not None:
            relay.cancel()
            with suppress(asyncio.CancelledError):
                await relay

    @app.after_shutdown
    async def close_resources():
        await client.aclose()
        await engine.dispose()

    return app


if __name__ == "__main__":
    asyncio.run(create_worker().run())
