import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from aio_pika.exceptions import DeliveryError
from faststream.rabbit import RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import QueueType
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from app import worker
from app.config import Settings
from app.db import Outbox, Payment
from app.processing import process_event


@pytest.fixture
async def broker(monkeypatch):
    prefix = "test." + uuid4().hex
    exchange = RabbitExchange(prefix, durable=True)
    dead = RabbitQueue(prefix + ".dlq", durable=True, queue_type=QueueType.QUORUM)
    work = RabbitQueue(
        prefix + ".new",
        durable=True,
        queue_type=QueueType.QUORUM,
        arguments={
            **worker.WORK_QUEUE.arguments,
            "x-dead-letter-exchange": prefix,
            "x-dead-letter-routing-key": dead.name,
        },
    )
    monkeypatch.setattr(worker, "EXCHANGE", exchange)
    monkeypatch.setattr(worker, "WORK_QUEUE", work)
    monkeypatch.setattr(worker, "DEAD_QUEUE", dead)
    connection = worker.make_broker(
        os.getenv("TEST_RABBITMQ_URL", "amqp://payments:payments@localhost:55672/")
    )
    await connection.connect()
    await worker.declare_topology(connection)
    try:
        yield connection
    finally:
        for schema in (work, dead):
            queue = await connection.declare_queue(schema)
            await queue.delete(if_unused=False, if_empty=False)
        declared = await connection.declare_exchange(exchange)
        await declared.delete(if_unused=False)
        await connection.stop()


async def receive(queue):
    async with asyncio.timeout(12):
        while True:
            message = await queue.get(fail=False)
            if message is not None:
                await message.ack()
                return json.loads(message.body)
            await asyncio.sleep(0.05)


async def test_confirmed_publish_and_unroutable_recovery(broker, sessions, pending):
    queue = await broker.declare_queue(worker.WORK_QUEUE)
    exchange = await broker.declare_exchange(worker.EXCHANGE)
    await queue.unbind(exchange, routing_key=worker.WORK_QUEUE.name)
    with pytest.raises(DeliveryError):
        await worker.publish_one(sessions, broker)
    async with sessions() as session:
        assert (await session.get(Outbox, pending[1])).published_at is None
    await queue.bind(exchange, routing_key=worker.WORK_QUEUE.name)
    assert await worker.publish_one(sessions, broker)
    received = await receive(queue)
    assert received["event_id"] == str(pending[1])
    assert not await worker.publish_one(sessions, broker)


async def test_delay_is_respected_and_published_again_after_lost_commit(broker, sessions, pending):
    async with sessions.begin() as session:
        event = await session.get(Outbox, pending[1])
        event.available_at = datetime.now(UTC) + timedelta(hours=1)
    assert not await worker.publish_one(sessions, broker)
    async with sessions.begin() as session:
        event = await session.get(Outbox, pending[1])
        event.available_at = datetime.now(UTC)
    assert await worker.publish_one(sessions, broker)
    queue = await broker.declare_queue(worker.WORK_QUEUE)
    first = await receive(queue)
    # Recreate the durable state after a confirm was received but the DB commit was lost.
    async with sessions.begin() as session:
        (await session.get(Outbox, pending[1])).published_at = None
    assert await worker.publish_one(sessions, broker)
    assert (await receive(queue))["event_id"] == first["event_id"]


async def test_real_consumer_retries_then_routes_to_dlq(
    broker, database, sessions, pending, monkeypatch
):
    gateway = AsyncMock(return_value=True)
    requests = []

    def receiver(request):
        requests.append(request)
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:

        async def process(sessions, event_id, unused_client):
            await process_event(sessions, event_id, client, gateway)

        monkeypatch.setattr(worker, "process_event", process)
        monkeypatch.setattr(worker, "open_database", lambda url: database)
        app = worker.create_worker(
            Settings(
                api_key="integration-test-key-long-enough",
                rabbitmq_url=os.getenv(
                    "TEST_RABBITMQ_URL", "amqp://payments:payments@localhost:55672/"
                ),
            )
        )
        await app.start()
        try:
            queue = await broker.declare_queue(worker.DEAD_QUEUE)
            message = await receive(queue)
            assert message["payment_id"] == str(pending[0])
            assert message["attempt"] == 3
            assert message["error"] == "Webhook HTTP 503"
            assert len(requests) == 3
            assert gateway.await_count == 1
            async with sessions() as session:
                assert (await session.get(Payment, pending[0])).status == "succeeded"
                attempts = list(
                    await session.scalars(
                        select(Outbox).where(Outbox.kind == "process").order_by(Outbox.attempt)
                    )
                )
                assert [event.attempt for event in attempts] == [1, 2, 3]
                assert all(event.handled_at for event in attempts)
        finally:
            await app.stop()


async def test_poison_message_is_dead_lettered(broker, database, monkeypatch):
    monkeypatch.setattr(worker, "open_database", lambda url: database)
    app = worker.create_worker(
        Settings(
            api_key="integration-test-key-long-enough",
            rabbitmq_url=os.getenv(
                "TEST_RABBITMQ_URL", "amqp://payments:payments@localhost:55672/"
            ),
        )
    )
    await app.start()
    try:
        await broker.publish(
            {"event_id": "invalid"},
            exchange=worker.EXCHANGE,
            routing_key=worker.WORK_QUEUE.name,
            persist=True,
        )
        dead = await broker.declare_queue(worker.DEAD_QUEUE)
        assert (await receive(dead))["event_id"] == "invalid"
    finally:
        await app.stop()


async def test_database_outage_keeps_delivery_without_spending_attempts(
    broker, database, sessions, pending, monkeypatch
):
    recovered = asyncio.Event()
    failures = 0
    gateway = AsyncMock(return_value=True)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(204))
    ) as client:

        async def process(sessions, event_id, unused_client):
            nonlocal failures
            if failures < 2:
                failures += 1
                raise OperationalError("database unavailable", {}, Exception("connection lost"))
            await process_event(sessions, event_id, client, gateway)
            recovered.set()

        monkeypatch.setattr(worker, "process_event", process)
        monkeypatch.setattr(worker, "open_database", lambda url: database)
        app = worker.create_worker(
            Settings(
                api_key="integration-test-key-long-enough",
                rabbitmq_url=os.getenv(
                    "TEST_RABBITMQ_URL", "amqp://payments:payments@localhost:55672/"
                ),
            )
        )
        await app.start()
        try:
            async with asyncio.timeout(10):
                await recovered.wait()
            async with sessions() as session:
                events = list(await session.scalars(select(Outbox)))
                assert len(events) == 1
                assert events[0].attempt == 1
                assert events[0].handled_at is not None
                assert (await session.get(Payment, pending[0])).webhook_delivered_at is not None
        finally:
            await app.stop()
