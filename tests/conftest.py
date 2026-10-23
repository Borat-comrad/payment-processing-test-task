import os
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api import create_app
from app.config import Settings
from app.db import Base, Outbox, Payment

API_KEY = "integration-test-key-long-enough"


@pytest.fixture
async def database():
    url = os.getenv(
        "TEST_DATABASE_URL", "postgresql+asyncpg://payments:payments@localhost:55432/payments"
    )
    schema = "test_" + uuid4().hex
    admin = create_async_engine(url)
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield engine, async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest.fixture
def sessions(database):
    return database[1]


@pytest.fixture
async def api(database, monkeypatch):
    monkeypatch.setattr("app.api.open_database", lambda url: database)
    app = create_app(Settings(api_key=API_KEY))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": API_KEY},
    ) as client:
        yield client


@pytest.fixture
def payment_body():
    return {
        "amount": "125.50",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order": 42},
        "webhook_url": "http://receiver.test/payments",
    }


@pytest.fixture
async def pending(sessions):
    async with sessions.begin() as session:
        payment = Payment(
            amount="125.50",
            currency="RUB",
            description="Order 42",
            details={},
            idempotency_key=uuid4().hex,
            request_hash="0" * 64,
            webhook_url="http://receiver.test/payments",
        )
        session.add(payment)
        await session.flush()
        event = Outbox(payment_id=payment.id)
        session.add(event)
        await session.flush()
        return payment.id, event.id
