import hashlib
import json
import logging
import secrets
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.db import Outbox, Payment, open_database
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    engine, sessions = open_database(settings.database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await engine.dispose()

    app = FastAPI(title="Payment processing", lifespan=lifespan)
    app.state.engine = engine

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        provided = request.headers.get("X-API-Key", "").encode()
        expected = settings.api_key.get_secret_value().encode()
        if not secrets.compare_digest(provided, expected):
            return JSONResponse({"detail": "Invalid API key"}, status_code=401)
        return await call_next(request)

    @app.exception_handler(SQLAlchemyError)
    async def database_unavailable(request: Request, exc: SQLAlchemyError):
        logger.error("Database request failed: %s", type(exc).__name__)
        return JSONResponse({"detail": "Database temporarily unavailable"}, status_code=503)

    @app.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
    async def create_payment(
        body: PaymentCreate,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=200, pattern=r"^[!-~]+$")],
    ):
        canonical = json.dumps(body.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        request_hash = hashlib.sha256(canonical.encode()).hexdigest()
        async with sessions.begin() as session:
            payment = await session.scalar(
                insert(Payment)
                .values(
                    amount=body.amount,
                    currency=body.currency,
                    description=body.description,
                    details=body.metadata,
                    webhook_url=str(body.webhook_url),
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                )
                .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
                .returning(Payment)
            )
            if payment is None:
                payment = await session.scalar(
                    select(Payment).where(Payment.idempotency_key == idempotency_key)
                )
                if payment.request_hash != request_hash:
                    raise HTTPException(409, "Idempotency key already used with another request")
            else:
                session.add(Outbox(payment_id=payment.id))
            response = PaymentAccepted.model_validate(payment)
        return response

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetail)
    async def get_payment(payment_id: UUID):
        async with sessions() as session:
            payment = await session.get(Payment, payment_id)
            if payment is None:
                raise HTTPException(404, "Payment not found")
            return PaymentDetail.model_validate(payment)

    return app
