from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:55432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:55672/"
    api_key: SecretStr = Field(min_length=16)
