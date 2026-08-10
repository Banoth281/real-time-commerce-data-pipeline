import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "commerce.orders.v1")
    kafka_dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "commerce.orders.dlq.v1")
    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN", "postgresql://commerce:commerce@localhost:5432/commerce"
    )
    events_per_second: float = float(os.getenv("EVENTS_PER_SECOND", "2"))


settings = Settings()

