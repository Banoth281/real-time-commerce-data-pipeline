import json
import logging
import signal
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer, Producer
from pydantic import ValidationError

from src.common.config import settings
from src.common.models import OrderEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stream-processor")
running = True

INSERT_ORDER = """
INSERT INTO orders (
    event_id, order_id, customer_id, product_id, category, quantity,
    unit_price, total_amount, country, event_time
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
"""
INSERT_METRIC = """
INSERT INTO pipeline_metrics (
    event_id, processing_latency_ms, consumer_partition, consumer_offset
) VALUES (%s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
"""


def stop(*_: object) -> None:
    global running
    running = False


def persist(conn: psycopg.Connection, event: OrderEvent, partition: int, offset: int) -> None:
    latency = max(0, int((datetime.now(timezone.utc) - event.event_time).total_seconds() * 1000))
    with conn.transaction():
        conn.execute(
            INSERT_ORDER,
            (
                event.event_id, event.order_id, event.customer_id, event.product_id,
                event.category, event.quantity, event.unit_price, event.total_amount,
                event.country, event.event_time,
            ),
        )
        conn.execute(INSERT_METRIC, (event.event_id, latency, partition, offset))


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "commerce-order-processor-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    dlq = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    consumer.subscribe([settings.kafka_topic])
    with psycopg.connect(settings.postgres_dsn, autocommit=True) as conn:
        log.info("Consuming %s", settings.kafka_topic)
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                log.error("Kafka error: %s", message.error())
                continue
            try:
                event = OrderEvent.model_validate_json(message.value())
                persist(conn, event, message.partition(), message.offset())
                consumer.commit(message=message, asynchronous=False)
            except (ValidationError, json.JSONDecodeError, UnicodeDecodeError) as error:
                log.warning("Invalid event sent to DLQ: %s", error)
                dlq.produce(settings.kafka_dlq_topic, key=message.key(), value=message.value())
                dlq.flush(5)
                consumer.commit(message=message, asynchronous=False)
            except Exception:
                log.exception("Processing failed; offset will be retried")
    consumer.close()


if __name__ == "__main__":
    main()

