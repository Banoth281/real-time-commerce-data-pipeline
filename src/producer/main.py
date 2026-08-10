import json
import logging
import random
import signal
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer

from src.common.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("order-producer")
running = True

CATALOG = [
    ("laptop", "Electronics", 899.99),
    ("headphones", "Electronics", 129.99),
    ("running-shoes", "Sports", 74.50),
    ("coffee-maker", "Home", 59.00),
    ("data-engineering-book", "Books", 39.99),
]
COUNTRIES = ["GB", "IN", "US", "DE", "FR"]


def make_event() -> dict:
    product_id, category, unit_price = random.choice(CATALOG)
    return {
        "event_id": str(uuid4()),
        "order_id": str(uuid4()),
        "customer_id": str(uuid4()),
        "product_id": product_id,
        "category": category,
        "quantity": random.randint(1, 5),
        "unit_price": unit_price,
        "country": random.choice(COUNTRIES),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def stop(*_: object) -> None:
    global running
    running = False


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    delay = 1 / max(settings.events_per_second, 0.1)
    log.info("Publishing to %s", settings.kafka_topic)
    while running:
        event = make_event()
        producer.produce(
            settings.kafka_topic,
            key=event["order_id"],
            value=json.dumps(event),
            on_delivery=lambda error, message: log.error("Delivery failed: %s", error) if error else None,
        )
        producer.poll(0)
        time.sleep(delay)
    producer.flush(10)


if __name__ == "__main__":
    main()

