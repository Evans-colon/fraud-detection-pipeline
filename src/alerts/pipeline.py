"""
Alert pipeline — consumes fraud alerts from Kafka and persists
them to the SQLite alert store.

Separated from the scoring service deliberately:
    The scorer's job is to make fast decisions and publish to Kafka.
    Storage is a separate concern — if the database is slow or down,
    it should not affect scoring latency. Kafka acts as the buffer
    between the two.

Run: python -m src.alerts.pipeline
"""
import json
import time

from confluent_kafka import Consumer, KafkaError

from config.settings import (
    ALERT_DB_PATH,
    KAFKA_BOOTSTRAP,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC_ALERTS,
)
from src.alerts.store import init_db, save_alert
from src.producer.schemas import FraudAlert


def run() -> None:
    """Main alert consumer loop."""
    init_db(ALERT_DB_PATH)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"{KAFKA_CONSUMER_GROUP}-alerts",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC_ALERTS])

    print(f"Alert pipeline started")
    print(f"  Consuming from: {KAFKA_TOPIC_ALERTS}")
    print(f"  Storing to: {ALERT_DB_PATH}")

    saved = 0
    errors = 0
    start = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                errors += 1
                continue

            try:
                data = json.loads(msg.value().decode())
                alert = FraudAlert(**data)
                if save_alert(alert):
                    saved += 1

                if saved % 50 == 0 and saved > 0:
                    elapsed = time.time() - start
                    print(
                        f"  Saved {saved} alerts | "
                        f"errors: {errors} | "
                        f"{saved/elapsed:.1f} alerts/s"
                    )

            except Exception as exc:
                errors += 1
                print(f"Error processing alert: {exc}")

    except KeyboardInterrupt:
        elapsed = time.time() - start
        print(f"\nStopped. Saved {saved} alerts in {elapsed:.1f}s")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()