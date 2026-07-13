"""
Real-time feature engine.

Consumes raw transactions from Kafka, computes windowed aggregations
per account in memory, and publishes enriched transactions to the
enriched-transactions topic.

Why in-memory windowing (not a database):
    Sub-millisecond feature lookup is required to stay within the
    100ms end-to-end scoring budget. A database query adds 5-50ms
    per transaction. For our scale (hundreds of TPS), an in-memory
    sliding window per account is fast enough and fits comfortably
    in RAM (500 accounts x 24h of events).

    At Paystack scale (millions of TPS), this would move to Redis
    with sorted sets for windowed aggregations — same concept,
    distributed implementation.
"""
import json
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Consumer, KafkaError, Producer

from config.settings import (
    KAFKA_BOOTSTRAP,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC_ENRICHED_TRANSACTION,
    KAFKA_TOPIC_RAW,
    WINDOW_1H_SECONDS,
    WINDOW_5M_SECONDS,
    WINDOW_24H_SECONDS,
)
from src.producer.schemas import EnrichedTransaction, RawTransaction


class SlidingWindow:
    """
    In-memory sliding window for a single account.

    Stores (timestamp, amount) pairs in a deque. On each access,
    expired entries (outside the window duration) are purged.
    This keeps memory bounded — only events within the largest
    window (24h) are retained per account.
    """

    def __init__(self):
        self.events: deque = deque()

    def add(self, timestamp: float, amount: float) -> None:
        self.events.append((timestamp, amount))

    def _purge_expired(self, now: float, max_window: int) -> None:
        cutoff = now - max_window
        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

    def count_within(self, now: float, window_seconds: int) -> int:
        cutoff = now - window_seconds
        return sum(1 for ts, _ in self.events if ts >= cutoff)

    def sum_within(self, now: float, window_seconds: int) -> float:
        cutoff = now - window_seconds
        return sum(amt for ts, amt in self.events if ts >= cutoff)

    def unique_recipients_within(
        self, now: float, window_seconds: int, recipients: deque
    ) -> int:
        cutoff = now - window_seconds
        seen = set()
        for ts, recipient in recipients:
            if ts >= cutoff:
                seen.add(recipient)
        return len(seen)


class AccountState:
    """
    Per-account state tracked by the feature engine.

    Holds the sliding window of transaction events plus supporting
    data structures for recipient tracking and 30-day average.
    """

    def __init__(self):
        self.window = SlidingWindow()
        self.recipients: deque = deque()
        self.all_recipients: set = set()
        self.all_devices: set = set()
        self.avg_amount_30d: float = 0.0
        self.tx_count_total: int = 0

    def update_avg(self, amount: float) -> None:
        """Exponential moving average — weighted toward recent transactions."""
        self.tx_count_total += 1
        alpha = 0.1  # smoothing factor: 0.1 = slow adaptation, 0.3 = fast
        if self.tx_count_total == 1:
            self.avg_amount_30d = amount
        else:
            self.avg_amount_30d = (
                alpha * amount + (1 - alpha) * self.avg_amount_30d
            )


class FeatureEngine:
    """
    Stateful feature computation engine.

    Maintains per-account state in a dictionary. In production this
    state would be checkpointed to Redis periodically so it survives
    restarts without losing windowed data.
    """

    def __init__(self):
        # account_id → AccountState
        self._state: dict[str, AccountState] = defaultdict(AccountState)

    def compute(self, txn: RawTransaction) -> EnrichedTransaction:
        """
        Compute features for a transaction and return an EnrichedTransaction.

        This is the hot path — called once per transaction, must be fast.
        All operations are O(1) or O(window_size) with small constants.
        """
        now = txn.timestamp.timestamp()
        state = self._state[txn.account_id]

        #Windowed counts and sums
        tx_count_5m = state.window.count_within(now, WINDOW_5M_SECONDS)
        tx_count_1h = state.window.count_within(now, WINDOW_1H_SECONDS)
        tx_count_24h = state.window.count_within(now, WINDOW_24H_SECONDS)

        total_amount_5m = state.window.sum_within(now, WINDOW_5M_SECONDS)
        total_amount_1h = state.window.sum_within(now, WINDOW_1H_SECONDS)
        total_amount_24h = state.window.sum_within(now, WINDOW_24H_SECONDS)

        #Unique recipients in last hour
        unique_recipients_1h = state.window.unique_recipients_within(
            now, WINDOW_1H_SECONDS, state.recipients
        )

        #Behavioural flags
        is_new_recipient = (
            txn.recipient_id is not None
            and txn.recipient_id not in state.all_recipients
        )
        is_new_device = (
            txn.device_id is not None
            and txn.device_id not in state.all_devices
        )

        #Calendar features
        dt = txn.timestamp
        is_salary_period = dt.day >= 25 or dt.day <= 2
        is_weekend = dt.weekday() >= 5

        #Update state AFTER computing features
        # (features reflect state BEFORE this transaction, not including it)
        state.window.add(now, txn.amount)
        state.window._purge_expired(now, WINDOW_24H_SECONDS)

        if txn.recipient_id:
            state.recipients.append((now, txn.recipient_id))
            state.all_recipients.add(txn.recipient_id)
            # purge expired recipients
            cutoff = now - WINDOW_1H_SECONDS
            while state.recipients and state.recipients[0][0] < cutoff:
                state.recipients.popleft()

        if txn.device_id:
            state.all_devices.add(txn.device_id)

        state.update_avg(txn.amount)

        return EnrichedTransaction(
            **txn.model_dump(),
            tx_count_5m=tx_count_5m,
            tx_count_1h=tx_count_1h,
            tx_count_24h=tx_count_24h,
            total_amount_5m=total_amount_5m,
            total_amount_1h=total_amount_1h,
            total_amount_24h=total_amount_24h,
            avg_amount_30d=state.avg_amount_30d,
            unique_recipients_1h=unique_recipients_1h,
            is_new_recipient=is_new_recipient,
            is_new_device=is_new_device,
            hour_of_day=dt.hour,
            day_of_week=dt.weekday(),
            is_salary_period=is_salary_period,
            is_weekend=is_weekend,
        )


def run(poll_timeout: float = 1.0) -> None:
    """
    Main consumer loop.

    Reads raw transactions from Kafka, enriches them with computed
    features, and publishes to the enriched-transactions topic.
    """
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"{KAFKA_CONSUMER_GROUP}-feature-engine",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    engine = FeatureEngine()

    consumer.subscribe([KAFKA_TOPIC_RAW])
    print(f"Feature engine started — consuming from '{KAFKA_TOPIC_RAW}'")
    print(f"Publishing enriched transactions to '{KAFKA_TOPIC_ENRICHED_TRANSACTION}'")

    processed = 0
    errors = 0
    start = time.time()

    try:
        while True:
            msg = consumer.poll(poll_timeout)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}")
                errors += 1
                continue

            try:
                raw_data = json.loads(msg.value().decode())
                txn = RawTransaction(**raw_data)
                enriched = engine.compute(txn)

                producer.produce(
                    KAFKA_TOPIC_ENRICHED_TRANSACTION,
                    key=enriched.account_id.encode(),
                    value=enriched.model_dump_json().encode(),
                )

                processed += 1

                if processed % 100 == 0:
                    elapsed = time.time() - start
                    print(
                        f"  Enriched {processed} txns | "
                        f"{processed/elapsed:.1f} TPS | "
                        f"accounts tracked: {len(engine._state)} | "
                        f"errors: {errors}"
                    )
                    producer.flush()

            except Exception as exc:
                errors += 1
                print(f"Error processing message: {exc}")

    except KeyboardInterrupt:
        print(f"\nStopped. Processed {processed} transactions, {errors} errors.")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run()