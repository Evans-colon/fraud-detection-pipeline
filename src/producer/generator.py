"""
Nigerian payment transaction generator.

Simulates realistic transaction patterns including both legitimate
behaviour (salary cycles, market-day POS spikes, airtime top-ups)
and fraud patterns (velocity attacks, card cloning, channel switching).

Run standalone: python -m src.producer.generator
"""
import hashlib
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional



from config.settings import (
    BANK_CODES,
    FRAUD_RATE,
    GENERATOR_TPS,
    MCC_CODES, KAFKA_BOOTSTRAP, KAFKA_TOPIC_RAW,
)
from src.producer.schemas import Channel, RawTransaction, TransactionType

# Nigerian states and cities
LOCATIONS = {
    "Lagos": ["Lagos Island", "Ikeja", "Lekki", "Victoria Island", "Surulere"],
    "Abuja": ["Garki", "Wuse", "Maitama", "Asokoro", "Gwarinpa"],
    "Rivers": ["Port Harcourt", "Obio-Akpor"],
    "Kano": ["Kano Municipal", "Nassarawa"],
    "Oyo": ["Ibadan North", "Ibadan South-West"],
    "Anambra": ["Onitsha", "Awka", "Nnewi"],
    "Delta": ["Warri", "Asaba"],
    "Enugu": ["Enugu North", "Enugu South"],
}

# Stable simulated accounts (reused across transactions for realistic patterns)
NUM_ACCOUNTS = 500
ACCOUNTS = [
    hashlib.sha256(f"ACCT-{i:05d}".encode()).hexdigest()[:16]
    for i in range(NUM_ACCOUNTS)
]
DEVICES = [
    hashlib.sha256(f"DEV-{i:04d}".encode()).hexdigest()[:12]
    for i in range(200)
]


def _pick_location():
    state = random.choice(list(LOCATIONS.keys()))
    city = random.choice(LOCATIONS[state])
    return city, state


def _amount_for_channel(channel: Channel) -> float:
    """Realistic Naira amounts per channel based on Nigerian market data."""
    if channel == Channel.CARD_POS:
        return round(random.lognormvariate(8.5, 1.2), 2)   # median ~₦5,000
    if channel == Channel.USSD:
        return round(random.lognormvariate(8.0, 1.0), 2)   # median ~₦3,000
    if channel == Channel.NIP:
        return round(random.lognormvariate(10.5, 1.8), 2)  # median ~₦36,000
    if channel == Channel.CARD_WEB:
        return round(random.lognormvariate(9.5, 1.5), 2)   # median ~₦13,000
    return round(random.lognormvariate(9.0, 1.5), 2)


def _is_salary_period(dt: datetime) -> bool:
    return dt.day >= 25 or dt.day <= 2


def generate_legitimate_transaction(
    timestamp: Optional[datetime] = None,
) -> RawTransaction:
    ts = timestamp or datetime.now(timezone.utc)
    account = random.choice(ACCOUNTS)

    # NIP-heavy channel mix, reflects Nigerian market reality
    channel = random.choices(
        [Channel.NIP, Channel.CARD_POS, Channel.CARD_WEB,
         Channel.USSD, Channel.MOBILE_APP],
        weights=[35, 25, 10, 20, 10],
    )[0]

    # Salary period boosts NIP transfer amounts
    if _is_salary_period(ts) and channel == Channel.NIP and random.random() < 0.3:
        tx_type = TransactionType.TRANSFER
        amount = round(random.uniform(80_000, 2_000_000), 2)
    elif random.random() < 0.15:
        tx_type = TransactionType.AIRTIME
        amount = random.choice([100, 200, 500, 1000, 2000, 5000])
    elif channel in (Channel.CARD_POS, Channel.CARD_WEB):
        tx_type = TransactionType.PAYMENT
        amount = _amount_for_channel(channel)
    else:
        tx_type = TransactionType.TRANSFER
        amount = _amount_for_channel(channel)

    city, state = _pick_location()
    bank_codes = list(BANK_CODES.keys())

    return RawTransaction(
        transaction_id=str(uuid.uuid4()),
        timestamp=ts,
        account_id=account,
        recipient_id=(
            random.choice(ACCOUNTS)
            if tx_type == TransactionType.TRANSFER
            else None
        ),
        amount=min(amount, 4_999_999),  # cap below CBN threshold for legit txns
        channel=channel,
        transaction_type=tx_type,
        merchant_category_code=(
            random.choice(list(MCC_CODES.keys()))
            if tx_type == TransactionType.PAYMENT
            else None
        ),
        sender_bank_code=random.choice(bank_codes),
        recipient_bank_code=(
            random.choice(bank_codes)
            if tx_type == TransactionType.TRANSFER
            else None
        ),
        device_id=(
            random.choice(DEVICES) if channel != Channel.USSD else None
        ),
        ip_hash=(
            hashlib.md5(
                str(random.randint(0, 999999)).encode()
            ).hexdigest()[:10]
            if channel == Channel.CARD_WEB
            else None
        ),
        location_city=city,
        location_state=state,
        is_international=random.random() < 0.02,
    )


def generate_fraud_transaction(
    timestamp: Optional[datetime] = None,
) -> RawTransaction:
    ts = timestamp or datetime.now(timezone.utc)

    fraud_type = random.choices(
        ["velocity", "high_amount", "channel_switch", "odd_hours", "geo_anomaly"],
        weights=[30, 20, 20, 15, 15],
    )[0]

    base = generate_legitimate_transaction(ts)

    if fraud_type == "velocity":
        # Rapid successive transfers — draining an account
        return base.model_copy(update={
            "amount": round(random.uniform(45_000, 200_000), 2),
            "channel": Channel.NIP,
            "transaction_type": TransactionType.TRANSFER,
        })

    elif fraud_type == "high_amount":
        # Single unusually large transaction
        return base.model_copy(update={
            "amount": round(random.uniform(2_000_000, 8_000_000), 2),
            "channel": random.choice([Channel.NIP, Channel.CARD_WEB]),
        })

    elif fraud_type == "channel_switch":
        # Account normally uses USSD, suddenly uses CARD_WEB for high value
        return base.model_copy(update={
            "channel": Channel.CARD_WEB,
            "amount": round(random.uniform(200_000, 1_000_000), 2),
            "is_international": random.random() < 0.4,
        })

    elif fraud_type == "odd_hours":
        # High-value transaction at 1AM-4AM
        odd_ts = ts.replace(
            hour=random.randint(1, 3),
            minute=random.randint(0, 59)
        )
        return base.model_copy(update={
            "timestamp": odd_ts,
            "amount": round(random.uniform(500_000, 3_000_000), 2),
        })

    elif fraud_type == "geo_anomaly":
        # POS in an unexpected state
        return base.model_copy(update={
            "channel": Channel.CARD_POS,
            "location_state": random.choice(["Kano", "Rivers", "Enugu"]),
            "location_city": "Unexpected Location",
        })

    return base


def generate_transaction(
    timestamp: Optional[datetime] = None,
) -> tuple[RawTransaction, bool]:
    """Returns (transaction, is_fraud)."""
    is_fraud = random.random() < FRAUD_RATE
    if is_fraud:
        return generate_fraud_transaction(timestamp), True
    return generate_legitimate_transaction(timestamp), False


def stream_transactions(
    tps: float = GENERATOR_TPS,
    use_kafka: bool = True,
    summary_every: int = 100,
) -> None:
    """
    Continuously generate transactions and publish to Kafka.
    Falls back to stdout if Kafka is unavailable.
    """
    from confluent_kafka import Producer

    producer = None
    if use_kafka:
        try:
            producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
            # Verify connection with a metadata request
            producer.list_topics(timeout=5)
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
        except Exception as e:
            print(f"Kafka unavailable ({e}), falling back to stdout")
            producer = None

    interval = 1.0 / tps
    total = 0
    fraud_count = 0
    start = time.time()

    print(f"Generating at {tps} TPS | fraud rate: {FRAUD_RATE*100:.1f}%")
    print("-" * 65)

    try:
        while True:
            loop_start = time.monotonic()

            txn, is_fraud = generate_transaction()
            total += 1
            if is_fraud:
                fraud_count += 1

            payload = txn.model_dump_json().encode()

            if producer:
                producer.produce(
                    KAFKA_TOPIC_RAW,
                    key=txn.account_id.encode(),
                    value=payload,
                )
                # Flush every 50 messages to avoid buffering too long
                if total % 50 == 0:
                    producer.flush()
            else:
                tag = " [FRAUD]" if is_fraud else ""
                print(
                    f"[{total:>6}] {txn.channel.value:<12}"
                    f"₦{txn.amount:>12,.2f}  "
                    f"{txn.transaction_type.value:<14}{tag}"
                )

            if total % summary_every == 0:
                elapsed = time.time() - start
                print(
                    f"  --- {total} txns | "
                    f"{fraud_count} fraud ({100*fraud_count/total:.1f}%) | "
                    f"{total/elapsed:.1f} TPS actual"
                )

            elapsed = time.monotonic() - loop_start
            time.sleep(max(0.0, interval - elapsed))

    except KeyboardInterrupt:
        elapsed = time.time() - start
        print(f"\nStopped. {total} txns in {elapsed:.1f}s")
        if producer:
            producer.flush()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    stream_transactions(tps=5)