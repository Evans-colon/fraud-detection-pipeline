"""
Tests for the transaction generator.
"""
from datetime import datetime, timezone
from src.producer.generator import (
    generate_legitimate_transaction,
    generate_fraud_transaction,
    generate_transaction,
)
from src.producer.schemas import Channel, RawTransaction


def test_legitimate_transaction_returns_raw_transaction():
    txn = generate_legitimate_transaction()
    assert isinstance(txn, RawTransaction)
    assert txn.amount > 0
    assert txn.transaction_id is not None
    assert txn.sender_bank_code is not None


def test_legitimate_transaction_amount_below_cbn_threshold():
    # Legitimate transactions should stay below ₦5M
    for _ in range(50):
        txn = generate_legitimate_transaction()
        assert txn.amount < 5_000_000, (
            f"Legitimate transaction ₦{txn.amount:,.2f} exceeds CBN threshold"
        )


def test_legitimate_transaction_ussd_has_no_device():
    # USSD transactions must not have device fingerprints
    ussd_txns = []
    attempts = 0
    while len(ussd_txns) < 5 and attempts < 200:
        txn = generate_legitimate_transaction()
        if txn.channel.value == "USSD":
            ussd_txns.append(txn)
        attempts += 1
    for txn in ussd_txns:
        assert txn.device_id is None, "USSD transaction should not have device_id"


def test_fraud_transaction_returns_raw_transaction():
    txn = generate_fraud_transaction()
    assert isinstance(txn, RawTransaction)
    assert txn.amount > 0


def test_generate_transaction_returns_tuple():
    txn, is_fraud = generate_transaction()
    assert isinstance(txn, RawTransaction)
    assert isinstance(is_fraud, bool)


def test_fraud_rate_approximately_correct():
    total = 500
    fraud_count = sum(1 for _ in range(total) if generate_transaction()[1])
    fraud_rate = fraud_count / total
    # Should be within 3x of the configured 2% rate
    assert 0.001 <= fraud_rate <= 0.06, (
        f"Fraud rate {fraud_rate:.3f} outside expected range"
    )