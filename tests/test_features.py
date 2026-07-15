"""
Tests for the feature engine.
"""
from datetime import datetime, timezone
from src.features.engine import FeatureEngine, SlidingWindow
from src.producer.schemas import Channel, EnrichedTransaction, RawTransaction, TransactionType


def _make_txn(amount=10000.0, channel=Channel.NIP, account="ACC001"):
    return RawTransaction(
        transaction_id=f"TXN-{amount}",
        timestamp=datetime.now(timezone.utc),
        account_id=account,
        amount=amount,
        channel=channel,
        transaction_type=TransactionType.TRANSFER,
        sender_bank_code="000015",
        recipient_id="RECIPIENT-001",
        device_id="DEVICE-001",
    )


def test_first_transaction_has_zero_windowed_features():
    engine = FeatureEngine()
    txn = _make_txn()
    enriched = engine.compute(txn)
    # First transaction for an account has no prior history
    assert enriched.tx_count_5m == 0
    assert enriched.tx_count_1h == 0
    assert enriched.total_amount_1h == 0.0


def test_second_transaction_sees_first():
    engine = FeatureEngine()
    txn1 = _make_txn(amount=5000.0)
    txn2 = _make_txn(amount=8000.0)
    engine.compute(txn1)
    enriched2 = engine.compute(txn2)
    # Second transaction should see first in windowed counts
    assert enriched2.tx_count_1h == 1
    assert enriched2.total_amount_1h == 5000.0


def test_is_new_recipient_true_on_first_transfer():
    engine = FeatureEngine()
    txn = _make_txn()
    enriched = engine.compute(txn)
    assert enriched.is_new_recipient is True


def test_is_new_recipient_false_on_repeat():
    engine = FeatureEngine()
    txn1 = _make_txn()
    txn2 = _make_txn()
    engine.compute(txn1)
    enriched2 = engine.compute(txn2)
    assert enriched2.is_new_recipient is False


def test_different_accounts_isolated():
    engine = FeatureEngine()
    txn_a = _make_txn(account="ACC-A")
    txn_b = _make_txn(account="ACC-B")
    engine.compute(txn_a)
    enriched_b = engine.compute(txn_b)
    # Account B should not see Account A's history
    assert enriched_b.tx_count_1h == 0


def test_returns_enriched_transaction():
    engine = FeatureEngine()
    txn = _make_txn()
    enriched = engine.compute(txn)
    assert isinstance(enriched, EnrichedTransaction)


def test_sliding_window_count():
    import time
    window = SlidingWindow()
    now = time.time()
    window.add(now - 60, 1000)   # 1 minute ago
    window.add(now - 200, 2000)  # 3 minutes ago
    window.add(now - 400, 3000)  # 6 minutes ago (outside 5min window)
    assert window.count_within(now, 300) == 2  # only last 5min
    assert window.count_within(now, 3600) == 3  # all in last hour