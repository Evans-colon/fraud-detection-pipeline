from datetime import datetime, timezone


from src.producer.schemas import Channel, TransactionType, RawTransaction, EnrichedTransaction, Decision


def test_raw_transaction_valid():
    tnx = RawTransaction(
        transaction_id = "TEST-001",
        timestamp = datetime.now(timezone.utc),
        account_id = "abc123",
        amount = 50000.0,
        channel = Channel.NIP,
        transaction_type = TransactionType.PAYMENT,
        sender_bank_code = "00015",

)
    assert tnx.transaction_id == "TEST-001"
    assert tnx.amount == 50000.0
    assert tnx.channel == Channel.NIP
    assert tnx.transaction_type == TransactionType.PAYMENT


def test_raw_transaction_invalid_amount():
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        RawTransaction(
            transaction_id = "TEST-001",
            timestamp = datetime.now(timezone.utc),
            account_id = "abc123",
            amount = -50000.0,
            channel = "WIRE"
)


def test_enriched_transaction_inherits_raw():
    enriched = EnrichedTransaction(
        transaction_id="TEST-004",
        timestamp=datetime.now(timezone.utc),
        account_id="abc123",
        amount=5000.0,
        channel=Channel.CARD_POS,
        transaction_type=TransactionType.PAYMENT,
        sender_bank_code="000013",
        tx_count_5m=3,
        avg_amount_30d=4500.0,
    )
    assert enriched.tx_count_5m == 3
    assert enriched.avg_amount_30d == 4500.0
    assert enriched.channel == Channel.CARD_POS


def test_decision_enum():
    assert Decision.ALLOW == "ALLOW"
    assert Decision.FLAG == "FLAG"
    assert Decision.BLOCK == "BLOCK"