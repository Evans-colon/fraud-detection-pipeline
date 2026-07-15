"""
Tests for the rule engine and scoring service.
"""
from datetime import datetime, timezone
from src.producer.schemas import Channel, Decision, EnrichedTransaction, TransactionType
from src.scoring.scorer import RuleEngine


def _make_enriched(**kwargs):
    defaults = dict(
        transaction_id="TEST-SCORE-001",
        timestamp=datetime.now(timezone.utc),
        account_id="ACC001",
        amount=10000.0,
        channel=Channel.NIP,
        transaction_type=TransactionType.TRANSFER,
        sender_bank_code="000015",
        tx_count_5m=0,
        tx_count_1h=0,
        tx_count_24h=0,
        total_amount_5m=0.0,
        total_amount_1h=0.0,
        total_amount_24h=0.0,
        avg_amount_30d=10000.0,
        unique_recipients_1h=0,
        is_new_recipient=False,
        is_new_device=False,
        hour_of_day=14,
        day_of_week=1,
        is_salary_period=False,
        is_weekend=False,
    )
    defaults.update(kwargs)
    return EnrichedTransaction(**defaults)


def test_normal_transaction_no_rule_triggered():
    engine = RuleEngine()
    txn = _make_enriched()
    decision, reason = engine.evaluate(txn)
    assert decision is None
    assert reason is None


def test_cbn_single_threshold_triggers():
    engine = RuleEngine()
    txn = _make_enriched(amount=5_000_000.0)
    decision, reason = engine.evaluate(txn)
    assert decision == Decision.FLAG
    assert "CBN" in reason


def test_cbn_daily_cumulative_triggers():
    engine = RuleEngine()
    txn = _make_enriched(total_amount_24h=10_000_000.0)
    decision, reason = engine.evaluate(txn)
    assert decision == Decision.FLAG
    assert "CBN" in reason


def test_velocity_attack_triggers_block():
    engine = RuleEngine()
    txn = _make_enriched(tx_count_5m=11)
    decision, reason = engine.evaluate(txn)
    assert decision == Decision.BLOCK
    assert "Velocity" in reason or "velocity" in reason


def test_amount_spike_triggers():
    engine = RuleEngine()
    # Amount is 15x the 30-day average
    txn = _make_enriched(amount=150_000.0, avg_amount_30d=10_000.0)
    decision, reason = engine.evaluate(txn)
    assert decision == Decision.FLAG


def test_odd_hours_high_value_triggers():
    engine = RuleEngine()
    txn = _make_enriched(hour_of_day=2, amount=600_000.0, avg_amount_30d=200_000.0)
    decision, reason = engine.evaluate(txn)
    assert decision == Decision.FLAG
    assert "hours" in reason.lower() or "hour" in reason.lower()