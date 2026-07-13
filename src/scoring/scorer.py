"""
Fraud scoring service.

Consumes enriched transactions from Kafka, applies:
  1. Rule engine (hard CBN thresholds + business rules)
  2. ONNX model inference (probabilistic fraud score)
  3. Decision router (ALLOW / FLAG / BLOCK)

Publishes scored transactions to fraud-alerts topic
for flagged/blocked decisions.

Run: python -m src.scoring.scorer
"""
import json
import os
import time
from datetime import datetime, timezone

import numpy as np
import onnxruntime as rt
from confluent_kafka import Consumer, KafkaError, Producer

from config.settings import (
    CBN_DAILY_CUMULATIVE_NGN,
    CBN_SINGLE_TXN_REPORT_NGN,
    KAFKA_BOOTSTRAP,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC_ALERTS,
    KAFKA_TOPIC_ENRICHED_TRANSACTION,
    SCORE_THRESHOLD_BLOCK,
    SCORE_THRESHOLD_FLAG,
)
from src.producer.schemas import Decision, EnrichedTransaction, FraudAlert, ScoredTransaction

MODEL_PATH = os.getenv("MODEL_PATH", "models/fraud_model.onnx")
FEATURE_NAMES_PATH = "models/feature_names.json"
THRESHOLD_PATH = "models/threshold.json"

# Categorical encoding — must match training exactly
CHANNEL_MAP = {"NIP": 0, "CARD_POS": 1, "CARD_WEB": 2, "USSD": 3, "MOBILE_APP": 4}
TX_TYPE_MAP = {
    "TRANSFER": 0, "PAYMENT": 1, "WITHDRAWAL": 2,
    "AIRTIME": 3, "BILL_PAYMENT": 4
}


class RuleEngine:
    """
    Hard rule engine — runs before the ML model.

    Rules are evaluated in priority order. First match wins.
    Regulatory rules (CBN) always take precedence over business rules.
    """

    def evaluate(
        self, txn: EnrichedTransaction
    ) -> tuple[Decision | None, str | None]:
        """
        Returns (decision, reason) if a rule triggers, else (None, None).
        None means no rule matched — pass to ML model.
        """
        # --- CBN regulatory rules (mandatory) ---
        if txn.amount >= CBN_SINGLE_TXN_REPORT_NGN:
            return Decision.FLAG, f"CBN CTR: single transaction ₦{txn.amount:,.0f} >= ₦5M threshold"

        if txn.total_amount_24h >= CBN_DAILY_CUMULATIVE_NGN:
            return Decision.FLAG, f"CBN CTR: daily cumulative ₦{txn.total_amount_24h:,.0f} >= ₦10M threshold"

        # --- Velocity rules ---
        if txn.tx_count_5m > 10:
            return Decision.BLOCK, f"Velocity attack: {txn.tx_count_5m} transactions in 5 minutes"

        if txn.tx_count_1h > 50:
            return Decision.FLAG, f"High frequency: {txn.tx_count_1h} transactions in 1 hour"

        # --- Amount anomaly ---
        if txn.avg_amount_30d > 0 and txn.amount > txn.avg_amount_30d * 10:
            return Decision.FLAG, (
                f"Amount spike: ₦{txn.amount:,.0f} is "
                f"{txn.amount/txn.avg_amount_30d:.1f}x the 30-day average"
            )

        # --- Odd hours + high value ---
        if txn.hour_of_day in range(1, 4) and txn.amount > 500_000:
            return Decision.FLAG, f"High-value transaction at {txn.hour_of_day:02d}:00 (odd hours)"

        return None, None


class FraudScorer:
    """
    ML-based fraud scorer using the exported ONNX model.
    """

    def __init__(self, model_path: str, feature_names: list[str], threshold: float):
        self.session = rt.InferenceSession(
            model_path, providers=["CPUExecutionProvider"]
        )
        self.input_name = self.session.get_inputs()[0].name
        self.feature_names = feature_names
        self.threshold = threshold

    def _encode_bank_code(self, code: str) -> float:
        """Simple hash-based encoding for bank codes."""
        return float(hash(code) % 100)

    def extract_features(self, txn: EnrichedTransaction) -> np.ndarray:
        amount_to_avg = txn.amount / (txn.avg_amount_30d + 1)
        features = [
            txn.amount,
            amount_to_avg,
            float(CHANNEL_MAP.get(txn.channel.value, 0)),
            float(TX_TYPE_MAP.get(txn.transaction_type.value, 0)),
            float(txn.is_international),
            self._encode_bank_code(txn.sender_bank_code),
            float(txn.tx_count_5m),
            float(txn.tx_count_1h),
            float(txn.tx_count_24h),
            float(txn.total_amount_5m),
            float(txn.total_amount_1h),
            float(txn.total_amount_24h),
            float(txn.avg_amount_30d),
            float(txn.unique_recipients_1h),
            float(txn.is_new_recipient),
            float(txn.is_new_device),
            float(txn.hour_of_day),
            float(txn.day_of_week),
            float(txn.is_salary_period),
            float(txn.is_weekend),
        ]
        return np.array([features], dtype=np.float32)

    def score(self, txn: EnrichedTransaction) -> float:
        """Returns fraud probability 0.0-1.0."""
        X = self.extract_features(txn)
        outputs = self.session.run(None, {self.input_name: X})
        return float(outputs[1][0][1])  # P(fraud)

    def decide(self, score: float) -> tuple[Decision, str]:
        if score >= SCORE_THRESHOLD_BLOCK:
            return Decision.BLOCK, f"ML score {score:.3f} >= block threshold {SCORE_THRESHOLD_BLOCK}"
        if score >= SCORE_THRESHOLD_FLAG:
            return Decision.FLAG, f"ML score {score:.3f} >= flag threshold {SCORE_THRESHOLD_FLAG}"
        return Decision.ALLOW, f"ML score {score:.3f} below thresholds"


def run() -> None:
    """Main scoring consumer loop."""
    # Load model config
    with open(FEATURE_NAMES_PATH) as f:
        feature_names = json.load(f)
    with open(THRESHOLD_PATH) as f:
        threshold = json.load(f)["threshold"]

    rule_engine = RuleEngine()
    scorer = FraudScorer(MODEL_PATH, feature_names, threshold)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"{KAFKA_CONSUMER_GROUP}-scorer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    consumer.subscribe([KAFKA_TOPIC_ENRICHED_TRANSACTION])

    print("Scoring service started")
    print(f"  Model: {MODEL_PATH}")
    print(f"  Threshold: {threshold}")
    print(f"  Block threshold: {SCORE_THRESHOLD_BLOCK}")
    print(f"  Flag threshold: {SCORE_THRESHOLD_FLAG}")

    counts = {"allow": 0, "flag": 0, "block": 0, "errors": 0}
    latencies = []
    total = 0
    start = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                counts["errors"] += 1
                continue

            t0 = time.monotonic()
            try:
                data = json.loads(msg.value().decode())
                txn = EnrichedTransaction(**data)

                # Step 1: rule engine
                rule_decision, rule_reason = rule_engine.evaluate(txn)

                if rule_decision:
                    final_decision = rule_decision
                    reason = rule_reason
                    ml_score = None
                else:
                    # Step 2: ML model
                    ml_score = scorer.score(txn)
                    final_decision, reason = scorer.decide(ml_score)

                latency_ms = (time.monotonic() - t0) * 1000
                latencies.append(latency_ms)
                total += 1
                counts[final_decision.value.lower()] += 1

                # Publish alert for flagged/blocked transactions
                if final_decision != Decision.ALLOW:
                    import uuid
                    alert = FraudAlert(
                        alert_id=str(uuid.uuid4()),
                        transaction_id=txn.transaction_id,
                        timestamp=txn.timestamp,
                        account_id=txn.account_id,
                        amount=txn.amount,
                        channel=txn.channel,
                        decision=final_decision,
                        reason=reason,
                        ml_score=ml_score,
                    )
                    producer.produce(
                        KAFKA_TOPIC_ALERTS,
                        key=txn.account_id.encode(),
                        value=alert.model_dump_json().encode(),
                    )

                if total % 100 == 0:
                    elapsed = time.time() - start
                    p95 = np.percentile(latencies[-100:], 95)
                    producer.flush()
                    print(
                        f"  [{total:>5}] "
                        f"allow={counts['allow']} "
                        f"flag={counts['flag']} "
                        f"block={counts['block']} | "
                        f"p95 latency={p95:.1f}ms | "
                        f"TPS={total/elapsed:.1f}"
                    )

            except Exception as exc:
                counts["errors"] += 1
                print(f"Error scoring transaction: {exc}")

    except KeyboardInterrupt:
        elapsed = time.time() - start
        print(f"\nStopped. {total} transactions scored in {elapsed:.1f}s")
        print(f"Decisions: {counts}")
        if latencies:
            print(f"Latency p50={np.percentile(latencies, 50):.1f}ms "
                  f"p95={np.percentile(latencies, 95):.1f}ms "
                  f"p99={np.percentile(latencies, 99):.1f}ms")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run()