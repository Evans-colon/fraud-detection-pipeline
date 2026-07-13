"""
Generate a labeled historical dataset for fraud model training.

Uses the same generator as the live pipeline — this is intentional.
Training data generated from the same distribution as live data
minimises training-serving skew.

Output: data/transactions.csv with columns matching EnrichedTransaction
fields plus a binary 'is_fraud' label.

Run: python -m src.training.generate_dataset
"""
import csv
import os
import random
from datetime import datetime, timedelta, timezone

from src.producer.generator import generate_transaction
from src.features.engine import FeatureEngine

OUTPUT_PATH = "data/transactions.csv"
NUM_TRANSACTIONS = 50_000
FRAUD_RATE_OVERRIDE = 0.10
#10% fraud in training data vs 2% live
#Oversampling fraud gives the model more



def generate_dataset(
    n: int = NUM_TRANSACTIONS,
    fraud_rate: float = FRAUD_RATE_OVERRIDE,
    output_path: str = OUTPUT_PATH,
) -> None:
    os.makedirs("data", exist_ok=True)
    engine = FeatureEngine()

    # Spread transactions over the last 30 days for realistic
    # windowed feature computation during training
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=30)

    print(f"Generating {n:,} transactions ({fraud_rate*100:.0f}% fraud rate)...")
    print(f"Output: {output_path}")

    rows = []
    fraud_count = 0

    for i in range(n):
        #Random timestamp within the last 30 days
        ts = start + timedelta(seconds=random.randint(0, 30 * 24 * 3600))

        # Override fraud rate for training data
        is_fraud = random.random() < fraud_rate
        if is_fraud:
            from src.producer.generator import generate_fraud_transaction
            txn, _ = generate_fraud_transaction(ts), True
            txn = txn[0] if isinstance(txn, tuple) else txn
            fraud_count += 1
        else:
            from src.producer.generator import generate_legitimate_transaction
            txn = generate_legitimate_transaction(ts)

        # Compute features using the same engine as the live pipeline
        enriched = engine.compute(txn)

        row = {
            # Label
            "is_fraud": int(is_fraud),
            # Core transaction fields
            "amount": enriched.amount,
            "channel": enriched.channel.value,
            "transaction_type": enriched.transaction_type.value,
            "is_international": int(enriched.is_international),
            "sender_bank_code": enriched.sender_bank_code,
            # Windowed features
            "tx_count_5m": enriched.tx_count_5m,
            "tx_count_1h": enriched.tx_count_1h,
            "tx_count_24h": enriched.tx_count_24h,
            "total_amount_5m": enriched.total_amount_5m,
            "total_amount_1h": enriched.total_amount_1h,
            "total_amount_24h": enriched.total_amount_24h,
            "avg_amount_30d": enriched.avg_amount_30d,
            "unique_recipients_1h": enriched.unique_recipients_1h,
            # Behavioural flags
            "is_new_recipient": int(enriched.is_new_recipient),
            "is_new_device": int(enriched.is_new_device),
            # Calendar features
            "hour_of_day": enriched.hour_of_day,
            "day_of_week": enriched.day_of_week,
            "is_salary_period": int(enriched.is_salary_period),
            "is_weekend": int(enriched.is_weekend),
        }
        rows.append(row)

        if (i + 1) % 10_000 == 0:
            print(f"  {i+1:>6,} / {n:,} generated...")

    # Write CSV
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone.")
    print(f"Total: {n:,} | Fraud: {fraud_count:,} ({100*fraud_count/n:.1f}%)")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    generate_dataset()