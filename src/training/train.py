"""
Train a LightGBM fraud detection model on the generated dataset.

Tracks every run with MLflow. Exports the best model as a pickle
for ONNX conversion in the next step.

Run: python -m src.training.train
"""
import json
import os

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder

DATA_PATH = "data/transactions.csv"
MODEL_OUT = "models/fraud_model.pkl"
FEATURE_NAMES_OUT = "models/feature_names.json"
MLFLOW_EXPERIMENT = "fraud-detection-lgbm"

#Categorical columns that need encoding
CATEGORICAL_COLS = ["channel", "transaction_type", "sender_bank_code"]

#Feature columns (everything except the label)
FEATURE_COLS = [

    "amount",
    "amount_to_avg_ratio",
    "channel",
    "transaction_type",
    "is_international",
    "sender_bank_code",
    "tx_count_5m",
    "tx_count_1h",
    "tx_count_24h",
    "total_amount_5m",
    "total_amount_1h",
    "total_amount_24h",
    "avg_amount_30d",
    "unique_recipients_1h",
    "is_new_recipient",
    "is_new_device",
    "hour_of_day",
    "day_of_week",
    "is_salary_period",
    "is_weekend",
]


def load_and_prepare(path: str) -> tuple:
    df = pd.read_csv(path)

    # Encode categoricals as integers
    # LightGBM supports categorical features natively but needs integers
    encoders = {}
    for col in CATEGORICAL_COLS:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col].astype(str))
        encoders[col] = le
    df["amount_to_avg_ratio"] = df["amount"] / (df["avg_amount_30d"] + 1)
    X = df[FEATURE_COLS]
    y = df["is_fraud"]

    return train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y,  # preserve fraud ratio in both splits
    )


def evaluate(model, X_test, y_test) -> dict:
    y_prob = model.predict_proba(X_test)[:, 1]

    # Find optimal threshold by maximising F1
    thresholds = np.arange(0.1, 0.9, 0.01)
    f1_scores = []
    for t in thresholds:
        y_pred_t = (y_prob >= t).astype(int)
        f1_scores.append(f1_score(y_test, y_pred_t))
    best_threshold = thresholds[np.argmax(f1_scores)]

    y_pred = (y_prob >= best_threshold).astype(int)

    metrics = {
        "roc_auc": round(roc_auc_score(y_test, y_prob), 4),
        "avg_precision": round(average_precision_score(y_test, y_prob), 4),
        "precision": round(precision_score(y_test, y_pred), 4),
        "recall": round(recall_score(y_test, y_pred), 4),
        "f1": round(f1_score(y_test, y_pred), 4),
        "best_threshold": round(float(best_threshold), 3),
    }
    return metrics, best_threshold


def main():
    os.makedirs("models", exist_ok=True)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    print(f"Loading dataset from {DATA_PATH}...")
    X_train, X_test, y_train, y_test = load_and_prepare(DATA_PATH)
    print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
    print(f"Fraud in test: {y_test.sum()} ({100*y_test.mean():.1f}%)")

    params = {
        "n_estimators": 300,
        "max_depth": 8,
        "learning_rate": 0.05,
        "num_leaves": 63,
        "min_child_samples": 20,
        "scale_pos_weight": 9,  # compensates for class imbalance (9:1 ratio)
        "random_state": 42,
        "verbose": -1,
    }

    with mlflow.start_run(run_name="fraud-lgbm-v1"):
        mlflow.log_params(params)
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        mlflow.log_param("features", FEATURE_COLS)

        print("\nTraining LightGBM...")
        model = LGBMClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
        )

        print("Evaluating...")
        metrics, best_threshold = evaluate(model, X_test, y_test)
        mlflow.log_metrics(metrics)
        mlflow.log_param("best_threshold", best_threshold)

        print("\nResults:")
        for k, v in metrics.items():
            print(f"  {k:<20} {v}")

        # Save model + feature names
        import joblib
        joblib.dump(model, MODEL_OUT)
        with open(FEATURE_NAMES_OUT, "w") as f:
            json.dump(FEATURE_COLS, f)

        mlflow.sklearn.log_model(
            model, "model",
            serialization_format="pickle"
        )

        print(f"\nModel saved to {MODEL_OUT}")
        print(f"Feature names saved to {FEATURE_NAMES_OUT}")
        print("Run 'mlflow ui' to inspect this run.")


if __name__ == "__main__":
    main()