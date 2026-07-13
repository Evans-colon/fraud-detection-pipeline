"""
Export the trained LightGBM fraud model to ONNX format.

Why ONNX:
    Decouples the serving layer from the training framework.
    The scoring service loads model.onnx via onnxruntime — no
    LightGBM or scikit-learn dependency needed at inference time.


Run: python -m src.training.export
"""
import json
import os

import joblib
import numpy as np
import onnxruntime as rt
from lightgbm import LGBMClassifier
from onnxmltools.convert.lightgbm.operator_converters.LightGbm import (
    convert_lightgbm,
)
from skl2onnx import convert_sklearn, update_registered_converter
from skl2onnx.common.data_types import FloatTensorType
from skl2onnx.common.shape_calculator import (
    calculate_linear_classifier_output_shapes,
)

MODEL_IN = "models/fraud_model.pkl"
ONNX_OUT = "models/fraud_model.onnx"
FEATURE_NAMES_IN = "models/feature_names.json"
THRESHOLD_OUT = "models/threshold.json"


# Register LightGBM converter — same fix as Project 1
update_registered_converter(
    LGBMClassifier,
    "LightGbmLGBMClassifier",
    calculate_linear_classifier_output_shapes,
    convert_lightgbm,
    options={"nocl": [True, False], "zipmap": [True, False, "columns"]},
)


def main():
    os.makedirs("models", exist_ok=True)

    print("Loading trained model...")
    model = joblib.load(MODEL_IN)

    with open(FEATURE_NAMES_IN) as f:
        feature_names = json.load(f)
    n_features = len(feature_names)
    print(f"Features: {n_features} — {feature_names}")

    print("Converting to ONNX...")
    initial_type = [("input", FloatTensorType([None, n_features]))]
    onnx_model = convert_sklearn(
        model,
        initial_types=initial_type,
        target_opset={"": 17, "ai.onnx.ml": 3},
    )

    with open(ONNX_OUT, "wb") as f:
        f.write(onnx_model.SerializeToString())
    print(f"ONNX model saved to {ONNX_OUT}")

    # --- Parity check ---
    print("\nVerifying parity between native and ONNX model...")
    import pandas as pd
    from sklearn.preprocessing import LabelEncoder
    from src.training.train import CATEGORICAL_COLS, load_and_prepare

    X_train, X_test, y_train, y_test = load_and_prepare("data/transactions.csv")
    X_sample = X_test.iloc[:20].astype(np.float32)

    native_probs = model.predict_proba(X_sample)[:, 1]

    sess = rt.InferenceSession(ONNX_OUT, providers=["CPUExecutionProvider"])
    input_name = sess.get_inputs()[0].name
    onnx_out = sess.run(None, {input_name: X_sample.values})
    onnx_probs = np.array([p[1] for p in onnx_out[1]])

    max_diff = np.abs(native_probs - onnx_probs).max()
    print(f"Max probability difference: {max_diff:.6f}")

    if max_diff > 1e-3:
        print("⚠️  WARNING: parity check failed — investigate before serving.")
    else:
        print("✅ Parity check passed. Safe to serve.")

    # Save the optimal threshold found during training
    # The scoring service will load this instead of hardcoding 0.5
    threshold = 0.77  # from training output
    with open(THRESHOLD_OUT, "w") as f:
        json.dump({"threshold": threshold}, f)
    print(f"\nThreshold saved to {THRESHOLD_OUT}: {threshold}")
    print(f"Next: start the scoring service.")


if __name__ == "__main__":
    main()