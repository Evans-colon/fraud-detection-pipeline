# Real-Time Fraud Detection Pipeline

A production-grade real-time fraud detection system for Nigerian fintech
payment transactions. Built with Apache Kafka, LightGBM, ONNX Runtime,
FastAPI, and SQLite.

> **Context:** This is Project 1 of a four-project Nigerian fintech
> engineering curriculum covering fraud detection, credit risk scoring,
> AML monitoring, and churn prediction.

---

## What this does

Every payment transaction (NIP transfer, card payment, USSD, mobile) is:

1. **Ingested** via Kafka from a simulated Nigerian payment gateway
2. **Enriched** with 14 behavioural features computed in real time (velocity, amount baseline, new device/recipient flags, calendar context)
3. **Scored** by a hybrid rule engine (CBN thresholds) + LightGBM model (fraud probability)
4. **Routed** to one of three decisions: ALLOW, FLAG for ops review, or BLOCK
5. **Stored** in an audit trail (NDPA 2023 compliant)
6. **Displayed** on a live ops dashboard

**Performance:** 0.1ms P95 scoring latency, 1,400+ TPS feature engine throughput, F1=0.836, ROC-AUC=0.979.

---

## Nigerian fintech context

This pipeline is built specifically for the Nigerian payment ecosystem:

- **NIP/NIBSS** bank-to-bank transfers dominate by volume (35% of simulated traffic)
- **CBN mandatory thresholds** — single transaction ≥ ₦5M and daily cumulative ≥ ₦10M are hard rules, not model parameters
- **Salary period** (25th-2nd) creates legitimate high-volume NIP activity — the model accounts for this to suppress false positives
- **NDPA 2023** — all PII (account numbers, IPs) is hashed before entering the pipeline; raw identifiers are never stored
- **Nigerian bank codes** — GTBank (000013), Access (000014), Zenith (000015), Kuda (090267), OPay (100004), Moniepoint (100035) etc.

---

## Architecture

```
Transaction generator
        │
        ▼ Kafka: raw-transactions
Feature engine (sliding window aggregations)
        │
        ▼ Kafka: enriched-transactions
Scoring service (rule engine + LightGBM ONNX)
        │
        ▼ Kafka: fraud-alerts
Alert pipeline ──► SQLite alert store
        │
        ▼
Ops dashboard (FastAPI + WebSocket)
```

See [docs/architecture.md](docs/architecture.md) for the full system diagram, component descriptions, data flow, and deployment guide.

---

## Project structure

```
fraud-detection-pipeline/
├── README.md
├── docker-compose.yml          # Kafka + Zookeeper
├── config/
│   ├── settings.py             # Centralised config (CBN thresholds, Kafka topics, etc.)
│   └── rules.yaml              # Fraud detection rules
├── src/
│   ├── producer/
│   │   ├── schemas.py          # Pydantic models (RawTransaction, EnrichedTransaction, etc.)
│   │   └── generator.py        # Nigerian transaction simulator
│   ├── features/
│   │   └── engine.py           # Real-time windowed feature computation
│   ├── scoring/
│   │   └── scorer.py           # Rule engine + ONNX model inference
│   ├── alerts/
│   │   ├── store.py            # SQLite alert store
│   │   └── pipeline.py         # Kafka → SQLite consumer
│   ├── dashboard/
│   │   └── app.py              # FastAPI + WebSocket ops dashboard
│   └── training/
│       ├── generate_dataset.py # Historical dataset generation
│       ├── train.py            # LightGBM training + MLflow tracking
│       └── export.py           # ONNX export + parity verification
├── models/
│   ├── feature_names.json      # Feature column names (committed)
│   └── threshold.json          # Optimal decision threshold (committed)
└── docs/
    ├── PRD.md                  # Product Requirements Document
    ├── PID.md                  # Project Initiation Document
    └── architecture.md         # Technical architecture + decisions
```

---

## Prerequisites

- Python 3.12+
- Docker + Docker Compose plugin (`docker compose version`)
- 4GB+ RAM (Kafka + Python services)

---

## Setup

**1. Clone and create virtual environment:**
```bash
git clone git@github.com:Evans-colon/fraud-detection-pipeline.git
cd fraud-detection-pipeline
python -m venv venv
source venv/bin/activate
```

**2. Install dependencies:**
```bash
pip install confluent-kafka lightgbm scikit-learn pandas mlflow \
    onnx==1.22.0 skl2onnx==1.20.0 onnxmltools==1.16.0 onnxruntime==1.19.2 \
    fastapi "uvicorn[standard]" pydantic joblib websockets
```

**3. Start Kafka infrastructure:**
```bash
docker compose up -d zookeeper kafka
docker compose up kafka-init
```

Verify topics created:
```bash
docker exec fraud-kafka kafka-topics --list --bootstrap-server localhost:9092
```

Expected output:
```
enriched-transactions
fraud-alerts
model-metrics
raw-transactions
```

**4. Train the fraud model:**
```bash
python -m src.training.generate_dataset   # ~60s, generates 50K transactions
python -m src.training.train              # ~30s, trains LightGBM
python -m src.training.export             # ~10s, exports to ONNX
```

Expected training output:
```
roc_auc              0.9793
precision            0.8716
recall               0.8026
f1                   0.8357
best_threshold       0.77
✅ Parity check passed. Safe to serve.
```

---

## Running the pipeline

Open 5 terminals in the project directory (with venv activated):

```bash
# Terminal 1 — Transaction producer
python -m src.producer.generator

# Terminal 2 — Feature engine
python -m src.features.engine

# Terminal 3 — Scoring service
python -m src.scoring.scorer

# Terminal 4 — Alert pipeline
python -m src.alerts.pipeline

# Terminal 5 — Ops dashboard
python -m src.dashboard.app
```

Open the dashboard at **http://localhost:8000**

---

## Verifying it works

**Check Kafka has messages:**
```bash
docker exec fraud-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw-transactions \
  --max-messages 3
```

**Check enriched features are computed:**
```bash
docker exec fraud-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic enriched-transactions \
  --max-messages 1
```

**Check alerts are being stored:**
```bash
python -c "
from src.alerts.store import get_stats
print(get_stats())
"
```

**Check scoring decisions:**
```bash
# Watch the scorer terminal — should show:
# allow=XXXX flag=XXX block=XXX | p95 latency=0.1ms | TPS=XXXX
```

---

## Model details

| Property | Value |
|---|---|
| Algorithm | LightGBM (gradient boosting) |
| Features | 20 (see feature engine) |
| Training data | 50,000 transactions (10% fraud) |
| Test F1 | 0.836 |
| Test ROC-AUC | 0.979 |
| Decision threshold | 0.77 (optimised on validation F1) |
| Serving format | ONNX (via onnxruntime) |
| P95 inference latency | 0.1ms |

**Why LightGBM:** Sub-5ms inference, native feature importance for explainability, no GPU dependency at serving time. See [docs/architecture.md](docs/architecture.md) for full rationale.

---

## Fraud rules (CBN compliance)

| Rule | Threshold | Action | Regulatory? |
|---|---|---|---|
| Single transaction | ≥ ₦5,000,000 | FLAG | ✅ CBN mandatory |
| Daily cumulative | ≥ ₦10,000,000 | FLAG | ✅ CBN mandatory |
| Velocity attack | > 10 txns in 5 min | BLOCK | Business rule |
| High frequency | > 50 txns in 1 hour | FLAG | Business rule |
| Amount spike | > 10x 30-day average | FLAG | Business rule |
| Odd hours | 1AM-4AM + amount > ₦500K | FLAG | Business rule |

CBN rules run before the ML model and cannot be overridden by model output.

---

## Documentation

| Document | Location | Purpose |
|---|---|---|
| Product Requirements Document | [docs/PRD.md](docs/PRD.md) | What the system does and why |
| Project Initiation Document | [docs/PID.md](docs/PID.md) | How the project was executed, decisions made |
| Architecture Document | [docs/architecture.md](docs/architecture.md) | Technical design, data flow, component details |

---

## Tech stack

| Component | Technology |
|---|---|
| Message streaming | Apache Kafka 7.6.0 (Confluent) |
| ML training | LightGBM + scikit-learn + MLflow |
| ML serving | ONNX Runtime (no LightGBM at inference) |
| Stream processing | Python + confluent-kafka |
| API + dashboard | FastAPI + WebSocket + uvicorn |
| Alert storage | SQLite (upgrade path: PostgreSQL) |
| Infrastructure | Docker Compose |
| Data validation | Pydantic v2 |

---

## Extending this project

**Add a new fraud rule:** Edit `config/rules.yaml` and `src/scoring/scorer.py` `RuleEngine.evaluate()`.

**Add a new feature:** Add the field to `EnrichedTransaction` in `schemas.py`, compute it in `FeatureEngine.compute()` in `engine.py`, add it to `FEATURE_COLS` in `train.py`, and retrain.

**Retrain the model:** Run `generate_dataset.py` → `train.py` → `export.py`. The new `fraud_model.onnx` is picked up on next scorer restart.

**Scale horizontally:** Increase Kafka partitions and run multiple instances of the feature engine and scorer. Each instance joins the same consumer group and Kafka distributes partitions automatically.
