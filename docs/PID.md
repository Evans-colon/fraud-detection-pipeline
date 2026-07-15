# Project Initiation Document (PID)
# Real-Time Fraud Detection Pipeline

**Document version:** 1.0
**Project status:** Complete (v1.0)
**Author:** Evans (Data/AI Engineer)
**Date:** July 2026

---

## 1. Project overview

### What was built
A production-grade real-time fraud detection pipeline for Nigerian fintech
payment transactions. The system ingests transactions via Apache Kafka,
computes behavioural features in a stateful streaming engine, scores each
transaction through a hybrid rule + ML engine in under 15ms, and routes
decisions (ALLOW/FLAG/BLOCK) to an ops dashboard and persistent alert store.

### Why it was initiated
Nigerian fintechs face increasing fraud pressure on NIP, card, and USSD
payment channels, while CBN regulatory requirements mandate automated
detection and reporting of suspicious transactions. Manual review processes
cannot scale to real-time payment volumes. This project demonstrates a
production-ready architecture for addressing both problems simultaneously.

### Scope
This project covers the full data pipeline from transaction ingestion to
ops dashboard, including model training and serving. It does not cover
integration with real NIBSS payment rails, customer-facing notifications,
or AML network analysis (separate project).

---

## 2. Project objectives

1. Build a streaming data pipeline capable of processing ≥500 transactions/second
2. Train and deploy a fraud model achieving F1 ≥ 0.80 on test data
3. Implement all mandatory CBN regulatory thresholds as hard rules
4. Provide a live ops dashboard for fraud analyst workflow
5. Demonstrate production patterns: ONNX serving, MLflow tracking, Docker deployment
6. Comply with NDPA 2023 data minimisation requirements by design

---

## 3. Deliverables

| Deliverable | Status | Notes |
|---|---|---|
| Transaction generator (Nigerian payment simulation) | ✅ Complete | 5 fraud patterns, NIP-heavy channel mix |
| Kafka infrastructure (4 topics) | ✅ Complete | Docker Compose, topic creation automated |
| Feature engine | ✅ Complete | 1,400+ TPS, 0 errors, sliding window aggregations |
| Training dataset (50,000 transactions) | ✅ Complete | 10% fraud oversampling |
| LightGBM fraud model | ✅ Complete | F1: 0.836, ROC-AUC: 0.979 |
| ONNX export with parity verification | ✅ Complete | 0.000000 max probability difference |
| Scoring service (rule engine + ML) | ✅ Complete | 0.1ms P95 latency |
| Alert pipeline + SQLite store | ✅ Complete | 4,698+ alerts stored, 0 errors |
| Ops dashboard (FastAPI + WebSocket) | ✅ Complete | Live feed, stats, recent alerts |
| PRD | ✅ Complete | This document set |
| Architecture document | ✅ Complete | docs/architecture.md |
| README | ✅ Complete | Full setup and run instructions |

---

## 4. Technical decisions and rationale

### 4.1 Apache Kafka over RabbitMQ
RabbitMQ is a message queue — messages are consumed and deleted. Kafka is
an event log — messages are retained and replayable. Fraud detection requires
fan-out (multiple consumers reading the same transaction) and replay (backtesting
new models against historical data). RabbitMQ cannot support either natively.

### 4.2 LightGBM over neural networks
For tabular transaction data with engineered features, LightGBM matches or
outperforms deep learning while offering:
- Sub-5ms inference via ONNX export (vs 50ms+ for transformers)
- Native feature importance for explainability (regulatory requirement)
- No GPU dependency at inference time
- Proven production track record at major fintechs

### 4.3 Hybrid rule + ML scoring
CBN regulations mandate specific actions on transactions above ₦5M/₦10M
thresholds regardless of model output. A pure ML system would constitute a
regulatory violation. The hybrid approach: rules handle deterministic
regulatory requirements, ML handles probabilistic pattern detection.

### 4.4 ONNX for model serving
ONNX decouples training from serving. The scoring service has no dependency
on LightGBM or scikit-learn — only onnxruntime. This means:
- Smaller Docker images (no training libraries in production)
- Model can be replaced without changing serving code
- Framework-agnostic: future models (XGBoost, PyTorch) export to the same format

### 4.5 In-memory feature state over Redis
At development scale (500 accounts, hundreds of TPS), an in-memory Python
dictionary is sufficient and avoids a Redis infrastructure dependency. The
`AccountState` + `SlidingWindow` architecture is designed to be extracted
to Redis sorted sets at production scale with minimal code changes.

### 4.6 SQLite over PostgreSQL for alert storage
SQLite requires zero infrastructure, handles hundreds of alert inserts per
second comfortably, and supports the full SQL query surface needed for the
dashboard. The connection string is the only change needed to migrate to
PostgreSQL when multi-node deployment requires it.

---

## 5. Key metrics achieved

| Metric | Target | Achieved |
|---|---|---|
| Model F1 score | ≥ 0.80 | 0.836 |
| Model ROC-AUC | ≥ 0.95 | 0.979 |
| Scoring P95 latency | < 15ms | 0.1ms |
| Feature engine throughput | ≥ 500 TPS | 1,400+ TPS |
| Alert pipeline errors | 0 | 0 |
| CBN threshold coverage | 100% | 100% |

---

## 6. Issues encountered and resolutions

| Issue | Resolution |
|---|---|
| `onnxmltools` incompatible with new `onnx` versions | Pinned `onnx==1.22.0`, `onnxmltools==1.16.0`, `skl2onnx==1.20.0` |
| Docker DNS resolution failure (IPv6) | Added `{"dns": ["8.8.8.8", "1.1.1.1"]}` to `/etc/docker/daemon.json`, disabled IPv6 |
| `websocket` package shadowing FastAPI's WebSocket | Uninstalled conflicting `websocket==0.2.1` package |
| Dashboard HTML trapped inside websocket function | Fixed indentation — `DASHBOARD_HTML` must be at module level |
| Low F1 score (0.787) on initial training | Added `amount_to_avg_ratio` feature + threshold optimisation → 0.836 |
| KAFKA_TOPIC_RAW_TRANSACTION import error | Corrected to `KAFKA_TOPIC_RAW` matching settings.py definition |
| Port 8000 conflicts across projects | Used `sudo fuser -k 8000/tcp` to clear before starting dashboard |

---

## 7. Lessons learned

**On data engineering:**
- Windowed feature computation is the most critical and most complex component
  of a real-time ML pipeline — invest in testing it thoroughly before building
  the model
- Kafka topic partition count must match expected consumer parallelism — plan
  this before creating topics (decreasing partitions requires topic deletion)
- Consumer group IDs must be unique per service — sharing a group causes
  only one service to receive each message

**On ML engineering:**
- Training-serving skew is the most dangerous silent bug — using the same
  `FeatureEngine` class for both training data generation and live serving
  eliminates this risk entirely
- F1 score on simulated data is an optimistic upper bound — real fraud is
  subtler, expect degradation in production
- The optimal decision threshold is rarely 0.5 — always tune it on a
  validation set using the metric that matches your business objective

**On Nigerian fintech context:**
- Salary period (25th-28th) genuinely changes transaction patterns enough
  to affect model accuracy — it must be a feature, not an afterthought
- NIP dominates Nigerian transaction volume — any model trained without
  NIP-weighted data will underperform on real traffic
- CBN thresholds are hard business requirements, not suggestions — they
  must be implemented as rules before the model, never inside it

---

## 8. Future enhancements (v2.0 backlog)

- [ ] Prometheus + Grafana for infrastructure monitoring
- [ ] Redis for distributed feature state (enables horizontal scaling)
- [ ] Evidently drift detection with automated retraining trigger
- [ ] PostgreSQL migration for multi-node alert storage
- [ ] Model registry integration (MLflow) for hot-swap model deployment
- [ ] Kafka Connect for direct NIBSS/payment gateway integration
- [ ] Graph-based AML analysis (separate Project 3)
- [ ] A/B testing framework for model comparison in production
