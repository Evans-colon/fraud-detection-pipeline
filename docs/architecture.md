# Architecture Document
# Real-Time Fraud Detection Pipeline

**Version:** 1.0
**Date:** July 2026

---

## 1. System overview

The fraud detection pipeline is a real-time event-driven system that
processes Nigerian payment transactions as they arrive, enriches them
with behavioural features, scores them for fraud risk, and routes
decisions to an ops team.

The system follows the **Lambda-lite architecture** — a streaming path
handles real-time transaction scoring, while a batch path handles model
training on historical data. Both paths share the same feature definitions,
eliminating training-serving skew.

---

## 2. High-level architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                                       │
│                                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
│  │ NIP transfers│  │ Card payments│  │ USSD / mobile│                   │
│  │ (bank-to-bank│  │ (POS + online│  │ (*737#, *901#│                   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                   │
└─────────┼─────────────────┼─────────────────┼───────────────────────────┘
          └────────────┬────┘─────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    STREAMING LAYER (Kafka)                               │
│                                                                         │
│  raw-transactions (3 partitions)    ◄── Producer writes here            │
│  enriched-transactions (3 partitions) ◄── Feature engine writes here    │
│  fraud-alerts (1 partition)         ◄── Scorer writes here              │
│  model-metrics (1 partition)        ◄── Scorer writes latency/scores    │
└──────────┬──────────────────────────────────────────────────────────────┘
           │
           ├─────────────────────────────────────────┐
           ▼                                         ▼
┌──────────────────────┐                   ┌──────────────────────┐
│   FEATURE ENGINE     │                   │   BATCH TRAINING     │
│                      │                   │   PATH               │
│  SlidingWindow per   │                   │                      │
│  account (in-memory) │                   │  generate_dataset.py │
│                      │                   │  → train.py          │
│  Computes:           │                   │  → export.py         │
│  • tx_count_5m/1h/24h│                   │                      │
│  • total_amount_*    │                   │  Same FeatureEngine  │
│  • avg_amount_30d    │                   │  class used here     │
│  • is_new_recipient  │                   │  (no skew)           │
│  • is_new_device     │                   └──────────────────────┘
│  • calendar features │
└──────────┬───────────┘
           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    SCORING LAYER                                         │
│                                                                         │
│  ┌────────────────────┐      ┌──────────────────────────────────┐       │
│  │   RULE ENGINE      │      │   ML SCORING (ONNX)              │       │
│  │                    │      │                                  │       │
│  │ CBN ₦5M threshold  │      │  LightGBM → fraud_model.onnx     │       │
│  │ CBN ₦10M daily     │      │  20 features → P(fraud) ∈ [0,1] │       │
│  │ Velocity >10/5min  │      │  Threshold: 0.77 (optimised)     │       │
│  │ Amount spike >10x  │      │  P95 latency: 0.1ms              │       │
│  │ Odd hours + high ₦ │      │                                  │       │
│  └─────────┬──────────┘      └──────────────┬───────────────────┘       │
│            │  Rule triggered?               │  No rule → use model      │
│            └──────────────────┬─────────────┘                           │
│                               ▼                                         │
│                    ┌──────────────────┐                                 │
│                    │ DECISION ROUTER  │                                 │
│                    │                  │                                 │
│                    │ score ≥ 0.85 → BLOCK                              │
│                    │ score ≥ 0.50 → FLAG                               │
│                    │ score < 0.50 → ALLOW                              │
│                    └──────┬───────────┘                                 │
└───────────────────────────┼─────────────────────────────────────────────┘
                            │
              ┌─────────────┼────────────────┐
              ▼             ▼                ▼
    ┌──────────────┐ ┌────────────┐ ┌──────────────┐
    │ ALERT STORE  │ │ KAFKA      │ │ MODEL METRICS│
    │ (SQLite)     │ │ fraud-     │ │ (latency,    │
    │              │ │ alerts     │ │  scores,     │
    │ FraudAlert   │ │ topic      │ │  decisions)  │
    │ audit trail  │ └──────┬─────┘ └──────────────┘
    └──────┬───────┘        │
           │                ▼
           └────────► OPS DASHBOARD
                      (FastAPI + WebSocket)
                      http://localhost:8000
```

---

## 3. Component descriptions

### 3.1 Transaction generator (`src/producer/generator.py`)

Simulates realistic Nigerian payment traffic for development and testing.
Not present in production — replaced by actual payment gateway integration.

**Legitimate patterns:**
- NIP-heavy channel mix (35% NIP, 25% CARD_POS, 20% USSD, 10% CARD_WEB, 10% MOBILE_APP)
- Lognormal amount distributions per channel (median: NIP ₦36K, CARD_POS ₦5K, USSD ₦3K)
- Salary period boost (25th-2nd: higher NIP transfer amounts)
- Airtime micro-transactions (₦100-₦5,000)

**Fraud patterns (5 types):**
- Velocity attack: rapid NIP transfers (30% weight)
- High amount anomaly: single large transaction (20%)
- Channel switch: USSD account → CARD_WEB (20%)
- Odd hours: 1AM-3AM high-value transaction (15%)
- Geographic anomaly: POS in unexpected state (15%)

### 3.2 Kafka broker (`docker-compose.yml`)

Single-node Kafka 7.6.0 via Confluent Docker image.

**Topics:**

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| raw-transactions | 3 | 7 days | Raw payment events from producer |
| enriched-transactions | 3 | 7 days | Transactions + computed features |
| fraud-alerts | 1 | 90 days | Flagged/blocked decisions (audit) |
| model-metrics | 1 | 24 hours | Scoring latency + confidence distribution |

Partition count for raw and enriched is 3 to allow up to 3 parallel
consumer instances. Alert and metrics topics use 1 partition because
ordering matters (alerts) and volume is low (metrics).

### 3.3 Feature engine (`src/features/engine.py`)

Stateful Kafka consumer that maintains per-account state in memory and
computes windowed features for each transaction.

**Key classes:**
- `SlidingWindow`: deque of (timestamp, amount) pairs, O(1) add, O(window_size) query
- `AccountState`: per-account state (window, recipients, devices, 30-day EMA)
- `FeatureEngine`: orchestrates feature computation, returns `EnrichedTransaction`

**Features computed:**

| Feature | Window | Description |
|---|---|---|
| tx_count_5m | 5 min | Transaction count — velocity signal |
| tx_count_1h | 1 hour | Transaction count — frequency signal |
| tx_count_24h | 24 hours | Transaction count — daily activity |
| total_amount_5m | 5 min | Sum amount — drain detection |
| total_amount_1h | 1 hour | Sum amount — CBN daily tracking |
| total_amount_24h | 24 hours | Sum amount — CBN daily threshold |
| avg_amount_30d | 30 days | Exponential moving average baseline |
| unique_recipients_1h | 1 hour | Distinct recipients — mule detection |
| is_new_recipient | — | First transfer to this recipient |
| is_new_device | — | First transaction on this device |
| hour_of_day | — | 0-23, extracted from timestamp |
| day_of_week | — | 0-6 (Monday=0) |
| is_salary_period | — | True on days 25-28 and 1-2 |
| is_weekend | — | True on Saturday and Sunday |

**Performance:** 1,400+ TPS on a single thread, 0 errors in testing.

**Production scaling path:** Extract `AccountState` to Redis sorted sets.
`SlidingWindow.events` → Redis ZADD/ZRANGEBYSCORE. Same feature logic,
distributed state store. No changes to feature definitions needed.

### 3.4 Training pipeline (`src/training/`)

Offline batch pipeline for model training. Runs periodically (monthly
or on drift detection), not in the live data path.

**Components:**
- `generate_dataset.py`: Creates labeled CSV using the same `FeatureEngine`
  as the live pipeline (eliminates training-serving skew). 50,000 transactions,
  10% fraud oversampling for class balance.
- `train.py`: LightGBM with threshold optimisation. Logs all runs to MLflow.
- `export.py`: Converts trained model to ONNX, verifies parity, saves optimal threshold.

**Model: LightGBM classifier**
- 20 features (see feature engine section)
- `scale_pos_weight=9` to handle class imbalance (9:1 legitimate:fraud ratio in training)
- `amount_to_avg_ratio = amount / (avg_amount_30d + 1)` — derived feature that
  explicitly captures the anomaly signal
- Decision threshold optimised on validation F1: 0.77 (vs default 0.5)
- Final metrics: F1=0.836, ROC-AUC=0.979, Precision=0.872, Recall=0.803

### 3.5 Scoring service (`src/scoring/scorer.py`)

Real-time Kafka consumer that applies rule engine then ML model to each
enriched transaction and routes decisions.

**Scoring flow:**
```
EnrichedTransaction arrives
         │
         ▼
  ┌─────────────┐
  │ Rule engine │ ──► Rule triggered? ──► Decision + reason (skip ML)
  └─────────────┘
         │ No rule
         ▼
  ┌─────────────┐
  │  ML model   │ ──► P(fraud) ──► threshold routing ──► Decision + reason
  └─────────────┘
         │
         ▼
  Publish to fraud-alerts (if FLAG or BLOCK)
```

**Performance:** P95 latency 0.1ms, stable at 3,400+ TPS consuming backlog.

### 3.6 Alert pipeline (`src/alerts/pipeline.py` + `store.py`)

Consumes from `fraud-alerts` topic and persists to SQLite with full
audit trail. Separated from scoring to decouple storage latency from
scoring latency.

**Schema:**
```sql
CREATE TABLE alerts (
    alert_id        TEXT PRIMARY KEY,
    transaction_id  TEXT NOT NULL,
    timestamp       TEXT NOT NULL,
    account_id      TEXT NOT NULL,  -- hashed, NDPA compliant
    amount          REAL NOT NULL,
    channel         TEXT NOT NULL,
    decision        TEXT NOT NULL,  -- BLOCK or FLAG
    reason          TEXT NOT NULL,  -- human-readable explanation
    ml_score        REAL,           -- null if rule triggered
    rule_triggered  TEXT,           -- rule ID if applicable
    reviewed        INTEGER DEFAULT 0,
    reviewer_notes  TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
)
```

### 3.7 Ops dashboard (`src/dashboard/app.py`)

FastAPI application serving both REST and WebSocket endpoints.

**REST endpoints:**
- `GET /` — HTML dashboard
- `GET /api/stats` — aggregate counts from SQLite
- `GET /api/alerts?limit=N&decision=BLOCK` — recent alerts
- `GET /api/live?limit=N` — in-memory live event buffer

**WebSocket:**
- `WS /ws` — pushes new alerts to connected browsers in real time
- Background Kafka consumer thread feeds `_live_events` deque (maxlen=200)
- Each connected browser polls the deque every 500ms via asyncio

---

## 4. Data flow

### 4.1 Transaction lifecycle

```
1. Generator produces RawTransaction
   → Kafka: raw-transactions

2. Feature engine consumes RawTransaction
   → Computes EnrichedTransaction (adds 14 feature fields)
   → Kafka: enriched-transactions

3. Scoring service consumes EnrichedTransaction
   → Rule engine check
   → (if no rule) ML model inference
   → Decision: ALLOW / FLAG / BLOCK
   → (if FLAG or BLOCK) Kafka: fraud-alerts

4. Alert pipeline consumes FraudAlert
   → SQLite: alerts table

5. Dashboard reads from:
   → SQLite (REST endpoints: /api/stats, /api/alerts)
   → Kafka fraud-alerts (WebSocket live feed)
```

### 4.2 Schema evolution

Each stage has its own Pydantic schema:
- `RawTransaction` (15 fields) — raw payment event
- `EnrichedTransaction(RawTransaction)` (15 + 14 = 29 fields) — adds features
- `ScoredTransaction(EnrichedTransaction)` (29 + 6 = 35 fields) — adds scoring output
- `FraudAlert` (11 fields) — slim audit record for storage

Inheritance ensures that enriching a transaction never loses raw fields.
The alert schema is deliberately slim — only what ops analysts need,
excluding model internals that would clutter the review interface.

---

## 5. Infrastructure

### 5.1 Docker Compose services

| Service | Image | Purpose | Ports |
|---|---|---|---|
| zookeeper | confluentinc/cp-zookeeper:7.6.0 | Kafka coordination | 2181 |
| kafka | confluentinc/cp-kafka:7.6.0 | Message broker | 9092, 29092 |
| kafka-init | confluentinc/cp-kafka:7.6.0 | One-shot topic creation | — |

Python services run outside Docker during development (easier debugging,
faster iteration). Docker Compose wraps them for production deployment.

### 5.2 Network topology

```
Host machine (Python services)
         │
         │ localhost:9092 (PLAINTEXT_HOST listener)
         ▼
┌────────────────────────────┐
│   Docker network           │
│                            │
│  kafka:29092 ◄──────────── │ ◄── kafka-init uses this
│  (PLAINTEXT listener)      │
│                            │
│  zookeeper:2181 ◄───────── │ ◄── kafka uses this internally
└────────────────────────────┘
```

The dual-listener configuration (`KAFKA_ADVERTISED_LISTENERS`) is what
enables both host-machine Python code and Docker-internal services to
connect to the same Kafka broker on different network paths.

---

## 6. Security and compliance

### 6.1 NDPA 2023 compliance

| Requirement | Implementation |
|---|---|
| Data minimisation | Only hashed identifiers enter the pipeline |
| No raw PII storage | Account numbers, IPs hashed before processing |
| Consent-based processing | Not applicable (fraud detection is legitimate interest) |
| Audit trail | Every alert stored with timestamp, reason, reviewed status |
| Data retention | Alerts retained 90 days per CBN requirement |

### 6.2 CBN AML/CFT compliance

| Requirement | Implementation |
|---|---|
| Single transaction CTR (≥₦5M) | Hard rule in `RuleEngine.evaluate()` |
| Daily cumulative CTR (≥₦10M) | Tracked via `total_amount_24h` feature |
| SAR filing support | Alert audit trail provides required transaction history |
| Record retention | SQLite alerts table, 90-day minimum |

### 6.3 Model security

- Models are loaded once at startup from local filesystem, not downloaded at runtime
- ONNX format prevents arbitrary code execution (vs pickle deserialization)
- No external API calls during inference
- No raw transaction data leaves the pipeline (only decisions and hashed IDs)

---

## 7. Deployment

### 7.1 Local development

```bash
# Start infrastructure
docker compose up -d zookeeper kafka
docker compose up kafka-init

# Generate data and train model (once)
python -m src.training.generate_dataset
python -m src.training.train
python -m src.training.export

# Start pipeline (5 terminals)
python -m src.producer.generator    # Terminal 1
python -m src.features.engine       # Terminal 2
python -m src.scoring.scorer        # Terminal 3
python -m src.alerts.pipeline       # Terminal 4
python -m src.dashboard.app         # Terminal 5
```

### 7.2 Production deployment path

1. Add each Python service to `docker-compose.yml` with proper healthchecks
2. Replace SQLite with PostgreSQL (connection string change only)
3. Add Redis for distributed feature state
4. Add Prometheus instrumentation to each service
5. Add Grafana for infrastructure monitoring
6. Deploy to cloud provider (GCP Cloud Run, AWS ECS, or bare-metal)
