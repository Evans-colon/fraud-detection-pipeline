# Product Requirements Document (PRD)
# Real-Time Fraud Detection Pipeline

**Document version:** 1.0
**Status:** Approved
**Author:** Evans (Data/AI Engineer)
**Date:** July 2026
**Stakeholders:** Risk & Compliance, Engineering, Product, Operations

---

## 1. Problem statement

Nigerian fintechs processing NIP transfers, card payments, and USSD
transactions face an estimated 2-5% fraud rate on unprotected payment
rails. Each fraudulent transaction represents direct financial loss to
the institution or its customers, potential regulatory sanctions from
the CBN, and reputational damage that erodes customer trust.

The status quo at most Nigerian fintechs is one of:
- Purely rule-based systems (high false positive rate, miss novel patterns)
- Manual review queues (too slow for real-time payment decisions)
- No fraud detection at all (common at early-stage fintechs)

None of these adequately balance the three competing demands of fraud
detection: **catching fraud** (recall), **not blocking legitimate
customers** (precision), and **doing so in real time** (latency).

---

## 2. Objective

Build a real-time fraud detection pipeline that:

1. Scores every transaction within 100ms of arrival
2. Catches ≥80% of fraudulent transactions (recall)
3. Generates ≤20% false positives on flagged transactions (precision ≥80%)
4. Complies with CBN AML/CFT reporting requirements automatically
5. Provides ops analysts with a live dashboard for alert review
6. Degrades gracefully under load without affecting payment flow

---

## 3. Users

### Primary users

**Fraud analysts / ops team**
- Review flagged transactions in the ops dashboard
- Mark alerts as reviewed with notes
- Escalate to compliance team for CBN reporting
- Need: fast alert triage, clear reasoning for each decision

**Risk & compliance team**
- Monitor fraud rates and model performance over time
- Prepare CBN Currency Transaction Reports (CTRs) for flagged transactions
- Need: audit trail, regulatory threshold tracking, exportable reports

### Secondary users

**Engineering team**
- Monitor pipeline health (consumer lag, latency, error rates)
- Retrain and deploy updated models
- Need: MLflow experiment tracking, clear model versioning

**Product team**
- Understand fraud impact on customer experience
- Need: false positive rates, block rates by channel/amount

---

## 4. Functional requirements

### FR-1: Transaction ingestion
- The system MUST consume transactions from all payment channels: NIP, CARD_POS, CARD_WEB, USSD, MOBILE_APP
- The system MUST process transactions in the order they are received per account
- The system MUST not drop messages during scoring service restarts (Kafka offset management)

### FR-2: Feature computation
- The system MUST compute windowed aggregations for each transaction: tx count and total amount over 5-minute, 1-hour, and 24-hour windows
- The system MUST track per-account behavioural baselines (30-day average amount)
- The system MUST flag first-time recipients and new devices per account
- The system MUST extract calendar features (hour, day, salary period, weekend)

### FR-3: CBN regulatory compliance (mandatory, non-negotiable)
- The system MUST flag any single transaction ≥ ₦5,000,000 for CTR filing
- The system MUST flag any account whose cumulative daily transactions reach ₦10,000,000
- Regulatory flags MUST be applied before ML scoring and cannot be overridden by model output
- Every flagged/blocked transaction MUST be stored with a complete audit trail

### FR-4: ML-based fraud scoring
- The system MUST score transactions using a trained model outputting P(fraud) ∈ [0.0, 1.0]
- Transactions with score ≥ 0.85 MUST be blocked automatically
- Transactions with score ∈ [0.50, 0.85) MUST be flagged for human review
- Transactions with score < 0.50 MUST be allowed through
- The model MUST be replaceable without restarting the serving infrastructure

### FR-5: Alert management
- Every flagged/blocked transaction MUST generate a FraudAlert record in persistent storage
- Alerts MUST be queryable by decision type, account, and time range
- Alerts MUST have a reviewed/unreviewed status for ops workflow tracking
- The system MUST support adding reviewer notes to each alert

### FR-6: Ops dashboard
- The dashboard MUST show aggregate stats (total alerts, by decision type, unreviewed count)
- The dashboard MUST show a live feed of incoming alerts via WebSocket
- The dashboard MUST show recent historical alerts from persistent storage
- The dashboard MUST refresh stats automatically without page reload

### FR-7: Data privacy (NDPA 2023)
- Raw account numbers MUST NOT be stored anywhere in the pipeline
- All account identifiers MUST be SHA-256 hashed before processing
- IP addresses MUST be hashed before storage
- PII fields (names, BVN, phone numbers) MUST NOT enter the pipeline

---

## 5. Non-functional requirements

| Requirement | Target | Rationale |
|---|---|---|
| End-to-end latency (P95) | < 100ms | Payment gateway timeout window |
| Scoring latency (P95) | < 15ms | Leaves buffer for Kafka overhead |
| Throughput | ≥ 500 TPS sustained | Handles Nigerian peak (salary day) |
| Availability | 99.9% uptime | Downtime = unscored transactions |
| Alert storage | 90-day retention | CBN audit requirement |
| Model accuracy | F1 ≥ 0.80 on test data | Balances precision and recall |
| False positive rate | ≤ 20% of flags | Protects customer experience |

---

## 6. Out of scope (v1.0)

The following are explicitly excluded from this version:

- **Graph-based AML analysis** (transaction network detection) — Project 3
- **Real-time model retraining** — retraining is triggered manually on drift detection
- **Multi-region deployment** — single-node architecture sufficient for v1
- **Mobile SDK integration** — assumes transactions arrive via existing gateway
- **Customer-facing notifications** — ops dashboard only, no customer alerts
- **BVN/NIN verification** — identity verification is upstream of this pipeline

---

## 7. Success metrics

| Metric | Target | Measurement |
|---|---|---|
| Fraud recall | ≥ 80% | % of known fraud transactions caught |
| False positive rate | ≤ 20% | % of flags that are legitimate transactions |
| P95 scoring latency | < 15ms | Measured in Locust load tests |
| Pipeline uptime | ≥ 99.9% | Measured over 30-day window |
| Alert review time | < 4 hours | Time from alert creation to ops review |
| CBN threshold coverage | 100% | All transactions ≥ ₦5M flagged |

---

## 8. Assumptions and constraints

**Assumptions:**
- Transactions arrive via Kafka from the payment gateway (not directly from NIBSS)
- Account identifiers are hashed upstream before entering this pipeline
- The ops team has browser access to the dashboard
- Model retraining occurs monthly or when Evidently detects significant drift

**Constraints:**
- Must run on a single Linux machine during development (no cloud dependency)
- Must use only open-source components (no proprietary ML platforms)
- Must be deployable via Docker Compose for reproducibility
- All Nigerian bank codes and channel types must match CBN definitions

---

## 9. Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Model drift (fraud patterns evolve) | High | High | Evidently monitoring, monthly retraining schedule |
| High false positive rate frustrating ops team | Medium | Medium | Threshold tuning, human review layer |
| Kafka consumer lag during peak | Low | High | 3-partition topics, horizontal consumer scaling |
| In-memory feature state lost on restart | Medium | Medium | Checkpoint state to Redis (future enhancement) |
| NDPA 2023 audit failure | Low | Critical | PII never enters pipeline by design |
