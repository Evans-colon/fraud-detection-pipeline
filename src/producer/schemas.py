"""
Transaction schemas modeled on Nigerian payment rails (NIP/NIBSS).

Pydantic enforces types at every boundary — producer, consumer,
feature engine, scoring — so malformed data fails loudly at
ingestion, not silently inside the model.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class Channel(str, Enum):
    NIP = "NIP"
    CARD_POS = "CARD_POS"
    CARD_WEB = "CARD_WEB"
    USSD = "USSD"
    MOBILE_APP = "MOBILE_APP"


class TransactionType(str, Enum):
    TRANSFER = "TRANSFER"
    PAYMENT = "PAYMENT"
    WITHDRAWAL = "WITHDRAWAL"
    AIRTIME = "AIRTIME"
    BILL_PAYMENT = "BILL_PAYMENT"


class Decision(str, Enum):
    ALLOW = "ALLOW"
    FLAG = "FLAG"
    BLOCK ="BLOCK"

class RawTransaction(BaseModel):
    """A single payment event as it arrives from the payment gateway."""
    transaction_id: str
    timestamp: datetime
    account_id: str
    recipient_id: Optional[str] = None
    amount: float= Field(..., gt=0)
    channel: Channel
    transaction_type: TransactionType
    merchant_category_code: Optional[str] = None
    sender_bank_code: str
    recipient_bank_code: Optional[str] = None
    device_id: Optional[str] = None
    ip_hash: Optional[str] = None
    location_city: Optional[str] = None
    location_state: Optional[str] = None
    is_international: bool = False


class EnrichedTransaction(RawTransaction):
    """Transaction with computed features attached."""
    tx_count_5m: int = 0
    tx_count_1h: int = 0
    tx_count_24h: int = 0
    total_amount_5m: float = 0.0
    total_amount_1h: float = 0.0
    total_amount_24h: float = 0.0
    avg_amount_30d: float = 0.0
    unique_recipients_1h: int = 0
    is_new_recipient: bool = False
    is_new_device: bool = False
    hour_of_day: int = 0
    day_of_week: int = 0
    is_salary_period: bool = False
    is_weekend: bool = False


class ScoredTransaction(RawTransaction):
    """Transaction after fraud scoring"""
    rule_triggered: Optional[str] = None
    rule_action: Optional[str] = None
    ml_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    final_decision: Decision = Decision.ALLOW
    decision_reason: str = ""
    scoring_latency_ms: float = 0.0


class FraudAlert(BaseModel):
    """Alert record for flagged/blocked transactions."""
    alert_id: str
    transaction_id: str
    timestamp: datetime
    account_id: str
    amount: float
    channel: Channel
    decision: Decision
    reason: str
    ml_score: Optional[float] = None
    rule_triggered: Optional[str] = None
    reviewed: bool = False


