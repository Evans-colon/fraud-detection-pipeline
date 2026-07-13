"""
Alert store — SQLite-backed storage for flagged and blocked transactions.

Why SQLite (not PostgreSQL):
    SQLite requires zero infrastructure — no separate database container,
    no connection pooling, no credentials. For a single-node deployment
    handling hundreds of alerts per hour, SQLite is entirely sufficient.
    The upgrade path to PostgreSQL is a one-line connection string change
    plus replacing sqlite3 with psycopg2.

CBN requirement: every flagged/blocked transaction must be stored with
a complete audit trail — who flagged it, why, when, and whether it was
reviewed by a human operator.

Run standalone: python -m src.alerts.store (initialises the database)
"""
import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from config.settings import ALERT_DB_PATH
from src.producer.schemas import FraudAlert


def init_db(db_path: str = ALERT_DB_PATH) -> None:
    """
    Create the alerts database and tables if they don't exist.
    Safe to call multiple times — uses IF NOT EXISTS.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id        TEXT PRIMARY KEY,
                transaction_id  TEXT NOT NULL,
                timestamp       TEXT NOT NULL,
                account_id      TEXT NOT NULL,
                amount          REAL NOT NULL,
                channel         TEXT NOT NULL,
                decision        TEXT NOT NULL,
                reason          TEXT NOT NULL,
                ml_score        REAL,
                rule_triggered  TEXT,
                reviewed        INTEGER DEFAULT 0,
                reviewer_notes  TEXT,
                created_at      TEXT DEFAULT (datetime('now'))
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_account
            ON alerts(account_id)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_decision
            ON alerts(decision)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_timestamp
            ON alerts(timestamp)
        """)

        conn.commit()
    print(f"Alert database initialised at {db_path}")


@contextmanager
def get_connection(db_path: str = ALERT_DB_PATH):
    """Context manager for database connections."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # returns rows as dicts
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def save_alert(alert: FraudAlert, db_path: str = ALERT_DB_PATH) -> bool:
    """
    Persist a FraudAlert to the database.
    Returns True if saved, False if duplicate (idempotent).
    """
    try:
        with get_connection(db_path) as conn:
            conn.execute("""
                INSERT OR IGNORE INTO alerts (
                    alert_id, transaction_id, timestamp, account_id,
                    amount, channel, decision, reason,
                    ml_score, rule_triggered, reviewed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alert.alert_id,
                alert.transaction_id,
                alert.timestamp.isoformat(),
                alert.account_id,
                alert.amount,
                alert.channel.value,
                alert.decision.value,
                alert.reason,
                alert.ml_score,
                alert.rule_triggered,
                int(alert.reviewed),
            ))
        return True
    except Exception as exc:
        print(f"Failed to save alert {alert.alert_id}: {exc}")
        return False


def get_recent_alerts(
    limit: int = 50,
    decision: str | None = None,
    db_path: str = ALERT_DB_PATH,
) -> list[dict]:
    """
    Fetch recent alerts, optionally filtered by decision type.
    Used by the dashboard API.
    """
    with get_connection(db_path) as conn:
        if decision:
            rows = conn.execute("""
                SELECT * FROM alerts
                WHERE decision = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (decision.upper(), limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM alerts
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,)).fetchall()
    return [dict(row) for row in rows]


def get_stats(db_path: str = ALERT_DB_PATH) -> dict:
    """
    Aggregate stats for the dashboard summary panel.
    Returns counts by decision type and unreviewed count.
    """
    with get_connection(db_path) as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM alerts"
        ).fetchone()[0]

        by_decision = conn.execute("""
            SELECT decision, COUNT(*) as count
            FROM alerts
            GROUP BY decision
        """).fetchall()

        unreviewed = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE reviewed = 0"
        ).fetchone()[0]

    stats = {
        "total": total,
        "unreviewed": unreviewed,
        "by_decision": {row[0]: row[1] for row in by_decision},
    }
    return stats


if __name__ == "__main__":
    init_db()