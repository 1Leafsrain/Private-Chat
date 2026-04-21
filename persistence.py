"""
persistence.py - SQLite: Chat History + Unsent Message Queue
"""
import sqlite3
import json
import time
from typing import List, Optional
from config import DB_PATH
from utils.logger import get_logger

log = get_logger("persistence")


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Buat tabel jika belum ada."""
    with _get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS chat_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                seq         INTEGER NOT NULL,
                sender      TEXT NOT NULL,
                message     TEXT NOT NULL,
                direction   TEXT NOT NULL CHECK(direction IN ('sent','received')),
                timestamp   REAL NOT NULL,
                acked       INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS unsent_queue (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                seq         INTEGER NOT NULL UNIQUE,
                payload     TEXT NOT NULL,   -- JSON string
                retries     INTEGER NOT NULL DEFAULT 0,
                created_at  REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_history_seq ON chat_history(seq);
            CREATE INDEX IF NOT EXISTS idx_unsent_seq  ON unsent_queue(seq);
        """)
    log.info(f"Database diinisialisasi: {DB_PATH}")


# ─── Chat History ───────────────────────────────────────────────────────────

def save_message(seq: int, sender: str, message: str,
                 direction: str, timestamp: float, acked: bool = False):
    with _get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO chat_history "
            "(seq, sender, message, direction, timestamp, acked) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (seq, sender, message, direction, timestamp, int(acked))
        )


def mark_acked(seq: int):
    with _get_conn() as conn:
        conn.execute("UPDATE chat_history SET acked=1 WHERE seq=?", (seq,))
    log.debug(f"Pesan seq={seq} ditandai ACK.")


def load_history(limit: int = 50) -> List[sqlite3.Row]:
    with _get_conn() as conn:
        return conn.execute(
            "SELECT * FROM chat_history ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()


# ─── Unsent Queue ────────────────────────────────────────────────────────────

def enqueue_unsent(seq: int, payload: dict):
    with _get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO unsent_queue (seq, payload, retries, created_at) "
            "VALUES (?, ?, 0, ?)",
            (seq, json.dumps(payload), time.time())
        )
    log.debug(f"Pesan seq={seq} masuk ke unsent_queue.")


def dequeue_unsent(seq: int):
    with _get_conn() as conn:
        conn.execute("DELETE FROM unsent_queue WHERE seq=?", (seq,))
    log.debug(f"Pesan seq={seq} dihapus dari unsent_queue (berhasil terkirim).")


def increment_retry(seq: int):
    with _get_conn() as conn:
        conn.execute(
            "UPDATE unsent_queue SET retries = retries + 1 WHERE seq=?", (seq,)
        )


def get_all_unsent() -> List[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT seq, payload, retries FROM unsent_queue ORDER BY created_at"
        ).fetchall()
    return [
        {"seq": r["seq"], "payload": json.loads(r["payload"]), "retries": r["retries"]}
        for r in rows
    ]


def clear_unsent():
    with _get_conn() as conn:
        conn.execute("DELETE FROM unsent_queue")
