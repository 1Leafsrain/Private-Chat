"""
persistence.py - SQLite: Chat History, Unsent Queue, Users, Offline Messages
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
    """Buat semua tabel jika belum ada."""
    with _get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                username      TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                salt          TEXT NOT NULL,
                created_at    REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS chat_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                seq         INTEGER NOT NULL,
                sender      TEXT NOT NULL,
                recipient   TEXT NOT NULL DEFAULT '',
                message     TEXT NOT NULL,
                direction   TEXT NOT NULL CHECK(direction IN ('sent','received')),
                timestamp   REAL NOT NULL,
                acked       INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS unsent_queue (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                seq         INTEGER NOT NULL UNIQUE,
                payload     TEXT NOT NULL,
                retries     INTEGER NOT NULL DEFAULT 0,
                created_at  REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS offline_messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                seq         INTEGER NOT NULL,
                sender      TEXT NOT NULL,
                recipient   TEXT NOT NULL,
                message     TEXT NOT NULL,
                timestamp   REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_history_seq    ON chat_history(seq);
            CREATE INDEX IF NOT EXISTS idx_unsent_seq     ON unsent_queue(seq);
            CREATE INDEX IF NOT EXISTS idx_offline_recip  ON offline_messages(recipient);
        """)
    log.info(f"Database diinisialisasi: {DB_PATH}")


# ─── User Management ────────────────────────────────────────────────────────

def register_user(username: str, password_hash: str, salt: str) -> bool:
    """Simpan user baru ke DB. Return False jika username sudah ada."""
    try:
        with _get_conn() as conn:
            conn.execute(
                "INSERT INTO users (username, password_hash, salt, created_at) "
                "VALUES (?, ?, ?, ?)",
                (username, password_hash, salt, time.time())
            )
        log.info(f"User '{username}' berhasil didaftarkan.")
        return True
    except sqlite3.IntegrityError:
        log.warning(f"Username '{username}' sudah terdaftar.")
        return False


def get_user(username: str) -> Optional[sqlite3.Row]:
    """Ambil data user dari DB. Return None jika tidak ditemukan."""
    with _get_conn() as conn:
        return conn.execute(
            "SELECT * FROM users WHERE username = ?", (username,)
        ).fetchone()


# ─── Offline Messages (server-side buffer) ──────────────────────────────────

def save_offline_message(seq: int, sender: str, recipient: str, message: str):
    """Simpan pesan untuk user yang sedang offline."""
    with _get_conn() as conn:
        conn.execute(
            "INSERT INTO offline_messages (seq, sender, recipient, message, timestamp) "
            "VALUES (?, ?, ?, ?, ?)",
            (seq, sender, recipient, message, time.time())
        )
    log.debug(f"Offline message dari '{sender}' untuk '{recipient}' disimpan (seq={seq}).")


def get_offline_messages(recipient: str) -> List[dict]:
    """Ambil semua pesan offline untuk user tertentu."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT id, seq, sender, message, timestamp FROM offline_messages "
            "WHERE recipient = ? ORDER BY timestamp",
            (recipient,)
        ).fetchall()
    return [dict(r) for r in rows]


def clear_offline_messages(recipient: str):
    """Hapus semua pesan offline yang sudah dikirim ke user."""
    with _get_conn() as conn:
        conn.execute("DELETE FROM offline_messages WHERE recipient = ?", (recipient,))
    log.debug(f"Offline messages untuk '{recipient}' dihapus.")


# ─── Chat History ────────────────────────────────────────────────────────────

def save_message(seq: int, sender: str, message: str,
                 direction: str, timestamp: float, acked: bool = False,
                 recipient: str = ""):
    with _get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO chat_history "
            "(seq, sender, recipient, message, direction, timestamp, acked) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (seq, sender, recipient, message, direction, timestamp, int(acked))
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


# ─── Unsent Queue (client-side) ──────────────────────────────────────────────

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
