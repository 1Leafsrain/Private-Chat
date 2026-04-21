"""
reliability.py - Sequence Number, ACK Tracker, Timeout, Retransmission
Implements exponential backoff untuk retransmisi.
"""
import time
import threading
from typing import Callable, Dict, Optional
from config import ACK_TIMEOUT_SEC, MAX_RETRIES
from utils.logger import get_logger

log = get_logger("reliability")


class PendingMessage:
    """Merepresentasikan satu pesan yang menunggu ACK."""

    def __init__(self, seq: int, payload: dict, send_fn: Callable):
        self.seq         = seq
        self.payload     = payload
        self.send_fn     = send_fn          # callable(payload)
        self.retries     = 0
        self.sent_at     = time.monotonic()
        self.next_check  = time.monotonic() + ACK_TIMEOUT_SEC

    def timeout_reached(self) -> bool:
        return time.monotonic() >= self.next_check

    def backoff_interval(self) -> float:
        """Exponential backoff: 3s, 6s, 12s, 24s, 48s (capped di 60s)."""
        return min(ACK_TIMEOUT_SEC * (2 ** self.retries), 60.0)

    def reschedule(self):
        interval = self.backoff_interval()
        self.next_check = time.monotonic() + interval
        self.retries += 1
        log.debug(f"Retry {self.retries}/{MAX_RETRIES} untuk seq={self.seq}, "
                  f"next check dalam {interval:.1f}s")


class ReliabilityLayer:
    """
    Melacak pesan yang menunggu ACK dan menangani retransmisi otomatis.
    """

    def __init__(self, on_give_up: Optional[Callable[[int], None]] = None):
        """
        on_give_up(seq): dipanggil ketika pesan melebihi MAX_RETRIES.
        """
        self._pending: Dict[int, PendingMessage] = {}
        self._lock    = threading.Lock()
        self._seq_counter = 0
        self._on_give_up  = on_give_up
        self._stop_event  = threading.Event()
        self._thread  = threading.Thread(
            target=self._watchdog_loop, daemon=True, name="ReliabilityWatchdog"
        )
        self._thread.start()
        log.info("ReliabilityLayer aktif.")

    def next_seq(self) -> int:
        """Atomically increment dan return sequence number berikutnya."""
        with self._lock:
            self._seq_counter += 1
            return self._seq_counter

    def track(self, seq: int, payload: dict, send_fn: Callable):
        """Mulai melacak pesan; jika tidak di-ACK → retransmit."""
        with self._lock:
            self._pending[seq] = PendingMessage(seq, payload, send_fn)
        log.debug(f"Tracking seq={seq}")

    def ack(self, seq: int):
        """Tandai pesan seq sebagai sudah di-ACK → hentikan tracking."""
        with self._lock:
            if seq in self._pending:
                del self._pending[seq]
                log.debug(f"ACK diterima untuk seq={seq}")
            else:
                log.debug(f"ACK untuk seq={seq} tidak ditemukan (mungkin sudah selesai).")

    def _watchdog_loop(self):
        """Berjalan di background thread; cek timeout setiap 0.5 detik."""
        while not self._stop_event.is_set():
            time.sleep(0.5)
            with self._lock:
                to_retry  = []
                to_remove = []
                for seq, msg in self._pending.items():
                    if msg.timeout_reached():
                        if msg.retries >= MAX_RETRIES:
                            log.error(
                                f"Pesan seq={seq} melebihi MAX_RETRIES "
                                f"({MAX_RETRIES}x). Menyerah."
                            )
                            to_remove.append(seq)
                        else:
                            to_retry.append(msg)

                for seq in to_remove:
                    del self._pending[seq]
                    if self._on_give_up:
                        self._on_give_up(seq)

            # Kirim ulang di luar lock untuk menghindari deadlock
            for msg in to_retry:
                try:
                    log.info(f"Retransmit seq={msg.seq} (percobaan ke-{msg.retries + 1})")
                    msg.send_fn(msg.payload)
                    msg.reschedule()
                except Exception as e:
                    log.error(f"Gagal retransmit seq={msg.seq}: {e}")
                    msg.reschedule()

    def stop(self):
        self._stop_event.set()
        self._thread.join(timeout=2)
        log.info("ReliabilityLayer dihentikan.")

    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)
