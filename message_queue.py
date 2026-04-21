"""
message_queue.py - Internal non-blocking message queue
Decouples user interface dari network layer.
"""
import queue
import threading
from typing import Optional
from utils.logger import get_logger

log = get_logger("message_queue")


class MessageQueue:
    """
    Thread-safe queue dengan dukungan prioritas untuk retry messages.
    Priority 0 = normal, 1 = retry (lebih tinggi didahulukan).
    """

    def __init__(self, maxsize: int = 1000):
        # PriorityQueue: (priority, sequence, payload)
        self._q: queue.PriorityQueue = queue.PriorityQueue(maxsize=maxsize)
        self._lock = threading.Lock()
        log.info("MessageQueue diinisialisasi.")

    def put_message(self, payload: dict, seq: int, priority: int = 0):
        """
        Masukkan pesan ke queue.
        priority=0 → pesan baru, priority=-1 → retry (lebih urgent).
        """
        try:
            self._q.put_nowait((priority, seq, payload))
            log.debug(f"Enqueued seq={seq} priority={priority}")
        except queue.Full:
            log.warning(f"Queue penuh! Pesan seq={seq} dibuang.")

    def put_retry(self, payload: dict, seq: int):
        """Masukkan ulang pesan yang gagal dengan prioritas lebih tinggi."""
        self.put_message(payload, seq, priority=-1)

    def get_message(self, timeout: float = 0.1) -> Optional[tuple]:
        """
        Ambil pesan berikutnya. Return (priority, seq, payload) atau None jika kosong.
        """
        try:
            return self._q.get(timeout=timeout)
        except queue.Empty:
            return None

    def task_done(self):
        self._q.task_done()

    def qsize(self) -> int:
        return self._q.qsize()

    def empty(self) -> bool:
        return self._q.empty()
