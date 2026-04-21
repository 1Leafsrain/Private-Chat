"""
udp_handler.py - UDP Heartbeat (Keep-Alive) + Async Logging
Dikirim setiap HEARTBEAT_INTERVAL_SEC.
Jika HEARTBEAT_MISS_LIMIT heartbeat berturut-turut tidak diterima → disconnect.
"""
import socket
import json
import time
import threading
from typing import Callable, Optional
from config import (
    UDP_PORT, BUFFER_SIZE,
    HEARTBEAT_INTERVAL_SEC, HEARTBEAT_MISS_LIMIT
)
from utils.logger import get_logger

log = get_logger("udp_handler")


class UDPHeartbeat:
    """
    Mengelola heartbeat dua arah via UDP.
    - Sender: kirim heartbeat periodik ke peer
    - Receiver: dengarkan heartbeat dari peer, deteksi disconnect
    """

    def __init__(
        self,
        username:    str,
        peer_ip:     str,
        on_timeout:  Callable[[], None],    # dipanggil saat peer tidak responsif
        on_restored: Callable[[], None],    # dipanggil saat peer kembali hidup
    ):
        self.username    = username
        self.peer_ip     = peer_ip
        self.on_timeout  = on_timeout
        self.on_restored = on_restored

        self._seq         = 0
        self._last_recv   = time.monotonic()
        self._peer_alive  = True
        self._stop        = threading.Event()

        # Socket untuk kirim
        self._send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Socket untuk terima
        self._recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._recv_sock.bind(("0.0.0.0", UDP_PORT))
        self._recv_sock.settimeout(1.0)

        log.info(f"UDP Heartbeat diinisialisasi (peer={peer_ip}, port={UDP_PORT})")

    def start(self):
        threading.Thread(
            target=self._sender_loop, daemon=True, name="UDPSender"
        ).start()
        threading.Thread(
            target=self._receiver_loop, daemon=True, name="UDPReceiver"
        ).start()
        threading.Thread(
            target=self._watchdog_loop, daemon=True, name="UDPWatchdog"
        ).start()

    def _sender_loop(self):
        while not self._stop.is_set():
            self._seq += 1
            payload = json.dumps({
                "type":      "heartbeat",
                "sender":    self.username,
                "sender_ip": self._get_local_ip(),
                "timestamp": int(time.time()),
                "seq":       self._seq
            }).encode("utf-8")
            try:
                self._send_sock.sendto(payload, (self.peer_ip, UDP_PORT))
                log.debug(f"Heartbeat #{self._seq} dikirim ke {self.peer_ip}")
            except Exception as e:
                log.warning(f"Gagal kirim heartbeat: {e}")
            time.sleep(HEARTBEAT_INTERVAL_SEC)

    def _receiver_loop(self):
        while not self._stop.is_set():
            try:
                data, addr = self._recv_sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode("utf-8"))
                if msg.get("type") == "heartbeat":
                    self._last_recv = time.monotonic()
                    log.debug(f"Heartbeat diterima dari {addr[0]} seq={msg.get('seq')}")
                    if not self._peer_alive:
                        self._peer_alive = True
                        log.info(f"Koneksi ke {addr[0]} pulih.")
                        self.on_restored()
            except socket.timeout:
                continue
            except (json.JSONDecodeError, Exception) as e:
                log.debug(f"UDP recv error: {e}")

    def _watchdog_loop(self):
        """Cek apakah heartbeat dari peer masih dalam batas toleransi."""
        deadline = HEARTBEAT_INTERVAL_SEC * HEARTBEAT_MISS_LIMIT
        while not self._stop.is_set():
            time.sleep(HEARTBEAT_INTERVAL_SEC)
            elapsed = time.monotonic() - self._last_recv
            if elapsed > deadline and self._peer_alive:
                self._peer_alive = False
                log.warning(
                    f"Heartbeat dari peer tidak diterima selama {elapsed:.1f}s "
                    f"(limit: {deadline}s) → dianggap disconnect."
                )
                self.on_timeout()

    def _get_local_ip(self) -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    def reset_timer(self):
        """Reset timer saat koneksi TCP baru terbentuk."""
        self._last_recv = time.monotonic()
        self._peer_alive = True

    def stop(self):
        self._stop.set()
        self._send_sock.close()
        self._recv_sock.close()
