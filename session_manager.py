"""
session_manager.py - Manajemen sesi client yang terkoneksi ke server.
"""
import socket
import threading
from typing import Dict, List, Optional
from tcp_handler import encode_msg, MSG_SYSTEM, MSG_DISCONNECT
from utils.logger import get_logger

log = get_logger("session_manager")


class ClientSession:
    """Mewakili satu client yang sedang terkoneksi dan terautentikasi."""

    def __init__(self, username: str, conn: socket.socket, addr: tuple):
        self.username = username
        self.conn     = conn
        self.addr     = addr
        self._lock    = threading.Lock()

    def send(self, msg_type: int, payload: dict, seq: int = 0):
        """Kirim pesan ke client ini secara thread-safe."""
        try:
            data = encode_msg(seq, msg_type, payload)
            with self._lock:
                self.conn.sendall(data)
        except Exception as e:
            log.error(f"Gagal kirim ke '{self.username}' ({self.addr[0]}): {e}")
            raise

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


class SessionManager:
    """Melacak semua client yang sedang login dan terhubung."""

    def __init__(self):
        self._sessions: Dict[str, ClientSession] = {}
        self._lock = threading.Lock()

    def login(self, username: str, conn: socket.socket, addr: tuple) -> ClientSession:
        """Daftarkan session baru untuk user. Tutup sesi lama jika sudah ada."""
        session = ClientSession(username, conn, addr)
        with self._lock:
            old = self._sessions.get(username)
            if old:
                log.info(f"User '{username}' login dari tempat baru, sesi lama ditutup.")
                try:
                    old.send(MSG_SYSTEM, {"message": "Anda login dari perangkat lain. Sesi ini ditutup."})
                    old.close()
                except Exception:
                    pass
            self._sessions[username] = session
        log.info(f"User '{username}' ({addr[0]}) login. Total online: {len(self._sessions)}")
        return session

    def logout(self, username: str):
        """Hapus session user dari daftar aktif."""
        with self._lock:
            self._sessions.pop(username, None)
        log.info(f"User '{username}' logout. Total online: {len(self._sessions)}")

    def get_session(self, username: str) -> Optional[ClientSession]:
        """Ambil session aktif untuk user tertentu, atau None jika offline."""
        with self._lock:
            return self._sessions.get(username)

    def list_online(self) -> List[str]:
        """Kembalikan daftar username yang sedang online."""
        with self._lock:
            return list(self._sessions.keys())

    def broadcast_system(self, message: str, exclude: str = ""):
        """Kirim notifikasi sistem ke semua client online (opsional: kecuali satu user)."""
        with self._lock:
            sessions = list(self._sessions.values())
        for s in sessions:
            if s.username != exclude:
                try:
                    s.send(MSG_SYSTEM, {"message": message})
                except Exception:
                    pass
