"""
tcp_handler.py - TCP Non-Blocking Server & Client
Protocol: [16-byte header] + [JSON payload]

Header layout (16 bytes total):
  [0:4]   Sequence Number  (uint32, big-endian)
  [4:12]  Timestamp        (int64,  big-endian, unix ms)
  [12]    Message Type     (uint8)
  [13:16] Payload Length   (uint24, big-endian)

Message Types:
  0   CHAT           (tidak digunakan, dipertahankan untuk kompatibilitas)
  1   ACK            Client â†” Server: konfirmasi penerimaan pesan
  2-4 (deprecated)
  5   DISCONNECT     Client â†’ Server: putus koneksi dengan baik
  6   SYSTEM         Server â†’ Client: notifikasi sistem
  7   REGISTER       Client â†’ Server: {username, password}
  8   LOGIN          Client â†’ Server: {username} atau {username, password}
  9   LOGIN_OK       Server â†’ Client: {username}
  10  LOGIN_FAIL     Server â†’ Client: {reason}
  11  ROUTE          Clientâ†’Server: {to, message, seq} | Serverâ†’Client: {from, message, seq}
  12  USER_LIST      Client â†’ Server: {} (permintaan daftar user online)
  13  USER_LIST_RESP Server â†’ Client: {users: [...]}
"""
import socket
import struct
import json
import time
import threading
from typing import Callable, List, Optional
from config import TCP_PORT, BUFFER_SIZE, RECONNECT_INTERVAL_SEC, SERVER_HOST
from security import check_ip_whitelist
from utils.logger import get_logger

log = get_logger("tcp_handler")

# â”€â”€â”€ Konstanta tipe pesan â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
MSG_CHAT           = 0
MSG_ACK            = 1
MSG_DISCONNECT     = 5
MSG_SYSTEM         = 6
MSG_REGISTER       = 7
MSG_LOGIN          = 8
MSG_LOGIN_OK       = 9
MSG_LOGIN_FAIL     = 10
MSG_ROUTE          = 11
MSG_USER_LIST      = 12
MSG_USER_LIST_RESP = 13

HEADER_SIZE = 16


def _pack_header(seq: int, msg_type: int, payload_len: int) -> bytes:
    ts_ms = int(time.time() * 1000)
    return struct.pack(">IqB", seq, ts_ms, msg_type) + \
           payload_len.to_bytes(3, "big")


def _unpack_header(data: bytes) -> tuple:
    """Return (seq, timestamp_ms, msg_type, payload_len)."""
    seq, ts_ms, msg_type = struct.unpack(">IqB", data[:13])
    payload_len = int.from_bytes(data[13:16], "big")
    return seq, ts_ms, msg_type, payload_len


def encode_msg(seq: int, msg_type: int, payload: dict) -> bytes:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return _pack_header(seq, msg_type, len(body)) + body


# Alias internal
_encode_msg = encode_msg


def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    """Baca tepat n byte dari socket; return None jika koneksi putus."""
    data = b""
    while len(data) < n:
        try:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        except (ConnectionResetError, OSError):
            return None
    return data


# â”€â”€â”€ TCPServer (multi-client) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TCPServer:
    """
    Mendengarkan koneksi masuk dari banyak client sekaligus.
    Untuk setiap koneksi baru, spawns thread dan memanggil on_new_client(conn, addr).
    """

    def __init__(self, on_new_client: Callable[[socket.socket, tuple], None]):
        self.on_new_client = on_new_client
        self._stop = threading.Event()

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((SERVER_HOST, TCP_PORT))
        self._server_sock.listen(100)
        self._server_sock.settimeout(1.0)
        log.info(f"TCP server listening di {SERVER_HOST}:{TCP_PORT}")

    def start(self):
        t = threading.Thread(target=self._accept_loop, daemon=True, name="TCPServerAccept")
        t.start()

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn, addr = self._server_sock.accept()
                peer_ip = addr[0]
                log.info(f"Koneksi masuk dari {peer_ip}:{addr[1]}")

                if not check_ip_whitelist(peer_ip):
                    conn.close()
                    continue

                t = threading.Thread(
                    target=self.on_new_client,
                    args=(conn, addr),
                    daemon=True,
                    name=f"ClientHandler-{peer_ip}"
                )
                t.start()

            except socket.timeout:
                continue
            except Exception as e:
                if not self._stop.is_set():
                    log.error(f"Accept error: {e}")

    def stop(self):
        self._stop.set()
        try:
            self._server_sock.close()
        except Exception:
            pass


# â”€â”€â”€ TCPClient â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TCPClient:
    """
    Terhubung ke server standalone, melakukan login/register, kirim/terima pesan.
    """

    def __init__(
        self,
        username:         str,
        server_ip:        str,
        on_message:       Callable[[str, str, int], None],   # (from_user, text, seq)
        on_ack:           Callable[[int], None],              # (seq)
        on_connected:     Callable[[str], None],              # (server_ip)
        on_disconnect:    Callable[[], None],
        on_auth_required: Callable[[str], None],              # (action: "need_register"|"need_password")
        on_login_ok:      Callable[[], None],
        on_login_fail:    Callable[[str], None],              # (reason)
        on_user_list:     Callable[[List[str]], None],
        on_system:        Optional[Callable[[dict], None]] = None,
    ):
        self.username         = username
        self.server_ip        = server_ip
        self.on_message       = on_message
        self.on_ack           = on_ack
        self.on_connected     = on_connected
        self.on_disconnect    = on_disconnect
        self.on_auth_required = on_auth_required
        self.on_login_ok      = on_login_ok
        self.on_login_fail    = on_login_fail
        self.on_user_list     = on_user_list
        self.on_system        = on_system

        self._sock: Optional[socket.socket] = None
        self._stop      = threading.Event()
        self._send_lock = threading.Lock()

    def start(self):
        t = threading.Thread(target=self._connect_loop, daemon=True, name="TCPClientConnect")
        t.start()

    def _connect_loop(self):
        while not self._stop.is_set():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5.0)
                sock.connect((self.server_ip, TCP_PORT))
                sock.settimeout(None)
                log.info(f"Terhubung ke {self.server_ip}:{TCP_PORT}")

                self._sock = sock
                self.on_connected(self.server_ip)

                # Kirim username saja untuk cek status (baru/lama)
                self._send_raw(MSG_LOGIN, {"username": self.username})

                # Proses semua pesan masuk
                self._recv_loop(sock)

            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                log.info(
                    f"Gagal terhubung ke {self.server_ip}: {e}. "
                    f"Reconnect dalam {RECONNECT_INTERVAL_SEC}s..."
                )
                self._sock = None
                time.sleep(RECONNECT_INTERVAL_SEC)

    def _recv_loop(self, sock: socket.socket):
        while not self._stop.is_set():
            raw_header = _recv_exact(sock, HEADER_SIZE)
            if not raw_header:
                log.warning("Koneksi ke server terputus.")
                self._sock = None
                self.on_disconnect()
                break

            seq, ts, msg_type, plen = _unpack_header(raw_header)
            raw_body = _recv_exact(sock, plen) if plen > 0 else b"{}"
            if raw_body is None:
                self._sock = None
                self.on_disconnect()
                break

            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except json.JSONDecodeError:
                log.error("Payload JSON tidak valid, diabaikan.")
                continue

            self._dispatch(seq, msg_type, payload)

    def _dispatch(self, seq: int, msg_type: int, payload: dict):
        """Proses pesan masuk berdasarkan tipe."""
        if msg_type == MSG_ROUTE:
            from_user = payload.get("from", "unknown")
            message   = payload.get("message", "")
            msg_seq   = payload.get("seq", seq)
            self.on_message(from_user, message, msg_seq)
            self._send_ack(msg_seq)

        elif msg_type == MSG_ACK:
            acked_seq = payload.get("acked_seq", seq)
            self.on_ack(acked_seq)

        elif msg_type == MSG_LOGIN_OK:
            self.on_login_ok()

        elif msg_type == MSG_LOGIN_FAIL:
            reason = payload.get("reason", "unknown")
            self.on_login_fail(reason)

        elif msg_type == MSG_SYSTEM:
            action = payload.get("action", "")
            if action in ("need_register", "need_password"):
                self.on_auth_required(action)
            elif self.on_system:
                self.on_system(payload)

        elif msg_type == MSG_USER_LIST_RESP:
            users = payload.get("users", [])
            self.on_user_list(users)

        elif msg_type == MSG_DISCONNECT:
            log.info("Server mengirim DISCONNECT.")
            self.on_disconnect()

    def _send_raw(self, msg_type: int, payload: dict, seq: int = 0):
        if self._sock:
            try:
                with self._send_lock:
                    self._sock.sendall(encode_msg(seq, msg_type, payload))
            except Exception as e:
                log.error(f"Gagal kirim (type={msg_type}): {e}")
                raise

    def _send_ack(self, seq: int):
        self._send_raw(MSG_ACK, {"acked_seq": seq})

    def send_login(self, username: str, password: str):
        """Kirim kredensial login ke server."""
        self._send_raw(MSG_LOGIN, {"username": username, "password": password})

    def send_register(self, username: str, password: str):
        """Kirim permintaan registrasi ke server."""
        self._send_raw(MSG_REGISTER, {"username": username, "password": password})

    def send(self, payload: dict):
        """Kirim pesan chat ke server via MSG_ROUTE."""
        seq = payload.get("_seq", 0)
        self._send_raw(MSG_ROUTE, {
            "to":      payload.get("to", ""),
            "message": payload.get("message", ""),
            "seq":     seq,
        }, seq)

    def request_user_list(self):
        """Minta daftar user online dari server."""
        self._send_raw(MSG_USER_LIST, {})

    def stop(self):
        self._stop.set()
        if self._sock:
            try:
                self._send_raw(MSG_DISCONNECT, {"sender": self.username})
                self._sock.close()
            except Exception:
                pass

