"""
tcp_handler.py - TCP Non-Blocking Server & Client
Protocol: [16-byte header] + [JSON payload]

Header layout (16 bytes total):
  [0:4]   Sequence Number  (uint32, big-endian)
  [4:12]  Timestamp        (int64,  big-endian, unix ms)
  [12]    Message Type     (uint8: 0=CHAT, 1=ACK, 2=AUTH, 3=AUTH_OK, 4=AUTH_FAIL, 5=DISCONNECT)
  [13:16] Payload Length   (uint24, big-endian)
"""
import socket
import struct
import json
import time
import threading
from typing import Callable, Optional
from config import TCP_PORT, BUFFER_SIZE, RECONNECT_INTERVAL_SEC
from security import check_ip_whitelist, verify_password, get_password_hash
from utils.logger import get_logger

log = get_logger("tcp_handler")

# ─── Konstanta tipe pesan ─────────────────────────────────────────────────
MSG_CHAT        = 0
MSG_ACK         = 1
MSG_AUTH        = 2
MSG_AUTH_OK     = 3
MSG_AUTH_FAIL   = 4
MSG_DISCONNECT  = 5
MSG_SYSTEM      = 6

HEADER_SIZE = 16


def _pack_header(seq: int, msg_type: int, payload_len: int) -> bytes:
    ts_ms = int(time.time() * 1000)
    # >I = uint32, >q = int64, B = uint8, 3s = 3 bytes untuk payload length
    return struct.pack(">IqB", seq, ts_ms, msg_type) + \
           payload_len.to_bytes(3, "big")


def _unpack_header(data: bytes) -> tuple:
    """Return (seq, timestamp_ms, msg_type, payload_len)."""
    seq, ts_ms, msg_type = struct.unpack(">IqB", data[:13])
    payload_len = int.from_bytes(data[13:16], "big")
    return seq, ts_ms, msg_type, payload_len


def _encode_msg(seq: int, msg_type: int, payload: dict) -> bytes:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return _pack_header(seq, msg_type, len(body)) + body


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


# ─── TCPServer ────────────────────────────────────────────────────────────

class TCPServer:
    """
    Mendengarkan koneksi masuk (satu client), mengautentikasi,
    lalu meneruskan pesan ke callback.
    """

    def __init__(
        self,
        username: str,
        on_message:    Callable[[str, str, int], None],   # (sender, text, seq)
        on_ack:        Callable[[int], None],              # (seq)
        on_connected:  Callable[[str], None],              # (peer_ip)
        on_disconnect: Callable[[], None],
    ):
        self.username       = username
        self.on_message     = on_message
        self.on_ack         = on_ack
        self.on_connected   = on_connected
        self.on_disconnect  = on_disconnect
        self._client_sock: Optional[socket.socket] = None
        self._stop          = threading.Event()
        self._send_lock     = threading.Lock()

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind(("0.0.0.0", TCP_PORT))
        self._server_sock.listen(1)
        self._server_sock.settimeout(1.0)
        log.info(f"TCP server listening di port {TCP_PORT}")

    def start(self):
        t = threading.Thread(target=self._accept_loop, daemon=True, name="TCPServerAccept")
        t.start()

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn, addr = self._server_sock.accept()
                peer_ip = addr[0]
                log.info(f"Koneksi masuk dari {peer_ip}")

                if not check_ip_whitelist(peer_ip):
                    conn.close()
                    continue

                # Autentikasi
                if not self._authenticate(conn):
                    conn.close()
                    continue

                self._client_sock = conn
                self.on_connected(peer_ip)
                self._recv_loop(conn, peer_ip)

            except socket.timeout:
                continue
            except Exception as e:
                if not self._stop.is_set():
                    log.error(f"Accept error: {e}")

    def _authenticate(self, conn: socket.socket) -> bool:
        """Terima pesan AUTH dari client, verifikasi password."""
        raw_header = _recv_exact(conn, HEADER_SIZE)
        if not raw_header:
            return False
        seq, ts, msg_type, plen = _unpack_header(raw_header)
        if msg_type != MSG_AUTH:
            return False
        raw_body = _recv_exact(conn, plen)
        if not raw_body:
            return False
        payload = json.loads(raw_body.decode("utf-8"))
        candidate_hash = payload.get("password_hash", "")
        expected_hash  = get_password_hash()

        if candidate_hash == expected_hash:
            ack_msg = _encode_msg(seq, MSG_AUTH_OK, {"status": "ok", "server": "SecureChat"})
            conn.sendall(ack_msg)
            log.info("Autentikasi berhasil.")
            return True
        else:
            fail_msg = _encode_msg(seq, MSG_AUTH_FAIL, {"status": "fail"})
            conn.sendall(fail_msg)
            log.warning("Autentikasi GAGAL.")
            return False

    def _recv_loop(self, conn: socket.socket, peer_ip: str):
        while not self._stop.is_set():
            raw_header = _recv_exact(conn, HEADER_SIZE)
            if not raw_header:
                log.warning(f"Koneksi terputus dari {peer_ip}")
                self.on_disconnect()
                break
            seq, ts, msg_type, plen = _unpack_header(raw_header)
            raw_body = _recv_exact(conn, plen) if plen > 0 else b"{}"
            if raw_body is None:
                self.on_disconnect()
                break
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except json.JSONDecodeError:
                log.error("Payload JSON tidak valid, pesan diabaikan.")
                continue

            if msg_type == MSG_CHAT:
                sender  = payload.get("sender", "unknown")
                message = payload.get("message", "")
                self.on_message(sender, message, seq)
                self._send_ack(seq)

            elif msg_type == MSG_ACK:
                acked_seq = payload.get("acked_seq", seq)
                self.on_ack(acked_seq)

            elif msg_type == MSG_DISCONNECT:
                log.info(f"Peer {peer_ip} mengirim DISCONNECT.")
                self.on_disconnect()
                break

    def _send_ack(self, seq: int):
        if self._client_sock:
            try:
                with self._send_lock:
                    data = _encode_msg(0, MSG_ACK, {"acked_seq": seq})
                    self._client_sock.sendall(data)
                log.debug(f"ACK terkirim untuk seq={seq}")
            except Exception as e:
                log.error(f"Gagal kirim ACK: {e}")

    def send(self, payload: dict):
        """Kirim pesan via koneksi aktif."""
        if self._client_sock:
            try:
                with self._send_lock:
                    seq = payload.get("_seq", 0)
                    data = _encode_msg(seq, MSG_CHAT, payload)
                    self._client_sock.sendall(data)
            except Exception as e:
                log.error(f"Gagal kirim pesan: {e}")
                raise

    def stop(self):
        self._stop.set()
        if self._client_sock:
            try:
                self._client_sock.sendall(
                    _encode_msg(0, MSG_DISCONNECT, {"sender": self.username})
                )
                self._client_sock.close()
            except Exception:
                pass
        self._server_sock.close()


# ─── TCPClient ────────────────────────────────────────────────────────────

class TCPClient:
    """
    Terhubung ke TCPServer, melakukan autentikasi, kirim/terima pesan.
    """

    def __init__(
        self,
        username:       str,
        server_ip:      str,
        on_message:     Callable[[str, str, int], None],
        on_ack:         Callable[[int], None],
        on_connected:   Callable[[str], None],
        on_disconnect:  Callable[[], None],
    ):
        self.username       = username
        self.server_ip      = server_ip
        self.on_message     = on_message
        self.on_ack         = on_ack
        self.on_connected   = on_connected
        self.on_disconnect  = on_disconnect
        self._sock: Optional[socket.socket] = None
        self._stop          = threading.Event()
        self._send_lock     = threading.Lock()
        self._connected     = threading.Event()

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

                # Autentikasi
                if not self._do_auth(sock):
                    sock.close()
                    log.warning("Autentikasi gagal. Coba lagi...")
                    time.sleep(RECONNECT_INTERVAL_SEC)
                    continue

                self._sock = sock
                self._connected.set()
                self.on_connected(self.server_ip)
                self._recv_loop(sock)

            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                log.info(f"Gagal terhubung ke {self.server_ip}: {e}. "
                         f"Reconnect dalam {RECONNECT_INTERVAL_SEC}s...")
                self._connected.clear()
                time.sleep(RECONNECT_INTERVAL_SEC)

    def _do_auth(self, sock: socket.socket) -> bool:
        """Kirim password hash, tunggu AUTH_OK."""
        auth_payload = {
            "sender":        self.username,
            "password_hash": get_password_hash()
        }
        data = _encode_msg(1, MSG_AUTH, auth_payload)
        sock.sendall(data)

        raw_header = _recv_exact(sock, HEADER_SIZE)
        if not raw_header:
            return False
        _, _, msg_type, plen = _unpack_header(raw_header)
        raw_body = _recv_exact(sock, plen) if plen > 0 else b"{}"
        return msg_type == MSG_AUTH_OK

    def _recv_loop(self, sock: socket.socket):
        while not self._stop.is_set():
            raw_header = _recv_exact(sock, HEADER_SIZE)
            if not raw_header:
                log.warning("Koneksi ke server terputus.")
                self._connected.clear()
                self.on_disconnect()
                break
            seq, ts, msg_type, plen = _unpack_header(raw_header)
            raw_body = _recv_exact(sock, plen) if plen > 0 else b"{}"
            if raw_body is None:
                self._connected.clear()
                self.on_disconnect()
                break
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except json.JSONDecodeError:
                continue

            if msg_type == MSG_CHAT:
                sender  = payload.get("sender", "unknown")
                message = payload.get("message", "")
                self.on_message(sender, message, seq)
                self._send_ack(seq)

            elif msg_type == MSG_ACK:
                acked_seq = payload.get("acked_seq", seq)
                self.on_ack(acked_seq)

            elif msg_type == MSG_DISCONNECT:
                log.info("Server mengirim DISCONNECT.")
                self._connected.clear()
                self.on_disconnect()
                break

    def _send_ack(self, seq: int):
        if self._sock:
            try:
                with self._send_lock:
                    data = _encode_msg(0, MSG_ACK, {"acked_seq": seq})
                    self._sock.sendall(data)
            except Exception as e:
                log.error(f"Gagal kirim ACK: {e}")

    def send(self, payload: dict):
        if self._sock and self._connected.is_set():
            try:
                with self._send_lock:
                    seq = payload.get("_seq", 0)
                    data = _encode_msg(seq, MSG_CHAT, payload)
                    self._sock.sendall(data)
            except Exception as e:
                log.error(f"Gagal kirim pesan: {e}")
                raise

    def wait_connected(self, timeout: float = 30.0) -> bool:
        return self._connected.wait(timeout=timeout)

    def stop(self):
        self._stop.set()
        if self._sock:
            try:
                self._sock.sendall(
                    _encode_msg(0, MSG_DISCONNECT, {"sender": self.username})
                )
                self._sock.close()
            except Exception:
                pass
