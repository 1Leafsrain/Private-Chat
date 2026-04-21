"""
server.py - Standalone Chat Server
===================================
Server berdiri sendiri, tidak milik siapapun.
User A dan B cukup connect ke IP server ini untuk chat satu sama lain.

Cara menjalankan:
  python server.py

Opsional: ubah port di config.py (TCP_PORT, default 5555).
"""
import sys
import os
import json
import hmac
import socket
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import APP_NAME, TCP_PORT
from persistence import init_db, get_user, register_user
from security import check_ip_whitelist, generate_salt, hash_password, verify_password
from tcp_handler import (
    TCPServer, HEADER_SIZE,
    MSG_LOGIN, MSG_REGISTER, MSG_ROUTE, MSG_USER_LIST,
    MSG_LOGIN_OK, MSG_LOGIN_FAIL, MSG_SYSTEM, MSG_USER_LIST_RESP,
    MSG_DISCONNECT, MSG_ACK,
    _recv_exact, _unpack_header, encode_msg,
)
from session_manager import SessionManager, ClientSession
from message_router import MessageRouter
from utils.logger import get_logger

log = get_logger("server")

# ─── Singleton state ──────────────────────────────────────────────────────
session_mgr = SessionManager()
msg_router  = MessageRouter(session_mgr)


# ─── Per-client handler ───────────────────────────────────────────────────

def handle_new_client(conn: socket.socket, addr: tuple):
    """
    Dipanggil dalam thread tersendiri untuk setiap koneksi baru.
    Tahap 1: autentikasi (login atau register).
    Tahap 2: loop pesan (route, user_list, disconnect).
    """
    peer_ip  = addr[0]
    username = None
    session: ClientSession = None
    send_lock = threading.Lock()

    def send(msg_type: int, payload: dict, seq: int = 0):
        data = encode_msg(seq, msg_type, payload)
        with send_lock:
            conn.sendall(data)

    try:
        # ── Tahap 1: Baca pesan pertama (MSG_LOGIN) ────────────────────
        raw_header = _recv_exact(conn, HEADER_SIZE)
        if not raw_header:
            return
        seq, ts, msg_type, plen = _unpack_header(raw_header)
        raw_body = _recv_exact(conn, plen) if plen > 0 else b"{}"
        if raw_body is None:
            return
        payload = json.loads(raw_body.decode("utf-8"))

        if msg_type != MSG_LOGIN:
            log.warning(f"Pesan pertama dari {peer_ip} bukan LOGIN (type={msg_type}). Ditolak.")
            conn.close()
            return

        username = payload.get("username", "").strip()
        password = payload.get("password", "")

        if not username:
            send(MSG_LOGIN_FAIL, {"reason": "Username tidak boleh kosong."})
            conn.close()
            return

        # Jika client hanya kirim username (belum ada password), beritahu status
        if not password:
            existing = get_user(username)
            if existing:
                send(MSG_SYSTEM, {"action": "need_password"})
            else:
                send(MSG_SYSTEM, {"action": "need_register"})

            # Tunggu respons berikutnya: MSG_LOGIN dengan password atau MSG_REGISTER
            raw_header2 = _recv_exact(conn, HEADER_SIZE)
            if not raw_header2:
                return
            seq2, ts2, msg_type2, plen2 = _unpack_header(raw_header2)
            raw_body2 = _recv_exact(conn, plen2) if plen2 > 0 else b"{}"
            if raw_body2 is None:
                return
            payload2 = json.loads(raw_body2.decode("utf-8"))

            # Update state dari respons kedua
            msg_type = msg_type2
            password = payload2.get("password", "")
            # username bisa berubah jika client mengetik ulang (tidak umum, tapi aman)
            username = payload2.get("username", username).strip()

        # ── Proses registrasi atau login ───────────────────────────────
        existing = get_user(username)

        if msg_type == MSG_REGISTER:
            if existing:
                send(MSG_LOGIN_FAIL, {"reason": "Username sudah terdaftar. Gunakan /login <password>."})
                conn.close()
                return
            if not password:
                send(MSG_LOGIN_FAIL, {"reason": "Password tidak boleh kosong."})
                conn.close()
                return
            salt    = generate_salt()
            pw_hash = hash_password(password, salt)
            if not register_user(username, pw_hash, salt):
                send(MSG_LOGIN_FAIL, {"reason": "Gagal mendaftar. Coba username lain."})
                conn.close()
                return
            log.info(f"User baru '{username}' ({peer_ip}) berhasil didaftarkan.")

        elif msg_type == MSG_LOGIN:
            if not existing:
                send(MSG_LOGIN_FAIL, {"reason": "User tidak ditemukan. Gunakan /register <password>."})
                conn.close()
                return
            if not verify_password(password, existing["salt"], existing["password_hash"]):
                log.warning(f"Login gagal untuk '{username}' dari {peer_ip}.")
                send(MSG_LOGIN_FAIL, {"reason": "Password salah."})
                conn.close()
                return

        else:
            send(MSG_LOGIN_FAIL, {"reason": "Pesan tidak dikenali saat autentikasi."})
            conn.close()
            return

        # ── Login berhasil ─────────────────────────────────────────────
        session = session_mgr.login(username, conn, addr)
        send(MSG_LOGIN_OK, {"username": username})
        log.info(f"'{username}' ({peer_ip}) berhasil login.")

        # Kirim pesan offline yang belum tersampaikan
        msg_router.deliver_offline(username, session)

        # Beritahu user lain bahwa user ini online
        session_mgr.broadcast_system(f"{username} telah online.", exclude=username)

        # ── Tahap 2: Loop pesan utama ──────────────────────────────────
        while True:
            raw_header = _recv_exact(conn, HEADER_SIZE)
            if not raw_header:
                break
            seq, ts, msg_type, plen = _unpack_header(raw_header)
            raw_body = _recv_exact(conn, plen) if plen > 0 else b"{}"
            if raw_body is None:
                break
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except json.JSONDecodeError:
                log.error(f"Payload JSON tidak valid dari '{username}'.")
                continue

            if msg_type == MSG_ROUTE:
                to_user = payload.get("to", "").strip()
                message = payload.get("message", "")
                msg_seq = payload.get("seq", seq)
                if to_user and message:
                    msg_router.route(username, to_user, msg_seq, message, session)
                else:
                    log.warning(f"Pesan ROUTE tidak valid dari '{username}': {payload}")

            elif msg_type == MSG_USER_LIST:
                online = session_mgr.list_online()
                session.send(MSG_USER_LIST_RESP, {"users": online})

            elif msg_type == MSG_DISCONNECT:
                log.info(f"'{username}' mengirim DISCONNECT.")
                break

            elif msg_type == MSG_ACK:
                pass  # Client ACK untuk pesan diterima — tidak perlu di-forward di model ini

    except Exception as e:
        log.error(f"Error pada sesi '{username or peer_ip}': {e}")

    finally:
        if username:
            session_mgr.logout(username)
            session_mgr.broadcast_system(f"{username} telah offline.", exclude=username)
        try:
            conn.close()
        except Exception:
            pass
        log.info(f"Sesi '{username or peer_ip}' ({peer_ip}) ditutup.")


# ─── Entry Point ──────────────────────────────────────────────────────────

def _get_local_ip() -> str:
    """Deteksi IP LAN mesin ini secara otomatis."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def main():
    local_ip = _get_local_ip()
    w = 60

    print("=" * w)
    print(f"  {APP_NAME} — Standalone Server".center(w))
    print("=" * w)
    print(f"  Status     : BERJALAN")
    print(f"  Port       : {TCP_PORT}")
    print(f"  IP Server  : {local_ip}")
    print("-" * w)
    print(f"  Cara client terhubung ke server ini:")
    print(f"    python main.py <username> {local_ip}")
    print()
    print(f"  Contoh:")
    print(f"    python main.py Alice {local_ip}")
    print(f"    python main.py Bob   {local_ip}")
    print("-" * w)
    print(f"  Perintah client setelah konek:")
    print(f"    /register <password>   -- daftar akun baru (pertama kali)")
    print(f"    /login <password>      -- masuk (jika sudah terdaftar)")
    print(f"    /to <user> <pesan>     -- kirim pesan ke user lain")
    print(f"    /users                 -- lihat siapa yang sedang online")
    print(f"    /history               -- lihat riwayat chat lokal")
    print(f"    /quit                  -- keluar dari aplikasi")
    print("-" * w)
    print(f"  Tekan Ctrl+C untuk menghentikan server.")
    print("=" * w)
    print()

    init_db()
    server = TCPServer(on_new_client=handle_new_client)
    server.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nServer dihentikan.")
        server.stop()


if __name__ == "__main__":
    main()
