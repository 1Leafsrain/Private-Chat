"""
main.py - Client Aplikasi Chat
================================
Cara Penggunaan:
  python main.py <username> <server_ip>

Contoh:
  python main.py Alice 192.168.1.10
  python main.py Bob   192.168.1.10

Perintah saat autentikasi (sebelum login):
  /register <password>   â€” daftar sebagai pengguna baru
  /login <password>      â€” masuk dengan password

Perintah saat chat (setelah login):
  /to <user> <pesan>     â€” kirim pesan ke user tertentu
  /users                 â€” lihat daftar user yang sedang online
  /history               â€” lihat riwayat chat lokal
  /status                â€” lihat status koneksi
  /quit                  â€” keluar
"""
import sys
import time
import threading
import os
from typing import List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import APP_NAME, PROMPT, TCP_PORT
from persistence import init_db, save_message, mark_acked, get_all_unsent, enqueue_unsent, dequeue_unsent, set_username
from message_queue import MessageQueue
from reliability import ReliabilityLayer
from tcp_handler import TCPClient
from utils.logger import get_logger

log = get_logger("main")

# â”€â”€â”€ ANSI Colors â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
GREEN  = "\033[92m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"


def print_banner(username: str, server_ip: str):
    os.system("cls" if os.name == "nt" else "clear")
    print(f"{BOLD}{CYAN}{'â•'*55}{RESET}")
    print(f"{BOLD}{CYAN}  {APP_NAME}  |  User: {username}  |  Server: {server_ip}{RESET}")
    print(f"{BOLD}{CYAN}{'â•'*55}{RESET}")
    print(f"{DIM}  Menghubungi server...{RESET}")
    print(f"{CYAN}{'â”€'*55}{RESET}\n")


class ChatApp:
    def __init__(self, username: str, server_ip: str):
        self.username        = username
        self.server_ip       = server_ip
        self.connected       = False
        self._authenticated  = False
        self._cached_password: str = None   # Cache password untuk auto-login saat reconnect
        self._stop           = threading.Event()

        set_username(username)
        init_db(username)

        self.msg_queue   = MessageQueue()
        self.reliability = ReliabilityLayer(on_give_up=self._on_message_give_up)

        self.tcp = TCPClient(
            username         = username,
            server_ip        = server_ip,
            on_message       = self._on_recv_message,
            on_ack           = self._on_ack,
            on_connected     = self._on_connected,
            on_disconnect    = self._on_disconnect,
            on_auth_required = self._on_auth_required,
            on_login_ok      = self._on_login_ok,
            on_login_fail    = self._on_login_fail,
            on_user_list     = self._on_user_list,
            on_system        = self._on_system,
        )

        self._sender_thread = threading.Thread(
            target=self._send_worker, daemon=True, name="SendWorker"
        )

    # â”€â”€â”€ Callback dari TCP Layer â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _on_recv_message(self, from_user: str, text: str, seq: int):
        """Dipanggil saat pesan baru diterima dari server (dikirim oleh user lain)."""
        ts = time.time()
        save_message(seq, from_user, text, "received", ts, acked=True, recipient=self.username)
        print(f"\r{' '*60}\r{CYAN}{from_user}{RESET}: {text}")
        print(PROMPT, end="", flush=True)

    def _on_ack(self, seq: int):
        """Dipanggil saat ACK diterima dari server untuk pesan yang kita kirim."""
        self.reliability.ack(seq)
        mark_acked(seq)
        dequeue_unsent(seq)
        log.debug(f"Pesan seq={seq} berhasil di-ACK oleh server.")

    def _on_connected(self, server_ip: str):
        """Dipanggil saat koneksi TCP ke server berhasil."""
        self.connected = True
        print(f"\n{GREEN}âœ“ Terhubung ke server {server_ip}{RESET}")
        log.info(f"Terhubung ke server {server_ip}")

    def _on_disconnect(self):
        """Dipanggil saat koneksi ke server terputus."""
        self.connected      = False
        self._authenticated = False
        print(f"\n{YELLOW}âš  Koneksi terputus. Menunggu reconnect...{RESET}\n")
        log.info("Koneksi ke server terputus.")

    def _on_auth_required(self, action: str):
        """Server memberitahu status user: perlu register atau perlu password."""
        # Auto-login saat reconnect jika password sudah di-cache
        if self._cached_password and action == "need_password":
            self.tcp.send_login(self.username, self._cached_password)
            return

        if action == "need_register":
            print(f"\n{YELLOW}Pengguna baru! Ketik /register <password> untuk mendaftar.{RESET}")
        else:
            print(f"\n{YELLOW}Ketik /login <password> untuk masuk.{RESET}")
        print(PROMPT, end="", flush=True)

    def _on_login_ok(self):
        """Dipanggil saat login/register berhasil."""
        self._authenticated = True
        print(f"\n{GREEN}âœ“ Login berhasil! Selamat datang, {self.username}.{RESET}")
        print(f"{DIM}  /to <user> <pesan>  â€” kirim pesan{RESET}")
        print(f"{DIM}  /users              â€” lihat siapa online{RESET}")
        print(f"{DIM}  /history            â€” riwayat chat lokal{RESET}")
        print(f"{DIM}  /quit               â€” keluar{RESET}\n")
        self._resend_unsent()

    def _on_login_fail(self, reason: str):
        """Dipanggil saat login/register gagal."""
        print(f"\n{RED}âœ— Gagal: {reason}{RESET}")
        print(PROMPT, end="", flush=True)

    def _on_user_list(self, users: List[str]):
        """Dipanggil saat daftar user online diterima dari server."""
        print(f"\n{BOLD}â”€â”€ User Online ({len(users)}) â”€â”€{RESET}")
        for u in users:
            if u == self.username:
                print(f"  {DIM}â— {u} (kamu){RESET}")
            else:
                print(f"  {GREEN}â— {RESET}{u}")
        print()
        print(PROMPT, end="", flush=True)

    def _on_system(self, payload: dict):
        """Dipanggil saat notifikasi sistem diterima dari server."""
        message = payload.get("message", "")
        if message:
            print(f"\r{' '*60}\r{DIM}[sistem] {message}{RESET}")
            print(PROMPT, end="", flush=True)

    def _on_message_give_up(self, seq: int):
        print(f"\n{RED}âœ— Pesan seq={seq} gagal terkirim setelah maks retry.{RESET}\n")

    # â”€â”€â”€ Sender Worker â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _send_worker(self):
        """Mengambil pesan dari queue dan mengirim via TCP ke server."""
        while not self._stop.is_set():
            item = self.msg_queue.get_message(timeout=0.2)
            if item is None:
                continue
            priority, seq, payload = item
            try:
                if not self.connected or not self._authenticated:
                    enqueue_unsent(seq, payload)
                    log.info(f"Belum terhubung/login, pesan seq={seq} disimpan ke unsent_queue.")
                    continue
                try:
                    self.tcp.send(payload)
                    self.reliability.track(seq, payload, self.tcp.send)
                    log.debug(f"Pesan seq={seq} dikirim ke server.")
                except Exception as e:
                    log.error(f"Gagal kirim seq={seq}: {e}")
                    # Simpan ke unsent_queue saja; _resend_unsent() akan kirim ulang
                    # saat reconnect. JANGAN put_retry agar tidak duplikasi pesan.
                    enqueue_unsent(seq, payload)
            finally:
                self.msg_queue.task_done()

    def _resend_unsent(self):
        """Kirim ulang semua pesan yang tersimpan di unsent_queue."""
        unsent = get_all_unsent()
        if unsent:
            print(f"{YELLOW}â†º Mengirim ulang {len(unsent)} pesan yang tertunda...{RESET}")
        for item in unsent:
            self.msg_queue.put_retry(item["payload"], item["seq"])

    # â”€â”€â”€ Input Loop â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _show_history(self):
        from persistence import load_history
        rows = load_history(limit=20)
        print(f"\n{BOLD}â”€â”€ Riwayat Chat (20 terakhir) â”€â”€{RESET}")
        for r in reversed(rows):
            t       = time.strftime("%H:%M:%S", time.localtime(r["timestamp"]))
            dir_sym = "â†’" if r["direction"] == "sent" else "â†"
            ack_sym = "âœ“" if r["acked"] else "â€¦"
            color   = GREEN if r["direction"] == "sent" else CYAN
            recip   = f" â†’ {r['recipient']}" if r["recipient"] else ""
            print(
                f"  {DIM}{t}{RESET} {color}{dir_sym} {r['sender']}{recip}{RESET}: "
                f"{r['message']} {DIM}[{ack_sym}]{RESET}"
            )
        print()

    def _show_status(self):
        print(f"\n{BOLD}â”€â”€ Status Koneksi â”€â”€{RESET}")
        print(f"  Username   : {self.username}")
        print(f"  Server     : {self.server_ip}:{TCP_PORT}")
        print(f"  Connected  : {GREEN+'Ya'+RESET if self.connected else RED+'Tidak'+RESET}")
        print(f"  Login      : {GREEN+'Ya'+RESET if self._authenticated else RED+'Tidak'+RESET}")
        print(f"  Pending ACK: {self.reliability.pending_count()}")
        print(f"  Queue size : {self.msg_queue.qsize()}")
        print()

    def _input_loop(self):
        while not self._stop.is_set():
            try:
                text = input(PROMPT).strip()
            except (EOFError, KeyboardInterrupt):
                break

            if not text:
                continue

            if text == "/quit":
                print(f"\n{YELLOW}Sampai jumpa!{RESET}")
                self._stop.set()
                break

            # â”€â”€ Mode pra-autentikasi â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            if not self._authenticated:
                if text.startswith("/register "):
                    password = text[len("/register "):].strip()
                    if not password:
                        print(f"{RED}Gunakan: /register <password>{RESET}")
                        continue
                    self._cached_password = password
                    self.tcp.send_register(self.username, password)

                elif text.startswith("/login "):
                    password = text[len("/login "):].strip()
                    if not password:
                        print(f"{RED}Gunakan: /login <password>{RESET}")
                        continue
                    self._cached_password = password
                    self.tcp.send_login(self.username, password)

                else:
                    print(
                        f"{RED}Autentikasi diperlukan. "
                        f"Gunakan /register <password> atau /login <password>{RESET}"
                    )
                continue

            # â”€â”€ Mode chat â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            if text == "/history":
                self._show_history()
            elif text == "/status":
                self._show_status()
            elif text == "/users":
                self.tcp.request_user_list()
            elif text.startswith("/to "):
                parts = text[4:].split(" ", 1)
                if len(parts) < 2 or not parts[0].strip() or not parts[1].strip():
                    print(f"{RED}Gunakan: /to <username> <pesan>{RESET}")
                    continue
                to_user, message = parts[0].strip(), parts[1].strip()
                seq = self.reliability.next_seq()
                payload = {
                    "_seq":    seq,
                    "sender":  self.username,
                    "to":      to_user,
                    "message": message,
                }
                save_message(seq, self.username, message, "sent",
                             time.time(), acked=False, recipient=to_user)
                self.msg_queue.put_message(payload, seq)
            elif text.startswith("/"):
                print(
                    f"{RED}Perintah tidak dikenal. "
                    f"Coba /to <user> <pesan>, /users, /history, /status, /quit{RESET}"
                )
            else:
                print(f"{YELLOW}Gunakan /to <user> <pesan> untuk mengirim pesan.{RESET}")

    # â”€â”€â”€ Start / Stop â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def run(self):
        print_banner(self.username, self.server_ip)
        self.tcp.start()
        self._sender_thread.start()
        try:
            self._input_loop()
        finally:
            self._stop.set()
            self.tcp.stop()
            self.reliability.stop()
            print(f"\n{DIM}Semua koneksi ditutup.{RESET}")


# â”€â”€â”€ Entry Point â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def usage():
    print(__doc__)
    sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        usage()

    username  = sys.argv[1]
    server_ip = sys.argv[2]

    app = ChatApp(username=username, server_ip=server_ip)
    app.run()
