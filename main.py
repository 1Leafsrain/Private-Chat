"""
main.py - Entry Point Aplikasi Chat Private TCP+UDP
=====================================================
Cara Penggunaan:

  Mode SERVER (menunggu koneksi):
    python main.py server <username>

  Mode CLIENT (menghubungi server):
    python main.py client <username> <server_ip>

Contoh:
  Terminal 1:  python main.py server Alice
  Terminal 2:  python main.py client Bob 192.168.1.10

Tekan Ctrl+C untuk keluar.
"""
import sys
import time
import threading
import os

# Tambahkan direktori root ke sys.path agar import berjalan
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import APP_NAME, PROMPT, PASSWORD
from persistence import init_db, save_message, mark_acked, get_all_unsent, enqueue_unsent, dequeue_unsent
from message_queue import MessageQueue
from reliability import ReliabilityLayer
from tcp_handler import TCPServer, TCPClient
from udp_handler import UDPHeartbeat
from security import get_password_hash
from utils.logger import get_logger

log = get_logger("main")

# ─── ANSI Colors ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"


def print_banner(username: str, mode: str):
    os.system("cls" if os.name == "nt" else "clear")
    print(f"{BOLD}{CYAN}{'═'*55}{RESET}")
    print(f"{BOLD}{CYAN}  {APP_NAME}  |  {mode.upper()}  |  User: {username}{RESET}")
    print(f"{BOLD}{CYAN}{'═'*55}{RESET}")
    print(f"{DIM}  Ketik pesan lalu tekan Enter untuk mengirim.{RESET}")
    print(f"{DIM}  Ketik /history untuk melihat riwayat chat.{RESET}")
    print(f"{DIM}  Ketik /status  untuk melihat status koneksi.{RESET}")
    print(f"{DIM}  Ketik /quit    untuk keluar.{RESET}")
    print(f"{CYAN}{'─'*55}{RESET}\n")


class ChatApp:
    def __init__(self, username: str, mode: str, peer_ip: str = ""):
        self.username   = username
        self.mode       = mode          # "server" atau "client"
        self.peer_ip    = peer_ip
        self.connected  = False
        self._stop      = threading.Event()

        init_db()

        self.msg_queue  = MessageQueue()
        self.reliability = ReliabilityLayer(on_give_up=self._on_message_give_up)

        # UDP (heartbeat) — hanya aktif setelah peer_ip diketahui
        self._udp: UDPHeartbeat = None

        # TCP
        if mode == "server":
            self.tcp = TCPServer(
                username       = username,
                on_message     = self._on_recv_message,
                on_ack         = self._on_ack,
                on_connected   = self._on_connected,
                on_disconnect  = self._on_disconnect,
            )
        else:
            self.tcp = TCPClient(
                username       = username,
                server_ip      = peer_ip,
                on_message     = self._on_recv_message,
                on_ack         = self._on_ack,
                on_connected   = self._on_connected,
                on_disconnect  = self._on_disconnect,
            )

        # Worker thread untuk mengirim dari queue
        self._sender_thread = threading.Thread(
            target=self._send_worker, daemon=True, name="SendWorker"
        )

    # ─── Callback dari TCP Layer ──────────────────────────────────────────

    def _on_recv_message(self, sender: str, text: str, seq: int):
        """Dipanggil saat pesan chat baru diterima."""
        ts = time.time()
        save_message(seq, sender, text, "received", ts, acked=True)
        # Tampilkan di atas input prompt
        print(f"\r{' '*60}\r{CYAN}{sender}{RESET}: {text}")
        print(PROMPT, end="", flush=True)

    def _on_ack(self, seq: int):
        """Dipanggil saat ACK untuk pesan yang kita kirim diterima."""
        self.reliability.ack(seq)
        mark_acked(seq)
        dequeue_unsent(seq)
        log.debug(f"Pesan seq={seq} berhasil di-ACK.")

    def _on_connected(self, peer_ip: str):
        """Dipanggil saat koneksi TCP berhasil terbentuk."""
        self.connected = True
        self.peer_ip = peer_ip
        print(f"\n{GREEN}✓ Terhubung ke {peer_ip}{RESET}\n")
        log.info(f"Terhubung ke {peer_ip}")

        # Inisialisasi / restart UDP heartbeat
        if self._udp:
            self._udp.stop()
        self._udp = UDPHeartbeat(
            username    = self.username,
            peer_ip     = peer_ip,
            on_timeout  = self._on_heartbeat_timeout,
            on_restored = self._on_heartbeat_restored,
        )
        self._udp.start()

        # Kirim ulang pesan yang belum ter-ACK (dari persistence)
        self._resend_unsent()

    def _on_disconnect(self):
        """Dipanggil saat koneksi TCP terputus."""
        self.connected = False
        print(f"\n{YELLOW}⚠ Koneksi terputus. Menunggu reconnect...{RESET}\n")
        log.info("Koneksi terputus.")

    def _on_heartbeat_timeout(self):
        print(f"\n{RED}✗ Heartbeat timeout — peer tidak responsif.{RESET}\n")

    def _on_heartbeat_restored(self):
        print(f"\n{GREEN}✓ Koneksi pulih (heartbeat diterima kembali).{RESET}\n")

    def _on_message_give_up(self, seq: int):
        print(f"\n{RED}✗ Pesan seq={seq} gagal terkirim setelah maks retry.{RESET}\n")

    # ─── Sender Worker ────────────────────────────────────────────────────

    def _send_worker(self):
        """Mengambil dari queue dan mengirim via TCP."""
        while not self._stop.is_set():
            item = self.msg_queue.get_message(timeout=0.2)
            if item is None:
                continue
            priority, seq, payload = item
            if not self.connected:
                # Simpan ke unsent queue untuk dikirim ulang nanti
                enqueue_unsent(seq, payload)
                log.info(f"Belum terhubung, pesan seq={seq} disimpan ke unsent_queue.")
                self.msg_queue.task_done()
                continue
            try:
                self.tcp.send(payload)
                # Daftarkan ke reliability layer untuk tracking ACK
                self.reliability.track(seq, payload, self.tcp.send)
                log.debug(f"Pesan seq={seq} terkirim, menunggu ACK.")
            except Exception as e:
                log.error(f"Gagal kirim seq={seq}: {e}")
                enqueue_unsent(seq, payload)
                self.msg_queue.put_retry(payload, seq)
            self.msg_queue.task_done()

    def _resend_unsent(self):
        """Kirim ulang semua pesan yang tersimpan di unsent_queue."""
        unsent = get_all_unsent()
        if unsent:
            print(f"{YELLOW}↺ Mengirim ulang {len(unsent)} pesan yang tertunda...{RESET}")
        for item in unsent:
            self.msg_queue.put_retry(item["payload"], item["seq"])

    # ─── User Input Loop ──────────────────────────────────────────────────

    def _show_history(self):
        from persistence import load_history
        rows = load_history(limit=20)
        print(f"\n{BOLD}── Riwayat Chat (20 terakhir) ──{RESET}")
        for r in reversed(rows):
            t   = time.strftime("%H:%M:%S", time.localtime(r["timestamp"]))
            dir_sym = "→" if r["direction"] == "sent" else "←"
            ack_sym = "✓" if r["acked"] else "…"
            color = GREEN if r["direction"] == "sent" else CYAN
            print(f"  {DIM}{t}{RESET} {color}{dir_sym} {r['sender']}{RESET}: "
                  f"{r['message']} {DIM}[{ack_sym}]{RESET}")
        print()

    def _show_status(self):
        print(f"\n{BOLD}── Status Koneksi ──{RESET}")
        print(f"  Mode       : {self.mode}")
        print(f"  Username   : {self.username}")
        print(f"  Peer IP    : {self.peer_ip or '-'}")
        print(f"  Connected  : {GREEN+'Ya'+RESET if self.connected else RED+'Tidak'+RESET}")
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
            elif text == "/history":
                self._show_history()
            elif text == "/status":
                self._show_status()
            elif text.startswith("/"):
                print(f"{RED}Perintah tidak dikenal. Coba /history, /status, /quit{RESET}")
            else:
                # Kirim pesan chat
                seq = self.reliability.next_seq()
                payload = {
                    "_seq":    seq,
                    "sender":  self.username,
                    "message": text,
                }
                save_message(seq, self.username, text, "sent", time.time(), acked=False)
                self.msg_queue.put_message(payload, seq)

    # ─── Start / Stop ─────────────────────────────────────────────────────

    def run(self):
        print_banner(self.username, self.mode)

        if self.mode == "server":
            print(f"{YELLOW}Menunggu koneksi di port {5555}...{RESET}\n")
        else:
            print(f"{YELLOW}Menghubungi {self.peer_ip}...{RESET}\n")

        self.tcp.start()
        self._sender_thread.start()

        try:
            self._input_loop()
        finally:
            self._stop.set()
            self.tcp.stop()
            if self._udp:
                self._udp.stop()
            self.reliability.stop()
            print(f"\n{DIM}Semua koneksi ditutup.{RESET}")


# ─── Entry Point ──────────────────────────────────────────────────────────

def usage():
    print(__doc__)
    sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        usage()

    mode     = sys.argv[1].lower()
    username = sys.argv[2]

    if mode == "server":
        app = ChatApp(username=username, mode="server")
    elif mode == "client":
        if len(sys.argv) < 4:
            print(f"{RED}Error: mode client membutuhkan <server_ip>{RESET}")
            usage()
        server_ip = sys.argv[3]
        app = ChatApp(username=username, mode="client", peer_ip=server_ip)
    else:
        usage()

    app.run()
