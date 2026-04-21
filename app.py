"""
app.py - Desktop Chat Client (Terminal Disguise)
Tampilan seperti Command Prompt, tapi ini adalah aplikasi chat.
Jalankan: python app.py
"""
import sys
import os
import time
import threading
import queue as _queue
import tkinter as tk
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import TCP_PORT
from persistence import (init_db, save_message, mark_acked,
                         get_all_unsent, enqueue_unsent, dequeue_unsent)
from message_queue import MessageQueue
from reliability import ReliabilityLayer
from tcp_handler import TCPClient
from utils.logger import get_logger

log = get_logger("app")

BG      = "#0C0C0C"
FG      = "#CCCCCC"
GREEN   = "#16C60C"
BLUE    = "#3B78FF"
YELLOW  = "#F9F1A5"
RED     = "#E74856"
DIM     = "#767676"
CYAN    = "#61D6D6"
SEL_BG  = "#264F78"

FONT      = ("Consolas", 10)
FONT_BOLD = ("Consolas", 10, "bold")

ST_USERNAME   = "username"
ST_IP         = "ip"
ST_CONNECTING = "connecting"
ST_AUTH       = "auth"
ST_CHAT       = "chat"


class TerminalChat:
    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("Command Prompt")
        root.configure(bg=BG)
        root.geometry("860x520")
        root.minsize(640, 420)

        self.state            = ST_USERNAME
        self.username: Optional[str] = None
        self.server_ip: Optional[str] = None
        self._cached_pw: Optional[str] = None
        self._authenticated   = False
        self._stop            = threading.Event()

        self.tcp: Optional[TCPClient]              = None
        self.msg_queue: Optional[MessageQueue]     = None
        self.reliability: Optional[ReliabilityLayer] = None
        self._sender_thread: Optional[threading.Thread] = None

        self._ui_q: _queue.Queue = _queue.Queue()

        self._build_ui()
        self._print_header()
        self._set_prompt("C:\\> ")
        self._write_now("")
        self._write_now("Masukkan username Anda.", "dim")
        self.state = ST_USERNAME

        self.root.after(40, self._poll_ui)

    # ------------------------------------------------------------------ UI
    def _build_ui(self):
        frame_out = tk.Frame(self.root, bg=BG)
        frame_out.pack(fill=tk.BOTH, expand=True)

        self.output = tk.Text(
            frame_out,
            bg=BG, fg=FG, font=FONT,
            wrap=tk.WORD, state=tk.DISABLED,
            cursor="arrow", relief=tk.FLAT,
            padx=8, pady=6,
            insertbackground=FG,
            selectbackground=SEL_BG,
            selectforeground="#FFFFFF",
            spacing1=1, spacing3=1,
        )

        scrollbar = tk.Scrollbar(
            frame_out,
            command=self.output.yview,
            bg="#1C1C1C", troughcolor=BG,
            activebackground="#555555",
            width=8, relief=tk.FLAT, bd=0,
        )
        self.output.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.output.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        for tag, col in [
            ("default", FG), ("green", GREEN), ("blue", BLUE),
            ("yellow", YELLOW), ("red", RED), ("dim", DIM), ("cyan", CYAN),
        ]:
            self.output.tag_configure(tag, foreground=col, font=FONT)
        self.output.tag_configure("bold", font=FONT_BOLD)

        sep = tk.Frame(self.root, bg="#2D2D2D", height=1)
        sep.pack(fill=tk.X, side=tk.BOTTOM)

        input_frame = tk.Frame(self.root, bg=BG)
        input_frame.pack(fill=tk.X, side=tk.BOTTOM)

        self.lbl_prompt = tk.Label(
            input_frame, text="C:\\> ",
            bg=BG, fg=FG, font=FONT, padx=8, pady=4,
        )
        self.lbl_prompt.pack(side=tk.LEFT)

        self.entry_var = tk.StringVar()
        self.entry = tk.Entry(
            input_frame,
            textvariable=self.entry_var,
            bg=BG, fg=FG, font=FONT,
            relief=tk.FLAT, insertbackground=FG,
            highlightthickness=0, bd=0,
        )
        self.entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 8), pady=4)
        self.entry.bind("<Return>", self._on_enter)
        self.entry.focus_set()

    # ---------------------------------------------------------- thread-safe write
    def _write(self, text: str, color: str = "default", newline: bool = True):
        self._ui_q.put(("write", text, color, newline))

    def _write_now(self, text: str, color: str = "default", newline: bool = True):
        self.output.configure(state=tk.NORMAL)
        self.output.insert(tk.END, text + ("\n" if newline else ""), color)
        self.output.see(tk.END)
        self.output.configure(state=tk.DISABLED)

    def _set_prompt(self, text: str):
        self._ui_q.put(("prompt", text))

    def _set_state(self, state: str):
        self._ui_q.put(("state", state))

    def _poll_ui(self):
        try:
            while True:
                item = self._ui_q.get_nowait()
                cmd  = item[0]
                if cmd == "write":
                    self._write_now(item[1], item[2], item[3])
                elif cmd == "prompt":
                    self.lbl_prompt.configure(text=item[1])
                elif cmd == "state":
                    self.state = item[1]
        except _queue.Empty:
            pass
        self.root.after(40, self._poll_ui)

    def _print_header(self):
        self._write_now("Microsoft Windows [Version 10.0.22631.4751]", "dim")
        self._write_now("(c) Microsoft Corporation. All rights reserved.", "dim")

    # ---------------------------------------------------------- input dispatch
    def _on_enter(self, event=None):
        text    = self.entry_var.get()
        self.entry_var.set("")
        stripped = text.strip()
        prompt   = self.lbl_prompt.cget("text")
        self._write_now(prompt + stripped, "default")
        if not stripped:
            return
        if self.state == ST_USERNAME:
            self._handle_username(stripped)
        elif self.state == ST_IP:
            self._handle_ip(stripped)
        elif self.state == ST_AUTH:
            self._handle_auth(stripped)
        elif self.state == ST_CHAT:
            self._handle_chat(stripped)

    def _handle_username(self, text: str):
        if not all(c.isalnum() or c in "-_" for c in text):
            self._write_now("Username hanya huruf/angka/-/_. Coba lagi.", "red")
            return
        self.username = text
        self.state    = ST_IP
        self._write_now("")
        self._write_now("Masukkan IP server.", "dim")

    def _handle_ip(self, text: str):
        self.server_ip = text
        self.state     = ST_CONNECTING
        self._write_now(f"Menghubungi {self.server_ip}:{TCP_PORT}...", "dim")
        self.lbl_prompt.configure(text=f"C:\\Users\\{self.username}> ")
        self._start_chat_backend()

    def _handle_auth(self, text: str):
        low = text.lower()
        if low.startswith("/register "):
            pw = text[10:].strip()
            if not pw:
                self._write_now("Gunakan: /register <password>", "red")
                return
            self._cached_pw = pw
            self.tcp.send_register(self.username, pw)
        elif low.startswith("/login "):
            pw = text[7:].strip()
            if not pw:
                self._write_now("Gunakan: /login <password>", "red")
                return
            self._cached_pw = pw
            self.tcp.send_login(self.username, pw)
        elif text == "/quit":
            self._quit()
        else:
            self._write_now("Ketik /register <password>  atau  /login <password>", "yellow")

    def _handle_chat(self, text: str):
        if text == "/quit":
            self._quit()
        elif text == "/users":
            self.tcp.request_user_list()
        elif text == "/history":
            self._show_history()
        elif text == "/status":
            self._show_status()
        elif text.lower().startswith("/to "):
            parts = text[4:].split(" ", 1)
            if len(parts) < 2 or not parts[0].strip() or not parts[1].strip():
                self._write_now("Gunakan: /to <username> <pesan>", "red")
                return
            self._send_message(parts[0].strip(), parts[1].strip())
        elif text.startswith("/"):
            self._write_now(f"Perintah tidak dikenal: {text}", "red")
            self._write_now(
                "Tersedia: /to <user> <pesan>  /users  /history  /status  /quit", "dim"
            )
        else:
            self._write_now("Gunakan /to <user> <pesan> untuk mengirim pesan.", "yellow")

    # ---------------------------------------------------------- backend
    def _start_chat_backend(self):
        init_db()
        self.msg_queue   = MessageQueue()
        self.reliability = ReliabilityLayer(on_give_up=self._on_give_up)
        self.tcp = TCPClient(
            username         = self.username,
            server_ip        = self.server_ip,
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
        self.tcp.start()
        self._sender_thread = threading.Thread(
            target=self._send_worker, daemon=True, name="SendWorker"
        )
        self._sender_thread.start()

    def _send_message(self, to_user: str, message: str):
        seq     = self.reliability.next_seq()
        payload = {"_seq": seq, "sender": self.username, "to": to_user, "message": message}
        save_message(seq, self.username, message, "sent",
                     time.time(), acked=False, recipient=to_user)
        self.msg_queue.put_message(payload, seq)
        t = time.strftime("%H:%M:%S")
        self._write_now(f"  [{t}] Kamu -> {to_user} : {message}", "blue")

    def _on_recv_message(self, from_user: str, text: str, seq: int):
        ts = time.time()
        save_message(seq, from_user, text, "received", ts, acked=True, recipient=self.username)
        t  = time.strftime("%H:%M:%S", time.localtime(ts))
        self._write(f"  [{t}] {from_user} : {text}", "green")

    def _on_ack(self, seq: int):
        self.reliability.ack(seq)
        mark_acked(seq)
        dequeue_unsent(seq)

    def _on_connected(self, server_ip: str):
        self._write(f"Terhubung ke server {server_ip}.", "dim")

    def _on_disconnect(self):
        self._authenticated = False
        self._write("Koneksi terputus. Mencoba reconnect...", "yellow")
        self._set_state(ST_CONNECTING)

    def _on_auth_required(self, action: str):
        if self._cached_pw and action == "need_password":
            self.tcp.send_login(self.username, self._cached_pw)
            return
        if action == "need_register":
            self._write("")
            self._write("Pengguna baru terdeteksi.", "yellow")
            self._write("Ketik: /register <password>", "yellow")
        else:
            self._write("")
            self._write("Ketik: /login <password>", "yellow")
        self._set_state(ST_AUTH)

    def _on_login_ok(self):
        self._authenticated = True
        self._write(f"Login berhasil. Selamat datang, {self.username}!", "green")
        self._write("  /to <user> <pesan>   /users   /history   /status   /quit", "dim")
        self._write("")
        self._set_state(ST_CHAT)
        self._resend_unsent()

    def _on_login_fail(self, reason: str):
        self._write(f"Gagal: {reason}", "red")
        self._set_state(ST_AUTH)

    def _on_user_list(self, users: List[str]):
        self._write(f"User online ({len(users)}):", "cyan")
        for u in users:
            note = "  <- kamu" if u == self.username else ""
            self._write(f"  * {u}{note}", "cyan")

    def _on_system(self, payload: dict):
        msg = payload.get("message", "")
        if msg:
            self._write(f"[notif] {msg}", "dim")

    def _on_give_up(self, seq: int):
        self._write(f"Pesan seq={seq} gagal terkirim (maks retry).", "red")

    def _send_worker(self):
        while not self._stop.is_set():
            item = self.msg_queue.get_message(timeout=0.2)
            if item is None:
                continue
            _, seq, payload = item
            if not self._authenticated:
                enqueue_unsent(seq, payload)
                self.msg_queue.task_done()
                continue
            try:
                self.tcp.send(payload)
                self.reliability.track(seq, payload, self.tcp.send)
            except Exception as e:
                log.error(f"Gagal kirim seq={seq}: {e}")
                enqueue_unsent(seq, payload)
                self.msg_queue.put_retry(payload, seq)
            self.msg_queue.task_done()

    def _resend_unsent(self):
        unsent = get_all_unsent()
        if unsent:
            self._write(f"Mengirim ulang {len(unsent)} pesan tertunda...", "dim")
        for item in unsent:
            self.msg_queue.put_retry(item["payload"], item["seq"])

    def _show_history(self):
        from persistence import load_history
        rows = load_history(limit=20)
        self._write("-- Riwayat Chat (20 terakhir) --", "dim")
        for r in reversed(rows):
            t   = time.strftime("%H:%M:%S", time.localtime(r["timestamp"]))
            sym = "->" if r["direction"] == "sent" else "<-"
            ack = "v" if r["acked"] else "..."
            tag = "blue" if r["direction"] == "sent" else "green"
            recip = f" -> {r['recipient']}" if r.get("recipient") else ""
            self._write(f"  [{t}] {sym} {r['sender']}{recip}: {r['message']}  [{ack}]", tag)
        self._write("-" * 40, "dim")

    def _show_status(self):
        conn = self.tcp and getattr(self.tcp, "_sock", None) is not None
        self._write("-- Status --", "dim")
        self._write(f"  Username : {self.username or '-'}")
        self._write(f"  Server   : {self.server_ip or '-'}:{TCP_PORT}")
        self._write(f"  Koneksi  : {'Ya' if conn else 'Tidak'}", "green" if conn else "red")
        self._write(f"  Login    : {'Ya' if self._authenticated else 'Tidak'}",
                    "green" if self._authenticated else "red")
        if self.reliability:
            self._write(f"  Pending  : {self.reliability.pending_count()} ACK")
        self._write("-" * 40, "dim")

    def _quit(self):
        self._write("Menutup koneksi...", "dim")
        self._stop.set()
        if self.tcp:
            try:
                self.tcp.stop()
            except Exception:
                pass
        if self.reliability:
            try:
                self.reliability.stop()
            except Exception:
                pass
        self.root.after(300, self.root.destroy)


def main():
    root = tk.Tk()
    app  = TerminalChat(root)
    root.protocol("WM_DELETE_WINDOW", app._quit)
    root.mainloop()


if __name__ == "__main__":
    main()
