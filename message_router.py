"""
message_router.py - Routing pesan antar user: online (kirim langsung) atau offline (buffer DB).
"""
from __future__ import annotations
from typing import TYPE_CHECKING
from tcp_handler import MSG_ROUTE, MSG_ACK
from persistence import save_offline_message, get_offline_messages, clear_offline_messages
from utils.logger import get_logger

if TYPE_CHECKING:
    from session_manager import SessionManager, ClientSession

log = get_logger("message_router")


class MessageRouter:
    """
    Mengirim pesan ke user tujuan.
    - Jika tujuan online: kirim langsung via socket.
    - Jika tujuan offline: simpan ke tabel offline_messages di DB.
    Server-level ACK dikirim ke pengirim setelah routing selesai.
    """

    def __init__(self, session_manager: "SessionManager"):
        self.session_mgr = session_manager

    def route(
        self,
        from_user: str,
        to_user: str,
        seq: int,
        message: str,
        sender_session: "ClientSession",
    ):
        """
        Kirim pesan dari from_user ke to_user.
        ACK dikirim ke sender setelah server berhasil menerima/meneruskan.
        """
        target = self.session_mgr.get_session(to_user)
        if target:
            try:
                target.send(MSG_ROUTE, {
                    "from":    from_user,
                    "message": message,
                    "seq":     seq,
                })
                log.info(f"Pesan seq={seq} dari '{from_user}' → '{to_user}' (online) terkirim.")
            except Exception as e:
                log.error(f"Gagal kirim ke '{to_user}': {e}. Disimpan ke offline buffer.")
                save_offline_message(seq, from_user, to_user, message)
        else:
            save_offline_message(seq, from_user, to_user, message)
            log.info(f"Pesan seq={seq} dari '{from_user}' → '{to_user}' (offline) di-buffer.")

        # Kirim ACK ke sender: server sudah menerima/meneruskan pesan
        try:
            sender_session.send(MSG_ACK, {"acked_seq": seq})
        except Exception as e:
            log.error(f"Gagal kirim ACK ke '{from_user}': {e}")

    def deliver_offline(self, username: str, session: "ClientSession"):
        """Kirim semua pesan offline yang tersimpan untuk user ini setelah login."""
        messages = get_offline_messages(username)
        if not messages:
            return
        log.info(f"Mengirim {len(messages)} pesan offline untuk '{username}'.")
        for msg in messages:
            try:
                session.send(MSG_ROUTE, {
                    "from":    msg["sender"],
                    "message": msg["message"],
                    "seq":     msg["seq"],
                })
            except Exception as e:
                log.error(f"Gagal kirim offline message seq={msg['seq']} ke '{username}': {e}")
                return  # Hentikan jika koneksi putus
        clear_offline_messages(username)
        log.info(f"Offline messages untuk '{username}' berhasil dikirim.")
