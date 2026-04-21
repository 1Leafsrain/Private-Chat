"""
security.py - IP Whitelist + Password Authentication (salted SHA-256)
"""
import hashlib
import hmac
import os
import ipaddress
from config import PASSWORD, IP_WHITELIST
from utils.logger import get_logger

log = get_logger("security")

# Salt statis di-derive dari password agar deterministik antar dua peer.
# Untuk produksi, gunakan salt acak + key-exchange (Diffie-Hellman / TLS).
_SALT = hashlib.sha256(b"SecureChatSalt_" + PASSWORD.encode()).digest()


def hash_password(password: str) -> str:
    """Return salted HMAC-SHA256 hex digest dari password."""
    return hmac.new(_SALT, password.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_password(candidate: str) -> bool:
    """Verifikasi apakah candidate cocok dengan password konfigurasi."""
    expected = hash_password(PASSWORD)
    candidate_hash = hash_password(candidate)
    result = hmac.compare_digest(expected, candidate_hash)
    if not result:
        log.warning("Percobaan autentikasi gagal.")
    return result


def check_ip_whitelist(ip: str) -> bool:
    """
    Cek apakah IP diperbolehkan.
    Jika whitelist kosong, semua IP diizinkan (mode LAN terbuka).
    """
    if not IP_WHITELIST:
        return True
    try:
        addr = ipaddress.ip_address(ip)
        allowed = any(addr == ipaddress.ip_address(w) for w in IP_WHITELIST)
        if not allowed:
            log.warning(f"Koneksi ditolak dari IP {ip} (tidak ada di whitelist).")
        return allowed
    except ValueError:
        log.error(f"Format IP tidak valid: {ip}")
        return False


def get_password_hash() -> str:
    """Shortcut: kembalikan hash dari password konfigurasi (untuk dikirim saat login)."""
    return hash_password(PASSWORD)
