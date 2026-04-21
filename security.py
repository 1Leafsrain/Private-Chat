"""
security.py - IP Whitelist + Per-User Password Hashing
Setiap user memiliki salt unik yang di-generate saat registrasi.
"""
import hashlib
import hmac
import secrets
import ipaddress
from config import IP_WHITELIST
from utils.logger import get_logger

log = get_logger("security")


def generate_salt() -> str:
    """Generate salt acak 32-karakter hex untuk setiap user baru."""
    return secrets.token_hex(16)


def hash_password(password: str, salt: str) -> str:
    """Return HMAC-SHA256 hex digest dari password menggunakan salt unik per user."""
    return hmac.new(
        salt.encode("utf-8"),
        password.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


def verify_password(password: str, salt: str, stored_hash: str) -> bool:
    """Verifikasi password candidate terhadap stored hash."""
    candidate_hash = hash_password(password, salt)
    return hmac.compare_digest(candidate_hash, stored_hash)


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
