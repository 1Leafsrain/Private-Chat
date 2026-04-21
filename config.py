"""
config.py - Konfigurasi Aplikasi Chat
Ubah sesuai kebutuhan sebelum menjalankan aplikasi.
"""

# ─── Network ────────────────────────────────────────────────────────────────
TCP_PORT        = 5555      # Port untuk chat messages (TCP)
UDP_PORT        = 5556      # Port untuk heartbeat / control (UDP)
BUFFER_SIZE     = 65536     # Maksimum ukuran buffer socket
SERVER_HOST     = "0.0.0.0" # Alamat bind server (0.0.0.0 = semua interface)

# ─── Auth & Security ────────────────────────────────────────────────────────
# Password per-user disimpan di database (bukan config).
# Daftar IP yang diizinkan terhubung (kosongkan list untuk menonaktifkan whitelist)
IP_WHITELIST    = []                # contoh: ["192.168.1.10", "192.168.1.11"]

# ─── Reliability ────────────────────────────────────────────────────────────
ACK_TIMEOUT_SEC         = 3        # Waktu tunggu ACK pertama (detik)
MAX_RETRIES             = 5        # Maksimum percobaan retransmit
RECONNECT_INTERVAL_SEC  = 2        # Interval reconnect otomatis
HEARTBEAT_INTERVAL_SEC  = 5        # Interval kirim heartbeat UDP
HEARTBEAT_MISS_LIMIT    = 3        # Berapa kali heartbeat boleh hilang sebelum disconnect

# ─── Persistence ────────────────────────────────────────────────────────────
DB_PATH = "chat_history.db"        # File SQLite untuk history + unsent queue

# ─── Logging ────────────────────────────────────────────────────────────────
LOG_FILE        = "chat_app.log"
LOG_MAX_BYTES   = 5 * 1024 * 1024  # 5 MB per file
LOG_BACKUP_COUNT = 3

# ─── UI ─────────────────────────────────────────────────────────────────────
APP_NAME    = "SecureChat v2.0"
PROMPT      = ">> "
