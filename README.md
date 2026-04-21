# SecureChat v1.0
**Aplikasi Chat Private berbasis TCP & UDP — Python 3**

---

## Arsitektur Sistem

```
+-------------------+       +-------------------+
|     Client A      |       |     Client B      |
| (Sender/Receiver) | <---> | (Sender/Receiver) |
+-------------------+       +-------------------+
          |                           |
          | TCP 5555  (Chat + ACK)    |
          | UDP 5556  (Heartbeat)     |
          +---------------------------+
                   LAN / Loopback
```

| Layer              | Teknologi              | Fungsi                              |
|--------------------|------------------------|-------------------------------------|
| **Data Plane**     | TCP port 5555          | Pengiriman pesan reliable + ordered |
| **Control Plane**  | UDP port 5556          | Heartbeat keep-alive                |
| **Reliability**    | Seq + ACK + Retry      | Jaminan pengiriman + exponential backoff |
| **Persistence**    | SQLite                 | History chat + unsent queue         |
| **Security**       | HMAC-SHA256 + Whitelist| Auth password + IP filtering        |
| **Queue**          | PriorityQueue          | Non-blocking decoupling UI↔Network  |

---

## Struktur File

```
chat_app/
├── main.py            # Entry point + UI loop
├── tcp_handler.py     # TCP Server & Client non-blocking
├── udp_handler.py     # UDP Heartbeat sender/receiver/watchdog
├── message_queue.py   # Internal PriorityQueue (non-blocking)
├── security.py        # IP whitelist + HMAC-SHA256 auth
├── persistence.py     # SQLite: chat history + unsent queue
├── reliability.py     # Seq number, ACK tracker, exponential backoff
├── config.py          # Semua konfigurasi (port, password, timeout)
└── utils/
    └── logger.py      # Rotating file + console logger
```

---

## Cara Penggunaan

### Prasyarat
- Python 3.8+
- Tidak memerlukan library eksternal (hanya standard library)

### Menjalankan di Jaringan Lokal (LAN)

**Terminal 1 — Server (misal IP: 192.168.1.10):**
```bash
cd chat_app
python main.py server Alice
```

**Terminal 2 — Client (dari komputer lain di LAN):**
```bash
cd chat_app
python main.py client Bob 192.168.1.10
```

### Menjalankan di Komputer yang Sama (Testing)

**Terminal 1:**
```bash
python main.py server Alice
```

**Terminal 2:**
```bash
python main.py client Bob 127.0.0.1
```

---

## Konfigurasi (`config.py`)

| Parameter              | Default         | Keterangan                              |
|------------------------|-----------------|------------------------------------------|
| `TCP_PORT`             | 5555            | Port TCP untuk chat                      |
| `UDP_PORT`             | 5556            | Port UDP untuk heartbeat                 |
| `PASSWORD`             | rahasiaLAN123   | **Ganti ini** sebelum digunakan!         |
| `IP_WHITELIST`         | `[]`            | Kosong = semua IP diizinkan              |
| `ACK_TIMEOUT_SEC`      | 3               | Timeout sebelum retry pertama            |
| `MAX_RETRIES`          | 5               | Maks percobaan retransmit                |
| `HEARTBEAT_INTERVAL_SEC` | 5             | Interval kirim heartbeat UDP             |
| `HEARTBEAT_MISS_LIMIT` | 3               | Batas heartbeat hilang → disconnect      |

---

## Protokol TCP (Binary + JSON)

```
[Header 16 byte] + [JSON Payload]

Header:
  Byte 0-3  : Sequence Number  (uint32, big-endian)
  Byte 4-11 : Timestamp        (int64,  big-endian, unix ms)
  Byte 12   : Message Type     (uint8)
                0 = CHAT
                1 = ACK
                2 = AUTH
                3 = AUTH_OK
                4 = AUTH_FAIL
                5 = DISCONNECT
  Byte 13-15: Payload Length   (uint24, big-endian)
```

---

## Mekanisme Reliability

1. **Setiap pesan** mendapat Sequence Number unik.
2. **Penerima** wajib mengirim ACK dengan seq yang sama.
3. **Jika tidak ada ACK** dalam `ACK_TIMEOUT_SEC` detik → pesan di-retry.
4. **Exponential backoff**: 3s → 6s → 12s → 24s → 48s (max 60s).
5. **Setelah MAX_RETRIES** percobaan → pesan dinyatakan gagal.
6. **Saat offline**, pesan disimpan ke SQLite (unsent_queue).
7. **Saat reconnect**, semua unsent messages dikirim ulang otomatis.

---

## Perintah Chat

| Perintah    | Fungsi                         |
|-------------|-------------------------------|
| `/history`  | Tampilkan 20 pesan terakhir    |
| `/status`   | Lihat status koneksi & queue   |
| `/quit`     | Keluar dari aplikasi           |
| *(teks)*    | Kirim pesan ke peer            |

---

## Output File

| File              | Isi                                    |
|-------------------|----------------------------------------|
| `chat_history.db` | SQLite: riwayat + unsent queue         |
| `chat_app.log`    | Log lengkap (rotating, maks 5MB × 3)  |

---

## Pengembangan Lebih Lanjut

- [ ] **End-to-end encryption** (AES-256 pada payload JSON)
- [ ] **Multi-client** (ganti arsitektur ke N-client dengan thread pool)
- [ ] **File transfer** via TCP stream terpisah
- [ ] **GUI** menggunakan `tkinter` atau `PyQt6`
- [ ] **TLS** untuk transport layer security penuh
