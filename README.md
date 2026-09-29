# Surabaya Real-Time Traffic Stream ETL Pipeline (Kafka KRaft)

Pipeline **Stream ETL** berbasis **Apache Kafka (KRaft Mode)**, **Python Stream Worker**, dan **PostgreSQL** untuk memproses data kondisi lalu lintas Kota Surabaya secara *real-time* (*event-by-event*).

---

## 📌 Arsitektur & Keselarasan Gambar Presentasi

```
[ DATA SOURCES ]           [ STREAM INGESTION ]             [ STREAM ETL PROCESSING ]          [ STORAGE & OUTPUT ]
  (Sensors/GPS)   ------>   (Apache Kafka Broker)  ------>   (Stream ETL Consumer)   ------>  (PostgreSQL & Dashboard)
  ┌─────────────┐            ┌──────────────────┐             ┌─────────────────────┐          ┌──────────────────────┐
  │  PRODUCER   │ --publish->│  Topic:          │ --consume-->│      CONSUMER       │ --load-->│ dwh.realtime_events  │
  │ (Simulator) │            │  traffic-events  │             │ (Extract/Transform) │          │ dwh.window_summary   │
  └─────────────┘            └──────────────────┘             └─────────────────────┘          └──────────────────────┘
```

1. **Data Sources & Stream Ingestion**:
   - Simulator sensor IoT & GPS (`traffic-producer`) membaca rekaman sensor lalu lintas Surabaya secara bertahap dan mem-publish pesan ke topik Kafka `traffic-events`.
   - Menggunakan **Apache Kafka 3.7 KRaft Mode** (arsitektur modern tanpa dependensi ZooKeeper).
2. **Stream ETL Processing (Consumer)**:
   - **Extract**: Menangkap stream data dari topik Kafka dengan latensi sub-detik.
   - **Transform**: 
     - Melakukan validasi tipe data dan jangkauan nilai (*speed*, *volume*, *occupancy*, *congestion_index*).
     - Menghilangkan duplikasi (*in-memory deduplication*).
     - Menentukan klasifikasi kemacetan (`is_congested`).
   - **Integration**: Menggabungkan data sensor dengan tabel dimensi master jalan (`dwh.road_segments`) menggunakan `segment_id` secara in-memory.
3. **Storage & Output**:
   - Menyimpan raw cleaned stream ke `dwh.realtime_traffic_events`.
   - Melakukan agregasi mikro (*tumbling window* 10 menit) ke `dwh.realtime_window_summary` untuk konsumsi dashboard *real-time*.

---

## ⚖️ Perbandingan Batch ETL (Week 3) vs Stream ETL (Week 4)

| Aspek | Week 3 (Batch ETL) | Week 4 (Stream ETL) |
| :--- | :--- | :--- |
| **Orkestrator / Broker** | Apache Airflow + PySpark | Apache Kafka (KRaft Mode) |
| **Frekuensi Eksekusi** | Terjadwal berkala (harian / per batch file) | Terus-menerus (*continuous, event-by-event*) |
| **Latensi Data** | Tinggi (menit hingga jam) | Sangat Rendah (< 1 detik / *near real-time*) |
| **Volume Pemrosesan** | Memproses dataset utuh sekaligus | Memproses rekaman data satu per satu / micro-batch |
| **Penyimpanan Target** | Ringkasan harian (`dwh.daily_traffic_summary`) | Event langsung & ringkasan window (`realtime_traffic_events`) |

---

## 🚀 Cara Menjalankan Pipeline

### 1. Jalankan Seluruh Service Docker

Buka terminal pada folder `Week4`:
```bash
docker compose up -d --build
```

### 2. Cek Status Container

```bash
docker compose ps
```
Pastikan semua service (`kafka`, `kafka-ui`, `stream-postgres`, `traffic-producer`, `stream-consumer`) berstatus *Up / Healthy*.

### 3. Monitoring Live Stream via Kafka UI (Bagus untuk Demo)

Buka browser dan akses:
- **URL**: [http://localhost:8085](http://localhost:8085)
- Klik menu **Topics** $\rightarrow$ **traffic-events** $\rightarrow$ **Messages**.
- Anda akan melihat event kondisi lalu lintas Surabaya terus bertambah secara *live*.

---

## 🔍 Verifikasi Data di PostgreSQL

Hubungkan ke PostgreSQL (misal via DBeaver, pgAdmin, atau terminal):
- **Host**: `localhost`
- **Port**: `5434` *(dibedakan dari port 5433 Week 3 agar tidak konflik)*
- **Database**: `traffic_warehouse`
- **User / Password**: `de_user` / `password123`

### Contoh Query Verifikasi:

```sql
-- 1. Cek jumlah event streaming yang berhasil masuk
SELECT COUNT(*) AS total_realtime_events FROM dwh.realtime_traffic_events;

-- 2. Cek 10 event lalu lintas terbaru beserta nama jalannya
SELECT 
    event_timestamp,
    segment_id,
    road_name,
    speed_kmh,
    condition,
    is_congested,
    processed_at
FROM dwh.realtime_traffic_events
ORDER BY processed_at DESC
LIMIT 10;

-- 3. Cek agregasi window 10 menit per segmen jalan
SELECT 
    window_start,
    window_end,
    road_name,
    total_events,
    avg_speed_kmh,
    total_volume_vehicles,
    dominant_condition
FROM dwh.realtime_window_summary
ORDER BY window_start DESC, total_events DESC
LIMIT 10;
```

---

## 💡 Panduan Menjawab Pertanyaan Dosen Saat Presentasi

1. **T: Mengapa tidak memakai ZooKeeper?**
   - *Jawab*: Kami menggunakan arsitektur modern Apache Kafka KRaft (*Kafka Raft Metadata mode*), di mana konsensus metadata dikelola langsung di dalam node Kafka broker sendiri. Ini mengurangi *overhead resource*, menyederhanakan konfigurasi container, dan menjadi standar resmi Kafka versi 3+.

2. **T: Bagaimana cara melakukan data enrichment (join) pada streaming?**
   - *Jawab*: Data master segmen jalan dimuat sekali ke database dan di-cache dalam memori Consumer pada saat *bootstrap*. Setiap event Kafka yang masuk hanya membawa `segment_id`, lalu Consumer memetakan nama jalan secara in-memory sebelum di-insert ke PostgreSQL dengan latensi < 1 detik.

3. **T: Bagaimana mencegah data duplikat?**
   - *Jawab*: Consumer menerapkan *sliding cache deduplication* berbasis kombinasi unik `timestamp + segment_id`, sehingga jika simulator atau jaringan mengirim ulang data yang sama, data tersebut tidak akan diduplikasi di tabel PostgreSQL.
