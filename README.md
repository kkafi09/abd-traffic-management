# Surabaya Traffic Batch ETL Pipeline

Pipeline Batch ETL berbasis **Apache Airflow**, **PySpark**, dan **PostgreSQL** untuk memproses dan mengagregasi data kondisi lalu lintas Kota Surabaya ke Data Warehouse (Praktikum ABD Week 3).

---

## Alur Pipeline

1. **Extract**: Membaca data langsung dari file `traffic_conditions_surabaya.csv` dan `road_segments_surabaya.csv`.
2. **Transform**:
   - Menggabungkan data dengan nama segmen jalan (left join).
   - Menghasilkan metadata `batch_id` secara programatik di PySpark (`BATCH_YYYYMMDD_000`, dst.).
   - Menghitung ringkasan harian (total records, rata-rata kecepatan, total volume kendaraan, okupansi, dan indeks kemacetan).
3. **Load**: Menulis ke tabel staging, kemudian melakukan idempotent upsert ke tabel `daily_traffic_summary` di PostgreSQL.

---

## Cara Menjalankan

1. Pastikan file CSV tersedia di dalam folder `data/`.
2. Jalankan container:
   ```bash
   docker compose up -d
   ```
3. Buka Airflow Web UI di browser:
   - URL: [http://localhost:8081](http://localhost:8081)
   - Login: `admin` / `admin`
4. Jalankan DAG `surabaya_traffic_batch_pipeline`.

---

## Database Target (PostgreSQL)

- Host: `localhost`
- Port: `5433`
- Database: `bank_warehouse`
- User / Password: `de_user` / `password123`
- Tabel Utama: `daily_traffic_summary`
