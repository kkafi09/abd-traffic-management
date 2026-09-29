import csv
import json
import logging
import os
import time
from collections import OrderedDict
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import psycopg2
from psycopg2.extras import execute_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [CONSUMER-ETL] %(message)s"
)
logger = logging.getLogger("TrafficStreamConsumer")

# Konfigurasi Environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "surabaya-stream-etl-group")

PG_HOST = os.getenv("POSTGRES_HOST", "target-postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "traffic_warehouse")
PG_USER = os.getenv("POSTGRES_USER", "de_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "password123")

ROADS_DATA_PATH = os.getenv("ROADS_DATA_PATH", "/app/data/road_segments_surabaya.csv")
BATCH_FLUSH_SIZE = int(os.getenv("BATCH_FLUSH_SIZE", "20"))
FLUSH_INTERVAL_SEC = float(os.getenv("FLUSH_INTERVAL_SEC", "1.0"))

# In-memory lookup & deduplikasi
road_master_cache = {}
seen_events = OrderedDict()  # LRU deduplication cache (max 10,000 keys)
MAX_SEEN_CACHE = 10000


def get_postgres_connection(max_retries: int = 30, retry_interval: int = 3):
    """Membuka koneksi ke PostgreSQL dengan auto-retry."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Menghubungkan ke PostgreSQL ({PG_HOST}:{PG_PORT}/{PG_DB}) - Percobaan {attempt}/{max_retries}...")
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASS
            )
            conn.autocommit = False
            logger.info("Berhasil terhubung ke PostgreSQL!")
            return conn
        except Exception as e:
            logger.warning(f"PostgreSQL belum siap ({e}). Menunggu {retry_interval} detik...")
            time.sleep(retry_interval)
    raise RuntimeError("Gagal terhubung ke PostgreSQL setelah batas percobaan.")


def sync_road_master(conn):
    """Memastikan data master dwh.road_segments terisi dan di-cache ke memori."""
    global road_master_cache
    cursor = conn.cursor()
    
    # 1. Cek isi tabel di PostgreSQL
    cursor.execute("SELECT COUNT(*) FROM dwh.road_segments;")
    count = cursor.fetchone()[0]
    
    if count == 0 and os.path.exists(ROADS_DATA_PATH):
        logger.info(f"Tabel dwh.road_segments kosong. Melakukan inisialisasi dari {ROADS_DATA_PATH}...")
        with open(ROADS_DATA_PATH, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            insert_query = """
                INSERT INTO dwh.road_segments (
                    segment_id, road_code, road_name, segment_order,
                    start_lat, start_lon, end_lat, end_lon,
                    length_m, point_count, geometry_geojson
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (segment_id) DO NOTHING;
            """
            rows = []
            for r in reader:
                rows.append((
                    r["segment_id"],
                    r.get("road_code"),
                    r.get("road_name"),
                    int(r["segment_order"]) if r.get("segment_order") else None,
                    float(r["start_lat"]) if r.get("start_lat") else None,
                    float(r["start_lon"]) if r.get("start_lon") else None,
                    float(r["end_lat"]) if r.get("end_lat") else None,
                    float(r["end_lon"]) if r.get("end_lon") else None,
                    int(r["length_m"]) if r.get("length_m") else None,
                    int(r["point_count"]) if r.get("point_count") else None,
                    r.get("geometry_geojson")
                ))
            execute_batch(cursor, insert_query, rows)
            conn.commit()
            logger.info(f"Berhasil memuat {len(rows)} data master segmen jalan ke PostgreSQL.")

    # 2. Baca seluruh master ke memory cache untuk instant lookup
    cursor.execute("SELECT segment_id, road_name FROM dwh.road_segments;")
    for row in cursor.fetchall():
        road_master_cache[row[0]] = row[1]
    
    cursor.close()
    logger.info(f"Cache master segmen jalan siap dengan {len(road_master_cache)} segmen.")


def get_kafka_consumer(servers: str, topic: str, group_id: str, max_retries: int = 30, retry_interval: int = 3) -> KafkaConsumer:
    """Membuka Kafka Consumer dengan auto-retry."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Menghubungkan Kafka Consumer ({servers}) - Percobaan {attempt}/{max_retries}...")
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=servers.split(","),
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000
            )
            logger.info(f"Consumer berhasil berlangganan ke topik: '{topic}'!")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker belum siap. Menunggu {retry_interval} detik...")
            time.sleep(retry_interval)
        except Exception as e:
            logger.error(f"Error koneksi consumer: {e}")
            time.sleep(retry_interval)
    raise RuntimeError("Gagal menginisialisasi Kafka Consumer.")


def is_duplicate(dedup_key: str) -> bool:
    """Mengecek duplikasi event berdasarkan sliding cache."""
    if dedup_key in seen_events:
        return True
    seen_events[dedup_key] = True
    if len(seen_events) > MAX_SEEN_CACHE:
        seen_events.popitem(last=False)
    return False


def flush_events_to_db(conn, events_batch):
    """Menyimpan batch event yang telah di-transform ke PostgreSQL."""
    if not events_batch:
        return

    cursor = conn.cursor()
    insert_events_sql = """
        INSERT INTO dwh.realtime_traffic_events (
            event_timestamp, segment_id, road_name, speed_kmh,
            volume_vehicles, occupancy_pct, congestion_index,
            condition, is_congested
        ) VALUES (
            %(event_timestamp)s, %(segment_id)s, %(road_name)s, %(speed_kmh)s,
            %(volume_vehicles)s, %(occupancy_pct)s, %(congestion_index)s,
            %(condition)s, %(is_congested)s
        );
    """

    upsert_window_sql = """
        INSERT INTO dwh.realtime_window_summary (
            window_start, window_end, segment_id, road_name,
            total_events, avg_speed_kmh, total_volume_vehicles,
            avg_occupancy_pct, avg_congestion_index, dominant_condition,
            last_updated_at
        ) VALUES (
            %(window_start)s, %(window_end)s, %(segment_id)s, %(road_name)s,
            1, %(speed_kmh)s, %(volume_vehicles)s, %(occupancy_pct)s,
            %(congestion_index)s, %(condition)s, NOW()
        )
        ON CONFLICT (window_start, segment_id) DO UPDATE SET
            total_events = dwh.realtime_window_summary.total_events + 1,
            avg_speed_kmh = ROUND(((dwh.realtime_window_summary.avg_speed_kmh * dwh.realtime_window_summary.total_events + EXCLUDED.avg_speed_kmh) / (dwh.realtime_window_summary.total_events + 1))::numeric, 2),
            total_volume_vehicles = dwh.realtime_window_summary.total_volume_vehicles + EXCLUDED.total_volume_vehicles,
            avg_occupancy_pct = ROUND(((dwh.realtime_window_summary.avg_occupancy_pct * dwh.realtime_window_summary.total_events + EXCLUDED.avg_occupancy_pct) / (dwh.realtime_window_summary.total_events + 1))::numeric, 2),
            avg_congestion_index = ROUND(((dwh.realtime_window_summary.avg_congestion_index * dwh.realtime_window_summary.total_events + EXCLUDED.avg_congestion_index) / (dwh.realtime_window_summary.total_events + 1))::numeric, 2),
            dominant_condition = EXCLUDED.dominant_condition,
            last_updated_at = NOW();
    """

    try:
        execute_batch(cursor, insert_events_sql, events_batch)
        execute_batch(cursor, upsert_window_sql, events_batch)
        conn.commit()
        logger.info(f"[LOADED] Berhasil menyimpan {len(events_batch)} events ke PostgreSQL (realtime_events & window_summary).")
    except Exception as e:
        conn.rollback()
        logger.error(f"Gagal menyimpan batch ke database: {e}")
    finally:
        cursor.close()


def run_stream_etl():
    """Alur utama Stream ETL: Extract -> Transform -> Integration -> Load."""
    conn = get_postgres_connection()
    sync_road_master(conn)
    consumer = get_kafka_consumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID)

    logger.info(">>> STREAM ETL WORKER SIAP & MENDENGARKAN EVENT KAFKA <<<")

    batch = []
    last_flush_time = time.time()
    total_processed = 0

    while True:
        try:
            records = consumer.poll(timeout_ms=500)
            now = time.time()

            for tp, messages in records.items():
                for msg in messages:
                    raw = msg.value

                    # ==================== 1. EXTRACT ====================
                    ts_str = raw.get("timestamp")
                    segment_id = raw.get("segment_id")

                    if not ts_str or not segment_id:
                        continue

                    # ==================== 2. TRANSFORM ==================
                    # Deduplikasi
                    dedup_key = f"{ts_str}_{segment_id}"
                    if is_duplicate(dedup_key):
                        continue

                    # Validasi & normalisasi
                    try:
                        event_timestamp = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        continue

                    speed_kmh = max(0.0, float(raw.get("speed_kmh", 0.0)))
                    volume_vehicles = max(0, int(raw.get("volume_vehicles", 0)))
                    occupancy_pct = max(0.0, min(100.0, float(raw.get("occupancy_pct", 0.0))))
                    congestion_index = max(0.0, min(100.0, float(raw.get("congestion_index", 0.0))))
                    condition = raw.get("condition") or "Unknown"

                    # Enrichment status kemacetan
                    is_congested = (congestion_index >= 50.0) or (condition in ["Padat", "Macet"])

                    # Window interval 10 menit (sliding / tumbling window)
                    minute_bucket = (event_timestamp.minute // 10) * 10
                    window_start = event_timestamp.replace(minute=minute_bucket, second=0, microsecond=0)
                    window_end = datetime.fromtimestamp(window_start.timestamp() + 600)

                    # ================== 3. INTEGRATION ==================
                    # Join menggunakan segment_id dengan data master segmen jalan
                    road_name = road_master_cache.get(segment_id, "Unknown Road")

                    enriched_event = {
                        "event_timestamp": event_timestamp,
                        "segment_id": segment_id,
                        "road_name": road_name,
                        "speed_kmh": round(speed_kmh, 2),
                        "volume_vehicles": volume_vehicles,
                        "occupancy_pct": round(occupancy_pct, 2),
                        "congestion_index": round(congestion_index, 2),
                        "condition": condition,
                        "is_congested": is_congested,
                        "window_start": window_start,
                        "window_end": window_end
                    }

                    batch.append(enriched_event)
                    total_processed += 1

            # ==================== 4. LOAD =======================
            if len(batch) >= BATCH_FLUSH_SIZE or (batch and (now - last_flush_time >= FLUSH_INTERVAL_SEC)):
                flush_events_to_db(conn, batch)
                batch.clear()
                last_flush_time = now

        except KeyboardInterrupt:
            logger.info("Menghentikan Stream ETL Consumer...")
            break
        except Exception as e:
            logger.error(f"Error pada loop streaming: {e}")
            time.sleep(1)

    if batch:
        flush_events_to_db(conn, batch)

    consumer.close()
    conn.close()
    logger.info("Consumer ditutup dengan aman.")


if __name__ == "__main__":
    run_stream_etl()
