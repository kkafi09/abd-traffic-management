import csv
import json
import logging
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PRODUCER] %(message)s"
)
logger = logging.getLogger("TrafficProducer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic-events")
DATA_FILE_PATH = os.getenv("DATA_FILE_PATH", "/app/data/traffic_conditions_surabaya.csv")
STREAM_DELAY_SECONDS = float(os.getenv("STREAM_DELAY_SECONDS", "0.2"))
BATCH_EMIT_SIZE = int(os.getenv("BATCH_EMIT_SIZE", "5"))
LOOP_STREAM = os.getenv("LOOP_STREAM", "true").lower() == "true"


def get_kafka_producer(servers: str, max_retries: int = 30, retry_interval: int = 3) -> KafkaProducer:
    """Membuka koneksi ke Kafka Broker dengan mekanisme retry otomatis saat startup."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Mencoba menghubungkan ke Kafka ({servers}) - Percobaan {attempt}/{max_retries}...")
            producer = KafkaProducer(
                bootstrap_servers=servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                acks=1,
                retries=3
            )
            logger.info("Berhasil terhubung ke Kafka Broker!")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker belum siap. Menunggu {retry_interval} detik...")
            time.sleep(retry_interval)
        except Exception as e:
            logger.error(f"Error tak terduga saat koneksi ke Kafka: {e}")
            time.sleep(retry_interval)
    raise RuntimeError("Gagal terhubung ke Kafka setelah batas maksimum percobaan.")


def stream_traffic_data():
    """Membaca CSV kondisi lalu lintas Surabaya dan mem-publish event ke Kafka."""
    if not os.path.exists(DATA_FILE_PATH):
        logger.error(f"File data tidak ditemukan: {DATA_FILE_PATH}")
        return

    producer = get_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    
    total_sent = 0
    cycle = 1

    while True:
        logger.info(f"--- Memulai siklus streaming #{cycle} dari file: {DATA_FILE_PATH} ---")
        with open(DATA_FILE_PATH, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            batch = []
            
            for row in reader:
                # Format payload event
                try:
                    payload = {
                        "timestamp": row["timestamp"],
                        "segment_id": row["segment_id"],
                        "speed_kmh": float(row["speed_kmh"]) if row["speed_kmh"] else 0.0,
                        "volume_vehicles": int(float(row["volume_vehicles"])) if row["volume_vehicles"] else 0,
                        "occupancy_pct": float(row["occupancy_pct"]) if row["occupancy_pct"] else 0.0,
                        "congestion_index": float(row["congestion_index"]) if row["congestion_index"] else 0.0,
                        "condition": row.get("condition", "Unknown")
                    }
                except (ValueError, KeyError) as e:
                    logger.warning(f"Melewati baris data rusak/invalid: {row} - Error: {e}")
                    continue

                batch.append(payload)
                
                if len(batch) >= BATCH_EMIT_SIZE:
                    for event in batch:
                        producer.send(
                            topic=KAFKA_TOPIC,
                            key=event["segment_id"],
                            value=event
                        )
                    producer.flush()
                    total_sent += len(batch)
                    logger.info(
                        f"[Sent {total_sent} events] Segment: {batch[-1]['segment_id']} | "
                        f"Speed: {batch[-1]['speed_kmh']} km/h | Status: {batch[-1]['condition']}"
                    )
                    batch.clear()
                    time.sleep(STREAM_DELAY_SECONDS)

            # Kirim sisa batch jika ada
            if batch:
                for event in batch:
                    producer.send(topic=KAFKA_TOPIC, key=event["segment_id"], value=event)
                producer.flush()
                total_sent += len(batch)
                batch.clear()

        if not LOOP_STREAM:
            logger.info("Streaming selesai (mode non-loop).")
            break

        cycle += 1
        logger.info("File selesai dibaca. Melanjutkan replay loop untuk simulasi live stream...")
        time.sleep(2)


if __name__ == "__main__":
    stream_traffic_data()
