-- Inisialisasi schema dwh dan staging
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- Tabel master segmen jalan Surabaya (Dimensi / Lookup Table)
CREATE TABLE IF NOT EXISTS dwh.road_segments (
    segment_id VARCHAR(50) PRIMARY KEY,
    road_code VARCHAR(50),
    road_name VARCHAR(150),
    segment_order INT,
    start_lat NUMERIC(10, 6),
    start_lon NUMERIC(10, 6),
    end_lat NUMERIC(10, 6),
    end_lon NUMERIC(10, 6),
    length_m INT,
    point_count INT,
    geometry_geojson TEXT
);

-- Tabel fakta stream: menyimpan setiap incoming event yang telah dibersihkan & di-enrich
CREATE TABLE IF NOT EXISTS dwh.realtime_traffic_events (
    event_id BIGSERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    segment_id VARCHAR(50) NOT NULL,
    road_name VARCHAR(150),
    speed_kmh NUMERIC(6, 2) NOT NULL,
    volume_vehicles INT NOT NULL,
    occupancy_pct NUMERIC(5, 2) NOT NULL,
    congestion_index NUMERIC(5, 2) NOT NULL,
    condition VARCHAR(50) NOT NULL,
    is_congested BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stream_timestamp ON dwh.realtime_traffic_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_stream_segment ON dwh.realtime_traffic_events(segment_id);

-- Tabel agregasi micro-batch / window stream (untuk real-time traffic dashboard)
CREATE TABLE IF NOT EXISTS dwh.realtime_window_summary (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    segment_id VARCHAR(50) NOT NULL,
    road_name VARCHAR(150) NOT NULL,
    total_events INT NOT NULL,
    avg_speed_kmh NUMERIC(6, 2) NOT NULL,
    total_volume_vehicles BIGINT NOT NULL,
    avg_occupancy_pct NUMERIC(5, 2) NOT NULL,
    avg_congestion_index NUMERIC(5, 2) NOT NULL,
    dominant_condition VARCHAR(50) NOT NULL,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, segment_id)
);
