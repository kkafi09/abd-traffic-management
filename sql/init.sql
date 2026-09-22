CREATE TABLE IF NOT EXISTS road_segments (
    segment_id VARCHAR(50) PRIMARY KEY,
    road_code VARCHAR(50),
    road_name VARCHAR(150),
    segment_order INT,
    start_lat NUMERIC(10, 6),
    start_lon NUMERIC(10, 6),
    end_lat NUMERIC(10, 6),
    end_lon NUMERIC(10, 6),
    length_m INT,
    point_count INT
);

CREATE TABLE IF NOT EXISTS daily_traffic_summary (
    traffic_date DATE NOT NULL,
    segment_id VARCHAR(50) NOT NULL,
    road_name VARCHAR(150) NOT NULL,
    condition VARCHAR(50) NOT NULL,
    batch_id VARCHAR(50),
    total_records INT NOT NULL,
    avg_speed_kmh NUMERIC(6, 2) NOT NULL,
    total_volume_vehicles BIGINT NOT NULL,
    avg_occupancy_pct NUMERIC(5, 2) NOT NULL,
    avg_congestion_index NUMERIC(5, 2) NOT NULL,
    PRIMARY KEY (traffic_date, segment_id, condition)
);

ALTER TABLE daily_traffic_summary ADD COLUMN IF NOT EXISTS batch_id VARCHAR(50);
