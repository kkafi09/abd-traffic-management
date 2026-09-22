from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'traffic_data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='surabaya_traffic_batch_pipeline',
    default_args=default_args,
    description='Pipeline Batch ETL Agregasi Kondisi Lalu Lintas Surabaya',
    schedule='@daily',
    catchup=False,
    tags=['traffic', 'pyspark', 'postgres']
) as dag:

    task_init_dwh = PostgresOperator(
        task_id='init_traffic_warehouse_schema',
        postgres_conn_id='postgres_traffic_conn',
        sql="""
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
        """
    )

    task_pyspark_transform = BashOperator(
        task_id='run_pyspark_traffic_transform',
        bash_command='python /opt/airflow/scripts/spark_etl.py'
    )

    task_upsert_dwh = PostgresOperator(
        task_id='upsert_to_traffic_warehouse',
        postgres_conn_id='postgres_traffic_conn',
        sql="""
            INSERT INTO daily_traffic_summary (
                traffic_date,
                segment_id,
                road_name,
                condition,
                batch_id,
                total_records,
                avg_speed_kmh,
                total_volume_vehicles,
                avg_occupancy_pct,
                avg_congestion_index
            )
            SELECT
                traffic_date,
                segment_id,
                road_name,
                condition,
                batch_id,
                total_records,
                avg_speed_kmh,
                total_volume_vehicles,
                avg_occupancy_pct,
                avg_congestion_index
            FROM staging_traffic_summary
            ON CONFLICT (traffic_date, segment_id, condition)
            DO UPDATE SET
                road_name = EXCLUDED.road_name,
                batch_id = EXCLUDED.batch_id,
                total_records = EXCLUDED.total_records,
                avg_speed_kmh = EXCLUDED.avg_speed_kmh,
                total_volume_vehicles = EXCLUDED.total_volume_vehicles,
                avg_occupancy_pct = EXCLUDED.avg_occupancy_pct,
                avg_congestion_index = EXCLUDED.avg_congestion_index;
        """
    )

    task_init_dwh >> task_pyspark_transform >> task_upsert_dwh

