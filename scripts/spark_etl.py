import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, round as _round, sum as _sum, count as _count, avg as _avg,
    when, lit, concat, date_format, lpad, hour, minute
)


def run_traffic_etl():
    data_dir = "/opt/airflow/data"

    # inisialisasi spark session dan konfigurasi driver postgresql
    builder = SparkSession.builder.appName("SurabayaTrafficBatchETL")
    jar_path = "/home/airflow/.ivy2/jars/org.postgresql_postgresql-42.6.0.jar"
    if os.path.exists(jar_path):
        builder = builder.config("spark.jars", jar_path)
    else:
        builder = builder.config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")

    spark = builder.getOrCreate()

    traffic_path = os.path.join(data_dir, "traffic_conditions_surabaya.csv")
    roads_path = os.path.join(data_dir, "road_segments_surabaya.csv")

    # baca data csv kondisi lalu lintas dan segmen jalan
    df_traffic = spark.read.csv(traffic_path, header=True, inferSchema=True)
    df_roads = spark.read.csv(roads_path, header=True, inferSchema=True)

    # gabungkan data lalu lintas dengan data segmen jalan
    df_roads_sel = df_roads.select("segment_id", "road_name")
    df_joined = df_traffic.join(df_roads_sel, on="segment_id", how="left")

    # transformasi data: format tanggal, tangani null, dan generate batch_id per interval 10 menit
    df_transformed = df_joined \
        .withColumn("traffic_date", to_date(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn(
            "condition",
            when(col("condition").isNull() | (col("condition") == ""), "Unknown")
            .otherwise(col("condition"))
        ) \
        .withColumn(
            "batch_id",
            concat(
                lit("BATCH_"),
                date_format(col("timestamp"), "yyyyMMdd_"),
                lpad(((hour(col("timestamp")) * 6) + (minute(col("timestamp")) / 10)).cast("int").cast("string"), 3, "0")
            )
        )

    # agregasi metrik lalu lintas harian per tanggal, segmen jalan, dan kondisi
    df_summary = df_transformed.groupBy("traffic_date", "segment_id", "road_name", "condition") \
        .agg(
            _count("*").alias("total_records"),
            _round(_avg("speed_kmh"), 2).alias("avg_speed_kmh"),
            _sum("volume_vehicles").alias("total_volume_vehicles"),
            _round(_avg("occupancy_pct"), 2).alias("avg_occupancy_pct"),
            _round(_avg("congestion_index"), 2).alias("avg_congestion_index")
        ) \
        .withColumn("batch_id", concat(lit("BATCH_"), date_format(col("traffic_date"), "yyyyMMdd")))

    jdbc_url = "jdbc:postgresql://target-postgres:5432/bank_warehouse"
    db_properties = {
        "user": "de_user",
        "password": "password123",
        "driver": "org.postgresql.Driver"
    }

    # simpan hasil agregasi ke tabel staging postgresql
    df_summary.write.jdbc(
        url=jdbc_url,
        table="staging_traffic_summary",
        mode="overwrite",
        properties=db_properties
    )

    # hentikan spark session
    spark.stop()

if __name__ == "__main__":
    run_traffic_etl()
