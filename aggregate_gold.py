from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, when

from config import POSTGRES_JDBC_URL, postgres_properties

SILVER_TABLE = "silver_flights"
GOLD_CONGESTION_TABLE = "gold_airspace_congestion"
GOLD_ALERTS_TABLE = "gold_low_altitude_alerts"


def build_spark():
    return (
        SparkSession.builder
        .appName("AggregateGoldLocal")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.jdbc(
        url=POSTGRES_JDBC_URL,
        table=SILVER_TABLE,
        properties=postgres_properties
    )

    # Mật độ không phận theo cửa sổ 5 phút, phân loại mức độ đông đúc
    congestion_df = (
        silver_df
        .groupBy(window(col("event_time"), "5 minutes"))
        .agg(count("*").alias("aircraft_count"))
        .withColumn(
            "congestion_level",
            when(col("aircraft_count") > 15, "HIGH")
            .when(col("aircraft_count") > 8, "MEDIUM")
            .otherwise("LOW")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "aircraft_count",
            "congestion_level"
        )
        .orderBy("window_start")
    )

    # Cảnh báo máy bay bay thấp bất thường (dưới 500m, chưa đáp)
    anomalies_df = (
        silver_df
        .filter((col("altitude") < 500) & (col("on_ground") == False))
        .select("icao24", "callsign", "altitude", "event_time")
    )

    print(f"Congestion windows: {congestion_df.count()}")
    print(f"Low altitude alerts: {anomalies_df.count()}")

    (congestion_df.write
        .jdbc(url=POSTGRES_JDBC_URL, table=GOLD_CONGESTION_TABLE,
              mode="overwrite", properties=postgres_properties))

    (anomalies_df.write
        .jdbc(url=POSTGRES_JDBC_URL, table=GOLD_ALERTS_TABLE,
              mode="overwrite", properties=postgres_properties))

    print("Gold tables updated: airspace congestion and low-altitude alerts.")


if __name__ == "__main__":
    main()