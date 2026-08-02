from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, countDistinct, when

from config import POSTGRES_URL, postgres_properties

SILVER_TABLE = "silver_transform"
GOLD_TABLE = "gold_analysis"

# Kích thước cửa sổ thời gian để nhóm dữ liệu
TIME_WINDOW = "15 minutes"


def build_spark():
    return (
        SparkSession.builder
        .appName("AggregateGoldLocal")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .getOrCreate()
    )


def add_altitude_bucket(df):
    """Phân nhóm độ cao thành các dải dễ đọc, dựa trên giai đoạn bay thực tế."""
    return df.withColumn(
        "altitude_bucket",
        when(col("altitude") < 1000, "0-1000m (Approach/Departure)")
        .when(col("altitude") < 3000, "1000-3000m")
        .when(col("altitude") < 6000, "3000-6000m")
        .when(col("altitude") < 10000, "6000-10000m")
        .otherwise("10000m+ (Cruise)")
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.jdbc(
        url=POSTGRES_URL,
        table=SILVER_TABLE,
        properties=postgres_properties
    )

    # Chỉ tính mật độ cho máy bay đang bay (loại on_ground để tránh nhiễu số liệu độ cao ~0)
    airborne_df = silver_df.filter(col("on_ground") == False).filter(col("altitude").isNotNull())

    bucketed_df = add_altitude_bucket(airborne_df)

    density_df = (
        bucketed_df
        .groupBy(window(col("event_time"), TIME_WINDOW), "altitude_bucket")
        .agg(countDistinct("icao24").alias("unique_aircraft_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "altitude_bucket",
            "unique_aircraft_count"
        )
        .orderBy("window_start", "altitude_bucket")
    )

    print(f"Density rows (time window x altitude bucket): {density_df.count()}")
    density_df.show(20, truncate=False)

    (density_df.write
        .jdbc(url=POSTGRES_URL, table=GOLD_TABLE,
              mode="overwrite", properties=postgres_properties))

    print(f"Gold table updated: {GOLD_TABLE}")


if __name__ == "__main__":
    main()