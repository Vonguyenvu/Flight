from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, countDistinct, floor, concat, lit

from config import POSTGRES_URL, postgres_properties

SILVER_TABLE = "silver_transform"
GOLD_HOURLY_TABLE = "gold_analysis"

TIME_WINDOW = "1 hour"

def build_spark():
    return (
        SparkSession.builder
        .appName("AnalysisGoldLocal")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.jdbc(
        url=POSTGRES_URL,
        table=SILVER_TABLE,
        properties=postgres_properties
    )
    
    airborne_df = silver_df.filter(col("on_ground") == False).filter(col("altitude").isNotNull())
    
    hourly_df = (
        airborne_df
        .groupBy(window(col("event_time"), TIME_WINDOW))
        .agg(countDistinct("icao24").alias("unique_aircraft_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "unique_aircraft_count"
        )
        .orderBy("window_start")
    )


    print(f"Hourly rows: {hourly_df.count()}")
    hourly_df.show(20, truncate=False)


    (hourly_df.write
        .jdbc(url=POSTGRES_URL, table=GOLD_HOURLY_TABLE,
              mode="overwrite", properties=postgres_properties))


    print(f"Gold tables updated: {GOLD_HOURLY_TABLE}")


if __name__ == "__main__":
    main()