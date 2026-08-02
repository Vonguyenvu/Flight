from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config import POSTGRES_URL, postgres_properties

SILVER_TABLE = "silver_transform"
BRONZE_TABLE = "bronze_flights"


def build_spark():
    return (
        SparkSession.builder
        .appName("TransformSilverLocal")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = spark.read.jdbc(
        url=POSTGRES_URL,
        table=BRONZE_TABLE,
        properties=postgres_properties
    )

    silver_df = (
        bronze_df
        .drop("id")  
        .dropDuplicates(["icao24", "event_time"])
        .filter(col("icao24").isNotNull() & (col("icao24") != ""))
        .filter(col("longitude").isNotNull() & col("latitude").isNotNull())
        .filter(col("longitude").between(-180, 180) & col("latitude").between(-90, 90))
        .filter(col("altitude").isNotNull() & (col("altitude") >= 0))
        .filter(col("velocity").isNotNull() & (col("velocity") >= 0))
    )

    count = silver_df.count()
    print(f"Writing {count} rows to Silver table '{SILVER_TABLE}'")

    (silver_df.write
        .option("truncate", "true")
        .jdbc(
            url=POSTGRES_URL,
            table=SILVER_TABLE,
            mode="overwrite",
            properties=postgres_properties
        ))

    print("Silver transform complete.")


if __name__ == "__main__":
    main()