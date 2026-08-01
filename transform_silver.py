from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config import POSTGRES_JDBC_URL, postgres_properties

SILVER_TABLE = "silver_flights"
BRONZE_TABLE = "flight_states"


def build_spark():
    return (
        SparkSession.builder
        .appName("TransformSilverLocal")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = spark.read.jdbc(
        url=POSTGRES_JDBC_URL,
        table=BRONZE_TABLE,
        properties=postgres_properties
    )

    silver_df = (
        bronze_df
        .drop("id")  # id là BIGSERIAL chỉ có ở Bronze, Silver không có cột này
        .dropDuplicates(["icao24", "event_time"])
        .filter(col("callsign").isNotNull())
    )

    count = silver_df.count()
    print(f"Writing {count} rows to Silver table '{SILVER_TABLE}'")

    (silver_df.write
        .option("truncate", "true")  # chỉ xóa data, giữ nguyên schema/index đã tạo qua schema.sql
        .jdbc(
            url=POSTGRES_JDBC_URL,
            table=SILVER_TABLE,
            mode="overwrite",
            properties=postgres_properties
        ))

    print("Silver transform complete.")


if __name__ == "__main__":
    main()