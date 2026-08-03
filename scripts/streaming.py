from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    FloatType, BooleanType, TimestampType
)
from pyspark.sql.functions import col, from_json, to_timestamp

from config import kafka_spark_options, POSTGRES_URL, postgres_properties, validate_config

CHECKPOINT_PATH = "./checkpoints/bronze_flights"
BRONZE_TABLE = "bronze_flights"

# Khớp đúng cấu trúc bảng flight_states trong schema.sql
flight_schema = StructType([
    StructField("event_time", StringType(), True), 
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("altitude", FloatType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity", FloatType(), True),
])


def build_spark():
    return (
        SparkSession.builder
        .appName("FlightStreamingLocal")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.postgresql:postgresql:42.7.3"
        )
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .getOrCreate()
    )


def write_batch_to_postgres(batch_df, batch_id):
    count = batch_df.count()
    print(f"[Batch {batch_id}] Writing {count} rows to Postgres table '{BRONZE_TABLE}'")
    (batch_df.write
        .jdbc(
            url=POSTGRES_URL,
            table=BRONZE_TABLE,
            mode="append",
            properties=postgres_properties
        ))


def main():
    validate_config()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream_df = (
        spark.readStream
        .format("kafka")
        .options(**kafka_spark_options)
        .load()
    )

    parsed_df = (
        raw_stream_df
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), flight_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .filter(col("longitude").isNotNull() & col("latitude").isNotNull())
    )

    query = (
        parsed_df
        .writeStream
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")   # chạy liên tục, xử lý batch mới mỗi 30s
        .start()
    )

    print("Streaming started, processing new data every 30s. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()