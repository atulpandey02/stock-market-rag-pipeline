import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def get_output_date():
    """
    Get output partition date.
    - Airflow: sys.argv[1] = {{ ds }} e.g. "2026-03-29" (UTC)
    - Manual:  datetime.now(UTC)
    Ensures Spark output path matches Snowflake load path.
    """
    if len(sys.argv) > 1:
        date_str         = sys.argv[1]
        year, month, day = date_str.split("-")
        print(f"Date source: Airflow ds = {date_str}")
    else:
        now  = datetime.now(timezone.utc)
        year  = str(now.year)
        month = f"{now.month:02d}"
        day   = f"{now.day:02d}"
        print(f"Date source: UTC now = {year}-{month}-{day}")
    return year, month, day


def create_spark_session():
    spark = (SparkSession.builder
        .appName("StockMarketBatchProcessor")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key",        "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key",        "minioadmin")
    hadoop_conf.set("fs.s3a.endpoint",          "http://minio:9000")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    return spark


def main():
    # ✅ Get date once — used for output path
    year, month, day = get_output_date()

    spark = create_spark_session()

    try:
        input_path  = (
            f"s3a://stock-market-data/raw/historical/"
            f"year={year}/month={month}/day={day}/")
        # ✅ Output path uses same date as {{ ds }} passed from Airflow
        output_path = (
            f"s3a://stock-market-data/processed/historical/"
            f"year={year}/month={month}/day={day}/"
        )

        print(f"\nReading data from S3: {input_path}")
        print(f"Writing to         : {output_path}")

        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(f"{input_path}"))

        count = df.count()
        if count == 0:
            print("No data found to process")
            sys.exit(0)

        df.show(5)
        df.printSchema()

        processed_df = (df
            .withColumn("date", F.to_date("date", "yyyy-MM-dd"))
            .withColumn("daily_range",
                        F.round(F.col("high") - F.col("low"), 2))
            .withColumn("daily_return_pct",
                        F.round(
                            ((F.col("close") - F.col("open")) / F.col("open")) * 100, 2))
            .withColumn("is_positive_day", F.col("close") > F.col("open"))
            .withColumn("sma_5",  F.round(F.avg("close").over(__window(5)),  2))
            .withColumn("sma_20", F.round(F.avg("close").over(__window(20)), 2))
            .withColumn("processing_time", F.current_timestamp())
        )

        print("\n---- Processing Historical Stock Data")
        print(f"Record count: {processed_df.count()}")
        processed_df.select(
            "symbol", "date", "open", "high", "low",
            "volume", "close", "daily_return_pct", "sma_5", "sma_20"
        ).show(5)

        (processed_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .partitionBy("symbol")
            .parquet(output_path))

        processed_df.select(
            "symbol", "date", "open", "high", "low", "close",
            "volume", "daily_range", "daily_return_pct",
            "is_positive_day", "sma_5", "sma_20"
        ).orderBy("symbol", "date").show(20, truncate=False)

        print(f"\nOutput written to: {output_path}")
        print("\n" + "=" * 45)
        print("BATCH PROCESSING COMPLETE")
        print("=" * 45)

    except Exception as e:
        print(f"Error in batch processing: {e}")
        sys.exit(1)
    finally:
        spark.stop()


def __window(days: int):
    from pyspark.sql.window import Window
    return (Window
            .partitionBy("symbol")
            .orderBy("date")
            .rowsBetween(-(days - 1), 0))


if __name__ == "__main__":
    main()