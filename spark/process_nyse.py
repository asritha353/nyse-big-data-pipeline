"""Clean Sqoop-imported NYSE prices and write them to HDFS as Parquet."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


PRICES_RAW_PATH = "hdfs://master:8020/data/nyse/prices_raw"
PRICES_CLEAN_PATH = "hdfs://master:8020/data/nyse/prices_clean"

PRICES_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("symbol", StringType(), nullable=True),
        StructField("open", DoubleType(), nullable=True),
        StructField("close", DoubleType(), nullable=True),
        StructField("low", DoubleType(), nullable=True),
        StructField("high", DoubleType(), nullable=True),
        StructField("volume", DoubleType(), nullable=True),
    ]
)


def main() -> None:
    spark = SparkSession.builder.appName("NYSE Big Data Pipeline").getOrCreate()

    try:
        raw_rdd = spark.sparkContext.textFile(PRICES_RAW_PATH)
        print(f"RDD RECORD COUNT: {raw_rdd.count()}")
        print("RDD SAMPLE RECORDS:")
        for row in raw_rdd.take(5):
            print(row)

        prices = (
            spark.read.option("header", "false")
            .schema(PRICES_SCHEMA)
            .csv(PRICES_RAW_PATH)
        )

        raw_count = prices.count()
        print(f"RAW ROW COUNT: {raw_count}")
        prices.printSchema()
        prices.show(5, truncate=False)

        without_nulls = prices.dropna()
        without_duplicates = without_nulls.dropDuplicates()
        cleaned = without_duplicates.filter(col("open") != 0).withColumn(
            "daily_return_pct",
            spark_round(((col("close") - col("open")) / col("open")) * 100, 4),
        )

        without_nulls_count = without_nulls.count()
        without_duplicates_count = without_duplicates.count()
        cleaned_count = cleaned.count()

        null_rows_removed = raw_count - without_nulls_count
        duplicate_rows_removed = without_nulls_count - without_duplicates_count
        zero_open_rows_removed = without_duplicates_count - cleaned_count

        print(f"NULL ROWS REMOVED: {null_rows_removed}")
        print(f"DUPLICATE ROWS REMOVED: {duplicate_rows_removed}")
        print(f"ZERO-OPEN ROWS REMOVED: {zero_open_rows_removed}")
        print(f"CLEANED ROW COUNT: {cleaned_count}")
        cleaned.show(10, truncate=False)

        cleaned.write.mode("overwrite").parquet(PRICES_CLEAN_PATH)
        print(f"CLEANED PARQUET WRITTEN TO: {PRICES_CLEAN_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
