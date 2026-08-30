"""Independent quality gate for the cleaned NYSE Parquet dataset."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, count_distinct, sum as spark_sum, when


PRICES_CLEAN_PATH = "hdfs://master:8020/data/nyse/prices_clean"
EXPECTED_ROWS = 15_120
EXPECTED_SYMBOLS = 20
EXPECTED_COLUMNS = {
    "date",
    "symbol",
    "open",
    "close",
    "low",
    "high",
    "volume",
    "daily_return_pct",
}


def main() -> None:
    spark = SparkSession.builder.appName("NYSE Parquet Quality Gate").getOrCreate()
    try:
        prices = spark.read.parquet(PRICES_CLEAN_PATH)
        if set(prices.columns) != EXPECTED_COLUMNS:
            raise ValueError(f"Unexpected Parquet columns: {prices.columns}")

        metrics = prices.agg(
            count("*").alias("row_count"),
            count_distinct("symbol").alias("symbol_count"),
            spark_sum(
                when(
                    col("date").isNull()
                    | col("symbol").isNull()
                    | col("open").isNull()
                    | col("close").isNull()
                    | col("daily_return_pct").isNull(),
                    1,
                ).otherwise(0)
            ).alias("null_count"),
            spark_sum(when(col("open") <= 0, 1).otherwise(0)).alias("invalid_open"),
            spark_sum(when(col("high") < col("low"), 1).otherwise(0)).alias(
                "invalid_range"
            ),
        ).first()

        actual = {
            "row_count": metrics["row_count"],
            "symbol_count": metrics["symbol_count"],
            "null_count": metrics["null_count"],
            "invalid_open": metrics["invalid_open"],
            "invalid_range": metrics["invalid_range"],
        }
        expected = {
            "row_count": EXPECTED_ROWS,
            "symbol_count": EXPECTED_SYMBOLS,
            "null_count": 0,
            "invalid_open": 0,
            "invalid_range": 0,
        }
        if actual != expected:
            raise ValueError(f"Spark quality gate failed: expected {expected}, got {actual}")

        print(f"SPARK QUALITY METRICS: {actual}")
        print("SPARK QUALITY GATE PASSED")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
