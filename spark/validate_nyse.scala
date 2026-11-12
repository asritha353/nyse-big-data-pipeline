// Independent quality gate for the cleaned NYSE Parquet dataset.
//
// Written in Scala for the Spark shell (no PySpark).
// Run on the cluster:
//   spark-shell --master spark://master:7077 -i /tmp/validate_nyse.scala < /dev/null
//
// Exits with code 0 when every check passes and 1 otherwise.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, countDistinct, lit, sum, when}

object ValidateNyse {

  val PricesCleanPath: String =
    sys.env.getOrElse("NYSE_PRICES_CLEAN_PATH", "hdfs://master:8020/data/nyse/prices_clean")

  val ExpectedRows: Long = 15120L
  val ExpectedSymbols: Long = 20L
  val ExpectedColumns: Set[String] = Set(
    "date", "symbol", "open", "close", "low", "high", "volume", "daily_return_pct"
  )

  def run(spark: SparkSession): Unit = {
    val prices = spark.read.parquet(PricesCleanPath)

    if (prices.columns.toSet != ExpectedColumns) {
      throw new IllegalStateException(
        s"Unexpected Parquet columns: ${prices.columns.mkString(", ")}"
      )
    }

    val anyKeyNull =
      col("date").isNull || col("symbol").isNull || col("open").isNull ||
        col("close").isNull || col("daily_return_pct").isNull

    val metrics = prices.agg(
      count(lit(1)).as("row_count"),
      countDistinct(col("symbol")).as("symbol_count"),
      sum(when(anyKeyNull, 1).otherwise(0)).as("null_count"),
      sum(when(col("open") <= 0, 1).otherwise(0)).as("invalid_open"),
      sum(when(col("high") < col("low"), 1).otherwise(0)).as("invalid_range")
    ).first()

    val actual = Map(
      "row_count" -> metrics.getAs[Long]("row_count"),
      "symbol_count" -> metrics.getAs[Long]("symbol_count"),
      "null_count" -> metrics.getAs[Long]("null_count"),
      "invalid_open" -> metrics.getAs[Long]("invalid_open"),
      "invalid_range" -> metrics.getAs[Long]("invalid_range")
    )
    val expected = Map(
      "row_count" -> ExpectedRows,
      "symbol_count" -> ExpectedSymbols,
      "null_count" -> 0L,
      "invalid_open" -> 0L,
      "invalid_range" -> 0L
    )

    if (actual != expected) {
      throw new IllegalStateException(
        s"Spark quality gate failed: expected $expected, got $actual"
      )
    }

    println(s"SPARK QUALITY METRICS: $actual")
    println(Seq("SPARK", "QUALITY", "GATE", "PASSED").mkString(" "))
  }
}

val validateNyseExitCode: Int =
  try {
    ValidateNyse.run(spark)
    0
  } catch {
    case error: Throwable =>
      println(s"SPARK QUALITY GATE FAILED: ${error.getMessage}")
      1
  }

spark.stop()
System.exit(validateNyseExitCode)
