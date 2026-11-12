// Clean Sqoop-imported NYSE prices and write them to HDFS as Parquet.
//
// Written in Scala for the Spark shell (no PySpark).
// Run on the cluster:
//   spark-shell --master spark://master:7077 -i /tmp/process_nyse.scala < /dev/null
//
// The script exits with code 0 on success and 1 on any failure, so Airflow
// and the shell can tell whether the job worked.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

object ProcessNyse {

  // Paths can be overridden with environment variables for local testing.
  val PricesRawPath: String =
    sys.env.getOrElse("NYSE_PRICES_RAW_PATH", "hdfs://master:8020/data/nyse/prices_raw")
  val PricesCleanPath: String =
    sys.env.getOrElse("NYSE_PRICES_CLEAN_PATH", "hdfs://master:8020/data/nyse/prices_clean")

  // Same column order as the PostgreSQL staging table that Sqoop imported.
  val PricesSchema: StructType = StructType(Seq(
    StructField("date", DateType, nullable = true),
    StructField("symbol", StringType, nullable = true),
    StructField("open", DoubleType, nullable = true),
    StructField("close", DoubleType, nullable = true),
    StructField("low", DoubleType, nullable = true),
    StructField("high", DoubleType, nullable = true),
    StructField("volume", DoubleType, nullable = true)
  ))

  def run(spark: SparkSession): Unit = {
    // ---------- 1. RDD demonstration ----------
    // textFile creates an RDD where each element is one line of Sqoop output.
    val rawRdd = spark.sparkContext.textFile(PricesRawPath)
    println(s"RDD RECORD COUNT: ${rawRdd.count()}")
    println("RDD SAMPLE RECORDS:")
    rawRdd.take(5).foreach(println)

    // MapReduce-style word count on the RDD: map each line to (symbol, 1),
    // then reduceByKey adds the 1s for every symbol.
    val recordsPerSymbol = rawRdd
      .map(line => line.split(","))
      .filter(fields => fields.length > 1)
      .map(fields => (fields(1), 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()
    println("RDD RECORDS PER SYMBOL (map + reduceByKey):")
    recordsPerSymbol.foreach { case (symbol, total) => println(s"  $symbol -> $total") }

    // ---------- 2. Typed DataFrame ----------
    // Sqoop text has no header, so an explicit schema gives the correct types.
    // Sqoop writes database NULLs as the text "null".
    val prices = spark.read
      .option("header", "false")
      .option("nullValue", "null")
      .schema(PricesSchema)
      .csv(PricesRawPath)

    val rawCount = prices.count()
    println(s"RAW ROW COUNT: $rawCount")
    prices.printSchema()
    prices.show(5, truncate = false)

    // ---------- 3. Cleaning ----------
    val withoutNulls = prices.na.drop()
    val withoutDuplicates = withoutNulls.dropDuplicates()

    // ---------- 4. Feature: open-to-close daily return (%) ----------
    // Zero opening prices are removed first to avoid division by zero.
    val cleaned = withoutDuplicates
      .filter(col("open") =!= 0)
      .withColumn(
        "daily_return_pct",
        round(((col("close") - col("open")) / col("open")) * 100, 4)
      )

    val withoutNullsCount = withoutNulls.count()
    val withoutDuplicatesCount = withoutDuplicates.count()
    val cleanedCount = cleaned.count()

    println(s"NULL ROWS REMOVED: ${rawCount - withoutNullsCount}")
    println(s"DUPLICATE ROWS REMOVED: ${withoutNullsCount - withoutDuplicatesCount}")
    println(s"ZERO-OPEN ROWS REMOVED: ${withoutDuplicatesCount - cleanedCount}")
    println(s"CLEANED ROW COUNT: $cleanedCount")
    cleaned.show(10, truncate = false)

    // ---------- 5. Write Snappy-compressed Parquet ----------
    cleaned.write.mode("overwrite").parquet(PricesCleanPath)
    println(s"CLEANED PARQUET WRITTEN TO: $PricesCleanPath")
  }
}

val processNyseExitCode: Int =
  try {
    ProcessNyse.run(spark)
    0
  } catch {
    case error: Throwable =>
      println(s"SPARK PROCESSING FAILED: ${error.getMessage}")
      error.printStackTrace()
      1
  }

spark.stop()
System.exit(processNyseExitCode)
