package com.github.AijaMurane.stockPrice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object StockPrices extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val fPath = "./src/resources/stock_prices.csv"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(fPath)

  /*
  Calculating the average daily return of every stock for every date.
  Output should be:
  date          average_return
  yyyy-MM-dd    return of all stocks on that date
  */

  val windowSpec = Window
    .partitionBy("ticker")
    .orderBy("date")

  val dailyReturn = round((col("close") - lag("close", 1, 0).over(windowSpec)) / lag("close", 1, 0).over(windowSpec), 6)
  val dailyReturnDF = df
    .select(expr("date"), dailyReturn.alias("daily_return"))
    .groupBy(col("date"))
    .agg(avg("daily_return").alias("average_return"))
    .orderBy(col("date"))

  /*
  Save the results to the file as Parquet and CSV
   */

  val pPath = "./src/resources/daily_returns.parquet"
  dailyReturnDF
    .write
    .format("parquet")
    .option("header", "true")
    .mode("overwrite")
    .save(pPath)

  val csvPath = "./src/resources/daily_returns.csv"
  dailyReturnDF
    .coalesce(1)
    .write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save(csvPath)

  /*
  Calculate which stock was traded most frequently - as measured by closing price * volume - on average.
   */
  df.createOrReplaceTempView("stock_prices_view")
  val mostFrequentStock = spark.sql("SELECT ticker, ROUND((SUM(close * volume)/COUNT(volume))/1000,2) " +
    "AS frequency_thousands " +
    "FROM stock_prices_view " +
    "GROUP BY ticker " +
    "ORDER BY frequency_thousands DESC " +
    "LIMIT 1")

  val mostFrequentStockName = mostFrequentStock.select(col("ticker")).first().toString().stripPrefix("[").stripSuffix("]")
  val mostFrequentStockFrequency = mostFrequentStock.select(col("frequency_thousands")).first().toString().stripPrefix("[").stripSuffix("]")

  println(s"The stock that was traded most frequently on average was $mostFrequentStockName with frequency of $mostFrequentStockFrequency thousands.")

  /*
  Calculate which stock was the most volatile as measured by annualized standard deviation of daily returns.
   */

  val dailyReturnStdDF = df
    .select(expr("ticker"), dailyReturn.alias("daily_return"), expr("date"))
    .withColumn("year", regexp_extract(col("date"), "^\\d{4}", 0))
    .groupBy("ticker", "year")
    .agg((stddev("daily_return") * sqrt(count("daily_return"))).alias("annualized_standard_deviation"))
    .orderBy(desc("annualized_standard_deviation"))

  val mostVolatileStockName = dailyReturnStdDF.select(col("ticker")).first().toString().stripPrefix("[").stripSuffix("]")
  val mostVolatileStockNameYear = dailyReturnStdDF.select(col("year")).first().toString().stripPrefix("[").stripSuffix("]")
  val mostVolatileStockNameASD = dailyReturnStdDF.select(col("annualized_standard_deviation")).first().toString().stripPrefix("[").stripSuffix("]")

  println(s"The most volatile stock by annualized standard deviation was $mostVolatileStockName in $mostVolatileStockNameYear with annualized standard deviation of $mostVolatileStockNameASD.")
}
