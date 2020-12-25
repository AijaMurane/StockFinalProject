import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, lag, regexp_extract, round, sqrt, stddev, count, var_samp}
import org.apache.spark.sql.expressions.Window


object StockPrices extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  val fPath = "./src/resources/stock_prices.csv"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(fPath)

  val windowSpec = Window
    .partitionBy("ticker")
    .orderBy("date")

  val dailyReturn = round((col("close") - lag("close", 1, 0).over(windowSpec))/lag("close", 1, 0).over(windowSpec),6)
  val dailyReturnDF = df.select(expr("date"), dailyReturn.alias("daily_return"))
  dailyReturnDF.show()

  val pPath = "./src/resources/daily_returns.parquet"
  dailyReturnDF
    .write
    .format("parquet")
    .mode("overwrite")
    .save(pPath)

  val csvPath = "./src/resources/daily_returns.csv"
  dailyReturnDF
    .coalesce(1)
    .write
    .format("csv")
    .mode("overwrite")
    .save(csvPath)

  df.createOrReplaceTempView("stock_prices_view")
  val mostFrequentStock = spark.sql("SELECT ticker, SUM(close * volume)/COUNT(volume) AS frequency " +
    "FROM stock_prices_view " +
    "GROUP BY ticker " +
    "ORDER BY frequency DESC " +
    "LIMIT 1")

  val mostFrequentStockName = mostFrequentStock.select(col("ticker")).first().toString()

  println(s"The stock that was traded most frequently on average was: $mostFrequentStockName")

  val dailyReturnStdDF = df
    .select(expr("ticker"), dailyReturn.alias("daily_return"), expr("date"))
    .withColumn("year", regexp_extract(col("date"), "^\\d{4}", 0))
  dailyReturnStdDF.show()

  dailyReturnStdDF
    .groupBy("ticker", "year")
    .agg((stddev("daily_return") * sqrt(count("daily_return"))).alias("annualized_standard_deviation"))
    .orderBy("ticker", "year")
    .show()



}
