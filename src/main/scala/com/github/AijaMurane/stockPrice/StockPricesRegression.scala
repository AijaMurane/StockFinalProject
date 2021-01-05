package com.github.AijaMurane.stockPrice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.text.SimpleDateFormat

object StockPricesRegression extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val fPath = "./src/resources/stock_prices.csv"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(fPath)
    .selectExpr("cast(date as date)", "cast(close as double)")
    .filter("ticker = 'AAPL'")

  df.show(false)
  df.printSchema()

  val firstDate = df.select("date").orderBy("date").first().toString().stripPrefix("[").stripSuffix("]")
  println(firstDate)

  val format = new SimpleDateFormat("yyyy-MM-dd")
  val firstDate2 = format.parse(firstDate)
  println(firstDate2)

  val dateAsNumberDF = df
    .agg(col("date") - firstDate2)

  dateAsNumberDF.printSchema()
  dateAsNumberDF.show()


  /*

  .selectExpr("cast(date as date) as features", "cast(close as double) as label")
    .filter("ticker = 'AAPL'")

  val lr = new LinearRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
  println(lr.explainParams())
  val lrModel = lr.fit(df)

  import spark.implicits._
  val summary = lrModel.summary
  summary.residuals.show()
  println(summary.objectiveHistory.toSeq.toDF.show())
  println(summary.rootMeanSquaredError)
  println(summary.r2)
*/

}
