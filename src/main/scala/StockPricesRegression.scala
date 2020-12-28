import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression


object StockPricesRegression extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  val fPath = "./src/resources/stock_prices.csv"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(fPath)
    .selectExpr("cast(cast(date as date) as int) as features", "cast(close as double) as label")
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


}
