import org.apache.spark.sql.SparkSession

object StockPricesRegression extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  val fPath = "./src/resources/stock_prices.csv"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(fPath)
}
