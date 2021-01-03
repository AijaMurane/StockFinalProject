import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, expr, lag, round}
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vectors

object StockPricesClassification extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val fPath = "./src/resources/stock_prices.csv"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(fPath)

  df.show(false)
  df.printSchema()

  val windowSpec = Window
    .partitionBy("ticker")
    .orderBy("date")

  /*
  1 means that stocks decreased
  2 means that stocks are same
  3 means that stocks increased
   */

  val compareFields = udf((closeValue:Double, previousCloseValue:Double) => {
    val change = closeValue - previousCloseValue
    if (change<0) 1 else if (change>0) 3 else 2
  }
  )

  val df2 = df.
            withColumn("label", compareFields(col("close"),lag("close", 1, 0).over(windowSpec)))

  df2.show()

  val assembler = new VectorAssembler()
    .setInputCols(Array("open", "high", "low", "close", "volume"))
    .setOutputCol("features")

  val output = assembler.transform(df2)
  val output2 = output.select("date", "features", "label")

  val Array(train, test) = output2.randomSplit(Array(0.7, 0.3),seed = 20)
  train.show(5, false)
  test.show(5, false)

  //so we want to train the model on train set
  val lr = new LogisticRegression()
  val lrModel = lr.fit(train)

  val lrPredictions = lrModel.transform(test)
  lrPredictions.printSchema()
  lrPredictions.show(false)
}
