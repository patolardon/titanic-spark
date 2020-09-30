package titanic

import Transformer.Transformers
import org.apache.spark.sql.SparkSession

object Main extends App {
  val transformers = new Transformers()
  import transformers._
  import Pipeline.Pipeline.{pipeline, evaluator}

  val spark = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()

  val df = spark
    .read.format("csv").option("header", "true").option("separator", ",")
    .load("src/main/resources/train.csv")

  val dataPreparation = Seq(prepareDataType, addMaritalStatus, fillNullAge, prepareEmbarked)
  val dataPrepared = dataPreparation.foldLeft(df)((df, transformer) => transformer.transform(df))
  val splits = dataPrepared.randomSplit(Array(0.75, 0.25))

  val df_train = splits(0)
  val df_test = splits(1)

  val resultatModel: Double = evaluator.evaluate(pipeline.fit(df_train).transform(df_test))
  println(s"The accucary is $resultatModel %")

}
