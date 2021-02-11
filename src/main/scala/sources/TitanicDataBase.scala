package titanic.sources

import titanic.common.Source
import org.apache.spark.sql.{DataFrame, SparkSession}

case class TitanicDataBase(spark:SparkSession) extends Source{

  override def load: DataFrame = {
    spark
      .read.format("csv").option("header", "true").option("separator", ",")
      .load("src/main/resources/train.csv")
  }
}
