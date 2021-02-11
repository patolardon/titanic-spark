package titanic.provider

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  def getSparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()

}
