package titanic.common

import org.apache.spark.sql.DataFrame


case class TrainSplitter(dataFrame: DataFrame, ratio:Array[Double]) {
  def split = dataFrame.randomSplit(ratio)
}
