package common

import org.apache.spark.sql.DataFrame
import titanic.common.Transformer

case class Pipeline(transformationStage: Transformer*) {
  def transform(dataFrame: DataFrame) = transformationStage.foldLeft(dataFrame)((df, transform) => transform.transform(df))

}
