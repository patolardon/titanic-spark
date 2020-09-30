package titanic.Transformer

import org.apache.spark.sql.DataFrame

trait Transformer {
  def transform(dataFrame: DataFrame): DataFrame

}
