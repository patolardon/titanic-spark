package titanic.common

import org.apache.spark.sql.DataFrame

trait Transformer {
  def transform(dataframe:DataFrame): DataFrame
}
