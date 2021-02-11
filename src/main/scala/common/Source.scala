package titanic.common

import org.apache.spark.sql.DataFrame

trait Source {
  def load:DataFrame
}
