package titanic.common

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.DataFrame

case class Evaluator(labelCol:String, dataFrame: DataFrame) {
  val evaluator: BinaryClassificationEvaluator = new BinaryClassificationEvaluator()
    .setLabelCol(labelCol)

  def evaluate = evaluator.evaluate(dataFrame)

}
