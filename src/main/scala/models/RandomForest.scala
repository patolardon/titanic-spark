package titanic.models

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import titanic.common.{Assembler, Transformer}

case class RandomForest(inputCol:Array[String], targetCol:String, test:DataFrame) extends Transformer with Assembler{
  lazy val testDataFrame = assemble(inputCol, "features").transform(test)

  override def transform(dataFrame: DataFrame) = {
    val forest = getModel(dataFrame)
    forest.transform(testDataFrame)
  }

  def getModel(dataFrame:DataFrame) = {
    val vectAssembler = assemble(inputCol, "features")
    val dataToTrain = vectAssembler.transform(dataFrame)
    new RandomForestClassifier()
      .setFeaturesCol("features").setLabelCol(targetCol).fit(dataToTrain)
  }

}
