package titanic.Pipeline

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler}
import titanic.Transformer.Transformers

object Pipeline {
  val transformer = new Transformers()

  val evaluator: BinaryClassificationEvaluator = new BinaryClassificationEvaluator()
    .setLabelCol("Survived")

  val vectAssemblerAge = new VectorAssembler()
    .setInputCols(Array("Age"))
    .setOutputCol("AgeToCluster").setHandleInvalid("skip")


  val MaritalStatusEncoder = new OneHotEncoder()
    .setInputCol("Marital Status")
    .setOutputCol("Marital_encoded")

  val EmbarkedEncoder = new OneHotEncoder()
    .setInputCol("Embarked")
    .setOutputCol("Embarked_encoded")

  val vectAssemblerFare = new VectorAssembler()
    .setInputCols(Array("Fare"))
    .setOutputCol("FareToCluster").setHandleInvalid("skip")


  val fareClassifier = new KMeans().setFeaturesCol("FareToCluster").setPredictionCol("FareClass").setK(3).setSeed(1L)

  val ageClassifier = new KMeans().setFeaturesCol("AgeToCluster").setPredictionCol("AgeClass").setK(3).setSeed(1L)

  val vectAssembler = new VectorAssembler()
    .setInputCols(Array("FareClass", "AgeClass", "Marital Status", "Embarked", "Pclass"))
    .setOutputCol("features")
    .setHandleInvalid("skip")

  val algo: RandomForestClassifier = new RandomForestClassifier()
    .setFeaturesCol("features").setLabelCol("Survived")

  val pipeline = new Pipeline()
    .setStages(Array(vectAssemblerAge, vectAssemblerFare, fareClassifier, EmbarkedEncoder, ageClassifier, MaritalStatusEncoder, vectAssembler, algo))


}
