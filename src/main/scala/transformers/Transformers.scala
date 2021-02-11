package titanic.Transformer

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import titanic.common.Transformer
import titanic.common.Assembler


class Transformers {

  case object AddMaritalStatus extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = {
      dataFrame.withColumn("Marital Status",
        when(col("Name").contains("Mrs"), 3)
          .when(col("Name").contains("Miss"), 2)
          .when(col("Name").contains("Mr"), 1)
          .otherwise(0))
    }
  }

  case object PrepareDataType extends Transformer {

    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.select(
      col("Age").cast("Integer"),
      col("Fare").cast("Float"),
      col("Embarked"),
      col("Pclass").cast("Integer"),
      col("Name"),
      col("Survived").cast("Integer"))
  }

  case object FillNullAge extends Transformer {
    val ageByMaritalStatus = Window.partitionBy("Marital Status")

    def transform(dataFrame: DataFrame): DataFrame = {
      dataFrame
        .withColumn("Age", when(col("Age").isNull, mean(col("Age")).over(ageByMaritalStatus)).otherwise(col("Age")))
    }
  }

  case object PrepareEmbarked extends Transformer {
    def transform(dataFrame: DataFrame): DataFrame = {
      dataFrame.withColumn("Embarked",
        when(col("Embarked") === "S", 3)
          .when(col("Embarked") === "C", 2)
          .when(col("Embarked") === "Q", 1)
          .otherwise(0))
    }
  }

  case object MaritalStatusEncoder extends Transformer {

    override def transform(dataframe: DataFrame): DataFrame = {
      val MaritalStatusEncoder = new OneHotEncoder()
        .setInputCol("Marital Status")
        .setOutputCol("Marital_encoded")
      MaritalStatusEncoder.transform(dataframe)
    }
  }

  case object EmbarkedEncoder extends Transformer {
    override def transform(dataframe: DataFrame): DataFrame = {
      val EmbarkedEncoder = new OneHotEncoder()
        .setInputCol("Embarked")
        .setOutputCol("Embarked_encoded")
      EmbarkedEncoder.transform(dataframe)
    }
  }

  case class Classifier(colToCluster:Array[String], outputCol:String) extends Transformer with Assembler {
    override def transform(dataframe: DataFrame): DataFrame = {
      val assembler = assemble(colToCluster, "colToCluster")
      val dataFrameToCluster = assembler.transform(dataframe)
      new KMeans().setFeaturesCol("colToCluster").setPredictionCol(outputCol).setK(3).setSeed(1L)
        .fit(dataFrameToCluster).transform(dataFrameToCluster).drop("colToCluster")
    }
  }


}


