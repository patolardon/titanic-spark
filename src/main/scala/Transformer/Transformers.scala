package titanic.Transformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import titanic.Transformer.Transformer


class Transformers {

  case object addMaritalStatus extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = {
      dataFrame.withColumn("Marital Status",
        when(col("Name").contains("Mrs"), 3)
          .when(col("Name").contains("Miss"), 2)
          .when(col("Name").contains("Mr"), 1)
          .otherwise(0))
    }
  }

  case object prepareDataType extends Transformer {

    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.select(
      col("Age").cast("Integer"),
      col("Fare").cast("Float"),
      col("Embarked"),
      col("Pclass").cast("Integer"),
      col("Name"),
      col("Survived").cast("Integer"))
  }

  case object fillNullAge extends Transformer {
    val ageByMaritalStatus = Window.partitionBy("Marital Status")

    def transform(dataFrame: DataFrame): DataFrame = {
      dataFrame
        .withColumn("Age", when(col("Age").isNull, mean(col("Age")).over(ageByMaritalStatus)).otherwise(col("Age")))
    }
  }

  case object prepareEmbarked extends Transformer {
    def transform(dataFrame: DataFrame): DataFrame = {
      dataFrame.withColumn("Embarked",
        when(col("Embarked") === "S", 3)
          .when(col("Embarked") === "C", 2)
          .when(col("Embarked") === "Q", 1)
          .otherwise(0))
    }
  }

}


