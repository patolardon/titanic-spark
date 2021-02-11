import common.Pipeline
import org.apache.spark.internal.Logging
import titanic.Transformer.Transformers
import titanic.sources.TitanicDataBase
import titanic.provider.SparkSessionProvider
import titanic.common.{Evaluator, TrainSplitter}
import titanic.models.RandomForest

object Main extends App with SparkSessionProvider with Logging {
  val transformers = new Transformers()

  import transformers._

  val spark = getSparkSession
  logInfo("Building sparkSession")
  val df = TitanicDataBase(spark).load

  val dataPreparation = Seq(PrepareDataType,
    AddMaritalStatus,
    FillNullAge,
    PrepareEmbarked,
    MaritalStatusEncoder,
    EmbarkedEncoder,
    Classifier(Array("Age"), "AgeClass"),
    Classifier(Array("Fare"), "FareClass"))

  val dataPrepared = Pipeline(dataPreparation: _*).transform(df)
  val split = TrainSplitter(dataPrepared, Array(0.75, 0.25)).split
  val model = RandomForest(Array("FareClass", "AgeClass", "Marital Status", "Embarked", "Pclass"), "Survived", split(0))
  val resultatModel: Double = Evaluator("Survived", model.transform(split(1))).evaluate

  logInfo(s"The accuracy is $resultatModel %")

}
