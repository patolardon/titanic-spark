package titanic
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.GivenWhenThen


object SharedSparkSession {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkSession for unit tests")
    .master("local[*]")
    .getOrCreate()
}

class TransformerTests extends AnyFlatSpec with GivenWhenThen {
  import SharedSparkSession.spark
  import spark.implicits._
  val transformer = new titanic.Transformer.Transformers()
  import transformer._

  "A marital status " should "give a result" in {
    Given("A specific value on column 1")
    val dataFrame:DataFrame = Seq(
      "Mr P",
      "Mrs D",
      "Miss H"
    ).toDF("Name")

    When("changing the marital status")
    val dataFrameWithMarital = addMaritalStatus.transform(dataFrame)

    Then("it should add a column named Marital Status")
    assert(dataFrameWithMarital.columns === Seq("Name", "Marital Status"))
  }

  "A dataframe with only strings value " should "have a new schema" in {
    Given("A specific value on column Embarked")
    val dataFrame:DataFrame = Seq(
      ("25", "2.5", "S", "3", "Pierre", "1"),
      ("25", "2.5", "S", "3", "Pierre", "1"),
      ("25", "2.5", "S", "3", "Pierre", "1"),
      ("25", "2.5", "S", "3", "Pierre", "1")
    ).toDF("Age", "Fare", "Embarked", "Pclass", "Name", "Survived")

    When("running prepareDataType transformer")
    val dataFrameWithCorrectType = prepareDataType.transform(dataFrame)
    val schema = dataFrameWithCorrectType.schema

    val newSchema = StructType(List(StructField(
      "Age", IntegerType, nullable = true),
      StructField("Fare", FloatType, nullable = true),
      StructField("Embarked", StringType, nullable = true),
      StructField("Pclass", IntegerType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Survived", IntegerType, nullable = true)))

    Then("The schema should have changed ")
    assert(schema === newSchema)
  }

  "A embarked value " should "change accoring to its value" in {
    Given("A specific value on column Embarked")
    val dataFrame:DataFrame = Seq(
      "S",
      "C",
      "Q",
      "H"
    ).toDF("Embarked")

    When("running prepareEmbarked transformer")
    val dataFrameWithEmbarked = prepareEmbarked.transform(dataFrame)
    val ListOfEmbarked = dataFrameWithEmbarked.select("Embarked").rdd.map(r => r(0)).collect.toList


    Then("it should be a list of int according to ")
    assert(ListOfEmbarked === List(3, 2, 1, 0))
  }

  "A missing age value " should "be filled accorging to its martital value" in {
    Given("A specific value on column Embarked")
    val dataFrame:DataFrame = Seq(
      ("Miss", Some(25)),
      ("Mrs", None),
      ("Mr", Some(25)),
      ("Mrs", Some(25))
    ).toDF("Marital Status", "Age")

    When("running fillNullAge transformer")
    val dataFrameWithoutNull= fillNullAge.transform(dataFrame)
    val ListOfAges = dataFrameWithoutNull.select("Age").rdd.map(r => r(0)).collect.toList


    Then("it should be a list of int without null ")
    assert(ListOfAges === List(25, 25, 25, 25))
  }

}
