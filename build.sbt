name := "titanic-spark"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.0" ,
  "org.apache.spark" %% "spark-mllib" % "2.4.0",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "org.scalactic" %% "scalactic" % "3.2.0"


)
