package titanic.common

import org.apache.spark.ml.feature.VectorAssembler

trait Assembler {
  def assemble(inputCol: Array[String], outputCol:String) = new VectorAssembler()
    .setInputCols(inputCol)
    .setOutputCol(outputCol).setHandleInvalid("skip")
}
