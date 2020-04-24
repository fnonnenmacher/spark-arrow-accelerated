package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.sql.{ArrowColumnVectorWithAccessibleFieldVector, vectorized}

import scala.collection.JavaConverters._

case class FPGAProjectExec(child: SparkPlan, outputAttributes: Seq[Attribute])
  extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    // In a productive setup probably makes sense to define here a fallback.
    // But for my prototypical setup this should never be called.
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    child.executeColumnar().mapPartitionsWithIndex((index, batchIter) =>

      batchIter.map { batch =>

        val inputVectors: Seq[FieldVector] = extractFieldVectors(batch)

        val inputRoot = new VectorSchemaRoot(inputVectors.asJava)

        // execute calculation on Fletcher/Cpp
        val outputRoot = ArrowProcessor.addThreeVectors(inputRoot)

        toResultBatch(outputRoot);
      }
    )
  }

  private def extractFieldVectors(batch: ColumnarBatch): Seq[FieldVector] = {
    (0 until batch.numCols).map(batch.column).map {
      case ArrowColumnVectorWithAccessibleFieldVector(fieldVector) => fieldVector
      case _ => throw new IllegalStateException(s"${getClass.getSimpleName} does only support columnar data in arrow format.")
    }
  }

  private def toResultBatch(root: VectorSchemaRoot): ColumnarBatch = {
    // map arrow field vectors to spark ArrowColumnVector
    val arrowVectors = root.getFieldVectors.asScala.map(x => new vectorized.ArrowColumnVector(x)).toArray[ColumnVector]

    //combine all column vectors in ColumnarBatch
    val batch = new ColumnarBatch(arrowVectors)
    batch.setNumRows(root.getRowCount)
    batch
  }

  override def output: Seq[Attribute] = outputAttributes
}