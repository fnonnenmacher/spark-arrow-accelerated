package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.SparkRowsToArrow.nullableInt
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._

case class FPGAProjectExec(child: SparkPlan, outputAttributes: Seq[Attribute])
  extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {

    child.execute().mapPartitionsWithIndex { (index, iter) =>

      //convert spark internal row to arrow
      val vectorSchemaRoot = SparkRowsToArrow.convert(iter, Seq(nullableInt("in1"), nullableInt("in2"), nullableInt("in3")))

      // execute calculation on Fletcher/Cpp
      val resultRoot = ArrowProcessor.addThreeVectors(vectorSchemaRoot)

      //map arrow vectors to spark ArrowColumnVector
      mapArrowResultsToSparkRow(resultRoot);
    }
  }

  private def mapArrowResultsToSparkRow(root: VectorSchemaRoot): Iterator[InternalRow] = {
    // map arrow field vectors to spark ArrowColumnVector
    val arrowVectors = root.getFieldVectors.asScala.map(x => new vectorized.ArrowColumnVector(x)).toArray[ColumnVector]

    //combine all column vectors in ColumnarBatch
    val batch = new ColumnarBatch(arrowVectors)
    batch.setNumRows(root.getRowCount)

    //get row iterator
    batch.rowIterator().asScala
  }

  override def output: Seq[Attribute] = outputAttributes
}