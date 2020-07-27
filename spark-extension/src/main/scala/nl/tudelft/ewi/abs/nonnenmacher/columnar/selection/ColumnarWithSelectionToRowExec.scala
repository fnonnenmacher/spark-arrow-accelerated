package nl.tudelft.ewi.abs.nonnenmacher.columnar.selection

import nl.tudelft.ewi.abs.nonnenmacher.utils.StartStopMeasurment
import org.apache.arrow.gandiva.evaluator.SelectionVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.ArrowMemoryToSparkMemoryExec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnarWithSelectionToRowExec(override val child: SparkPlan) extends ColumnarToRowExec(child) with StartStopMeasurment {

  override def supportCodegen: Boolean = false

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val conversionTime = longMetric("conversionTime")

    if (!child.isInstanceOf[ColumnarWithSelectionVectorSupport]) {
      throw new IllegalStateException("Cannot ")
    }

    val childWithSelection: ColumnarWithSelectionVectorSupport = child.asInstanceOf[ColumnarWithSelectionVectorSupport]

    // This avoids calling `output` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localOutput = this.output
    childWithSelection.executeColumnarWithSelection().mapPartitions { batches =>
      val toUnsafe = UnsafeProjection.create(localOutput, localOutput)

      batches.flatMap { case (batch, selectionVector) =>
        numInputBatches += 1
        numOutputRows += selectionVector.getRecordCount

        new SelectionVectorRowIterator(batch, selectionVector).map(toUnsafe)
      }
    }
  }

  // We have to override equals because subclassing a case class like RowToColumnarExec is not that clean
  // One of the issues is that the generated equals will see RowToColumnarExec and RowToArrowColumnarExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ArrowMemoryToSparkMemoryExec]
  }

  override def hashCode(): Int = super.hashCode()

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
    "conversionTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time converting in [ns]")
  )

  private class SelectionVectorRowIterator(val columnarBatch: ColumnarBatch, val selectionVector: SelectionVector) extends Iterator[InternalRow] {
    private var i = 0

    override def hasNext: Boolean = i < selectionVector.getRecordCount

    override def next(): InternalRow = {
      println(s"$i of ${selectionVector.getRecordCount}")
      if (i >= selectionVector.getRecordCount) throw new NoSuchElementException
      val nextIndex = selectionVector.getIndex(i)
      i = i + 1
      columnarBatch.getRow(nextIndex)
    }
  }
}
