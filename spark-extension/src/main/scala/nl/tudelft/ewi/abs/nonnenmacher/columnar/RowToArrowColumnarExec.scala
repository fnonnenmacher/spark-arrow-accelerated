package nl.tudelft.ewi.abs.nonnenmacher.columnar

import nl.tudelft.ewi.abs.nonnenmacher.columnar.VectorSchemaRootUtil.toBatch
import nl.tudelft.ewi.abs.nonnenmacher.utils.StartStopMeasurment
import org.apache.arrow.gandiva.evaluator.SelectionVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch


object ArrowColumnarExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectColumnar(_ => ArrowColumnarConversionRule)
  }
}

case class ConvertToArrowColumnsRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan match {
      case ColumnarToRowExec(child) => {
        if (child.isInstanceOf[ColumnarWithSelectionVectorSupport])
          new ColumnarWithSelectionToRowExec(apply(child))
        else
          plan.withNewChildren(plan.children.map(apply))
      }
      case RowToColumnarExec(child) => new RowToArrowColumnarExec(apply(child))
      case plan => plan.withNewChildren(plan.children.map(apply))
    }
  }
}

class RowToArrowColumnarExec(override val child: SparkPlan) extends RowToColumnarExec(child) {

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val maxRecordsPerBatch = conf.columnBatchSize

    child.execute().mapPartitions { rowIter =>

      val allocator =
        SparkArrowUtils.rootAllocator.newChildAllocator(s"${this.getClass.getSimpleName}", 0, Long.MaxValue)

      val arrowSchema = SparkArrowUtils.toArrowSchema(schema, conf.sessionLocalTimeZone)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      val arrowWriter = ArrowWriter.create(root)

      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        root.close()
        allocator.close()
      }

      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = rowIter.hasNext || {
          root.close()
          allocator.close()
          false
        }

        override def next(): ColumnarBatch = {

          //reset previously set field vectors
          arrowWriter.reset();

          var rowCount = 0
          while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
            val row = rowIter.next()
            arrowWriter.write(row)
            rowCount += 1
          }
          arrowWriter.finish()

          root.setRowCount(rowCount)
          toBatch(root)
        }
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
    other.isInstanceOf[RowToArrowColumnarExec]
  }

  override def hashCode(): Int = super.hashCode()
}

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

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
    "conversionTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time converting in [ns]")
  )

  override def hashCode(): Int = super.hashCode()
}

class SelectionVectorRowIterator(val columnarBatch: ColumnarBatch, val selectionVector: SelectionVector) extends Iterator[InternalRow] {
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
