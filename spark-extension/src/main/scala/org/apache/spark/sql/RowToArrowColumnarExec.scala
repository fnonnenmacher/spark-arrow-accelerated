package org.apache.spark.sql

import nl.tudelft.ewi.abs.nonnenmacher.utils.StartStopMeasurment
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{ColumnarRule, ColumnarToRowExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._


/**
 * Spark has added columnar processing support to version 3 See https://issues.apache.org/jira/browse/SPARK-27396 for details.
 *
 * Unfortunately, the team has decided to not use Apache Arrow, yet. However, this structure at least the opportunity to
 * override the existing memory mapping with an arrow based implementation.
 *
 */
object ArrowColumnarConversionRule extends ColumnarRule {
  override def postColumnarTransitions: Rule[SparkPlan] = ConvertToArrowColumnsRule();
}

object ArrowColumnarExtension {
  def apply(): (SparkSessionExtensions => Unit) = { e: SparkSessionExtensions =>
    e.injectColumnar(_ => ArrowColumnarConversionRule)
  }
}

case class ConvertToArrowColumnsRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan match {
      case ColumnarToRowExec(child) => new ArrowColumnarToRowExec(apply(child))
      case RowToColumnarExec(child) => new RowToArrowColumnarExec(apply(child))
      case plan => plan.withNewChildren(plan.children.map(apply))
    }
  }
}

class RowToArrowColumnarExec(override val child: SparkPlan) extends RowToColumnarExec(child) {

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val maxRecordsPerBatch = conf.columnBatchSize

    child.execute().mapPartitionsInternal { rowIter =>

      val allocator =
        ArrowUtils.rootAllocator.newChildAllocator(s"${this.getClass.getSimpleName}", 0, Long.MaxValue)

      val arrowSchema = ArrowUtils.toArrowSchema(schema, conf.sessionLocalTimeZone)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      val arrowWriter = ArrowWriter.create(root)

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
          ColumnarBatchWrapper(root).toColumnarBatch
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

class ArrowColumnarToRowExec(override val child: SparkPlan) extends ColumnarToRowExec(child) with StartStopMeasurment{

  override def supportCodegen: Boolean = false

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val conversionTime = longMetric("conversionTime")
    // This avoids calling `output` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localOutput = this.output
    child.executeColumnar().mapPartitionsInternal { batches =>
      val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
      batches.flatMap { batch =>
        start()
        numInputBatches += 1
        val colBatch = ColumnarBatchWrapper.from(batch)
        numOutputRows += colBatch.numRows
        val res = colBatch.rowIterator
          .map(toUnsafe)
        conversionTime += stop()
        res
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
    other.isInstanceOf[ArrowColumnarToRowExec]
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
    "conversionTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time converting in [ns]")
  )

  override def hashCode(): Int = super.hashCode()
}
