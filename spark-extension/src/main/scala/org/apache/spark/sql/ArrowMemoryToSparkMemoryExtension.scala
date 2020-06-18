package org.apache.spark.sql

import nl.tudelft.ewi.abs.nonnenmacher.TimeMeasuringIterator
import org.apache.arrow.vector.IntVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.execution.{ColumnarRule, ColumnarToRowExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._

object ArrowMemoryToSparkMemoryExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectColumnar(_ => ArrowMemoryToSparkMemoryRule)
  }
}

object ArrowMemoryToSparkMemoryRule extends ColumnarRule {
  override def postColumnarTransitions: Rule[SparkPlan] = {
    case ColumnarToRowExec(child) => ColumnarToRowExec(ArrowMemoryToSparkMemoryExec(postColumnarTransitions(child)))
    case plan => plan.withNewChildren(plan.children.map(postColumnarTransitions(_)))
  }
}

case class ArrowMemoryToSparkMemoryExec(override val child: SparkPlan) extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${classOf[ArrowMemoryToSparkMemoryExec].getSimpleName} only supports columnar processing.")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val conversionTime = longMetric("conversionTime")

    child.executeColumnar().mapPartitionsInternal { batches =>

      val newIter = batches.map { arrowBatch =>

        val root = VectorSchemaRootUtil.from(arrowBatch)
        val columnVecs = root.getFieldVectors.asScala.map(fv => convert(fv.asInstanceOf[IntVector])).toArray

        new ColumnarBatch(columnVecs, arrowBatch.numRows())
      }

      new TimeMeasuringIterator[ColumnarBatch](newIter, conversionTime)
    }
  }

  private def convert(intVector: IntVector): ColumnVector = {
    val columnVec = new OnHeapColumnVector(intVector.getValueCount, IntegerType)

    (0 until intVector.getValueCount).foreach(i =>
      columnVec.putInt(i, intVector.get(i))
    )
    columnVec
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "conversionTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time converting in [ns]")
  )

  override def output: Seq[Attribute] = child.output
}