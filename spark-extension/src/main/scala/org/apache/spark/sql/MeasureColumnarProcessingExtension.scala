package org.apache.spark.sql

import nl.tudelft.ewi.abs.nonnenmacher.TimeMeasuringIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{ColumnarRule, ColumnarToRowExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

object MeasureColumnarProcessingExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectColumnar(_ => MeasureColumnarProcessingRule)
  }
}

object MeasureColumnarProcessingRule extends ColumnarRule {
  override def postColumnarTransitions: Rule[SparkPlan] = {
    case ColumnarToRowExec(child) => ColumnarToRowExec(MeasureColumnarProcessingExec(postColumnarTransitions(child)))
    case plan => plan.withNewChildren(plan.children.map(postColumnarTransitions(_)))
  }
}


case class MeasureColumnarProcessingExec(override val child: SparkPlan) extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${classOf[MeasureColumnarProcessingExec].getSimpleName} only supports columnar processing.")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val conversionTime = longMetric("columnarProcessing")

    child.executeColumnar().mapPartitionsInternal { batches =>
      new TimeMeasuringIterator[ColumnarBatch](batches, conversionTime)
    }
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "columnarProcessing" -> SQLMetrics.createNanoTimingMetric(sparkContext, "columnar processing converting in [ns]")
  )

  override def output: Seq[Attribute] = child.output
}