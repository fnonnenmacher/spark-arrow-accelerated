package org.apache.spark.sql

import nl.tudelft.ewi.abs.nonnenmacher.{MaxIntAggregator, TimeMeasuringIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{ColumnarRule, ColumnarToRowExec, SparkPlan, UnaryExecNode}

object DirtyMaxAggregationConversionRule extends ColumnarRule {
  override def postColumnarTransitions: Rule[SparkPlan] = {
    case ColumnarToRowExec(child) => ColumnarToRowMaxAggregatorExec(postColumnarTransitions(child))
    case plan => plan.withNewChildren(plan.children.map(postColumnarTransitions(_)))
  }
}

object DirtyMaxAggregationExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions) {
    e.injectColumnar(_ => DirtyMaxAggregationConversionRule)
  }
}

case class ColumnarToRowMaxAggregatorExec(override val child: SparkPlan) extends UnaryExecNode {

  override def doExecute(): RDD[InternalRow] = {
    val aggregationTime = longMetric("aggregationTime")
    val processing = longMetric("processing")

    child.executeColumnar().mapPartitionsInternal { batches =>
      //      val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
      val maxIntAggregator = new MaxIntAggregator()

      val iter = batches.map { batch =>

        val start = System.nanoTime()

        val root = VectorSchemaRootUtil.from(batch)
        val res = maxIntAggregator.aggregate(root)

        aggregationTime += System.nanoTime() - start

        val ints: Array[Any] = Array(res)
        new GenericInternalRow(ints)
      }

      new TimeMeasuringIterator(iter, processing)
    }
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "aggregationTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time aggregating in [ns]"),
    "processing" -> SQLMetrics.createNanoTimingMetric(sparkContext, "total processing time")
  )

  override def output: Seq[Attribute] = child.output
}