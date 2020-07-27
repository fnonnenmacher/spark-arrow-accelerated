package nl.tudelft.ewi.abs.nonnenmacher.max.aggregation

import nl.tudelft.ewi.abs.nonnenmacher.MaxIntAggregator
import nl.tudelft.ewi.abs.nonnenmacher.columnar.ArrowColumnarConverters._
import nl.tudelft.ewi.abs.nonnenmacher.measuring.TimeMeasuringIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class SimpleMaxAggregationExec(override val child: SparkPlan) extends UnaryExecNode {

  override def doExecute(): RDD[InternalRow] = {
    val aggregationTime = longMetric("aggregationTime")
    val processing = longMetric("processing")

    child.executeColumnar().mapPartitions { batches =>
      //      val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
      val maxIntAggregator = new MaxIntAggregator()

      val iter = batches.map { batch =>

        val start = System.nanoTime()

        val root = batch.toArrow
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
