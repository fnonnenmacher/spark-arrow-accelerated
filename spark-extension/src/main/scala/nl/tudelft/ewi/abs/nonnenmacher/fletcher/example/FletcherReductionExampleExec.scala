package org.apache.spark.sql

import nl.tudelft.ewi.abs.nonnenmacher.FletcherReductionProcessor
import nl.tudelft.ewi.abs.nonnenmacher.utils.AutoCloseProcessingHelper._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtils

case class FletcherReductionExampleExec(out: Seq[Attribute], child: SparkPlan) extends UnaryExecNode {

  override def doExecute(): RDD[InternalRow] = {
    //    val aggregationTime = longMetric("aggregationTime")
    //    val processing = longMetric("processing")

    child.executeColumnar().mapPartitions { batches =>

      val inputSchema = ArrowUtils.toArrowSchema(child.schema, conf.sessionLocalTimeZone)

      batches
        .map(VectorSchemaRootUtil.from)
        .mapAndAutoClose(new FletcherReductionProcessor(inputSchema))
        .map(toRow)
    }
  }

  private def toRow(res: Long): InternalRow = {
    val arr: Array[Any] = Array(res)
    new GenericInternalRow(arr)
  }

  override def output: Seq[Attribute] = out
}
