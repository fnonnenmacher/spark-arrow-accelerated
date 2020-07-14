package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import io.netty.buffer.ArrowBuf
import nl.tudelft.ewi.abs.nonnenmacher.columnar.{ColumnarWithSelectionVectorSupport, VectorSchemaRootUtil}
import nl.tudelft.ewi.abs.nonnenmacher.utils.AutoCloseProcessingHelper._
import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.gandiva.evaluator.{Filter, SelectionVector, SelectionVectorInt16}
import org.apache.arrow.gandiva.expression.TreeBuilder.makeCondition
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.SparkArrowUtils

import scala.collection.JavaConverters._

case class GandivaFilterExec(filterExpression: Expression, child: SparkPlan) extends UnaryExecNode with ColumnarWithSelectionVectorSupport {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does not support regular columnar data processing. Only in combination with the SelectionVector")
  }

  override protected def doExecuteColumnarWithSelection(): RDD[(ColumnarBatch, SelectionVector)] = {
    val time = longMetric("time")

    child.executeColumnar().mapPartitions { batchIter =>
      var start: Long = 0
      val gandivaFilter = new GandivaFilter

      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        gandivaFilter.close()
      }
      batchIter
        .map { x => start = System.nanoTime(); x }
        .map(VectorSchemaRootUtil.from)
        .mapAndAutoClose(gandivaFilter)
        .map { case (root, selectionVector) => (VectorSchemaRootUtil.toBatch(root), selectionVector) }
        .map { x => time += System.nanoTime() - start; x }
    }
  }

  private class GandivaFilter extends ClosableFunction[VectorSchemaRoot, (VectorSchemaRoot, SelectionVector)] {

    private var isClosed = false;
    private val allocator: BufferAllocator = SparkArrowUtils.rootAllocator.newChildAllocator(s"${this.getClass.getSimpleName}", 0, Long.MaxValue)
    private val gandivaCondition = makeCondition(GandivaExpressionConverter.transform(filterExpression))
    private val gandivaFilter: Filter = Filter.make(SparkArrowUtils.toArrowSchema(child.schema, conf.sessionLocalTimeZone), gandivaCondition)
    private var selectionVector: Option[SelectionVector] = Option.empty

    override def apply(rootIn: VectorSchemaRoot): (VectorSchemaRoot, SelectionVector) = {
      selectionVector.foreach(_.getBuffer.close())

      val buffers: Seq[ArrowBuf] = rootIn.getFieldVectors.asScala.flatMap(f => f.getFieldBuffers.asScala)
      selectionVector = Option(newSelectionVector(rootIn.getRowCount))

      gandivaFilter.evaluate(rootIn.getRowCount, buffers.asJava, selectionVector.get)

      (rootIn, selectionVector.get)
    }

    def newSelectionVector(numRows: Int): SelectionVector = {
      if (numRows <= 65536) { //more than 2 bytes to store one index
        val selectionBuffer = allocator.buffer(numRows * 2)
        new SelectionVectorInt16(selectionBuffer) //INT 16 -> 2 Bytes per index => Max 2^16 rows
      } else {
        throw new IllegalAccessException("batch size > 65536 currently not supported")
        //        val selectionBuffer = allocator.buffer(numRows * 4)
        //        new SelectionVectorInt32(selectionBuffer) //INT 32 -> 4 Bytes per index => Max 2^32 rows
      }
    }

    override def close(): Unit = {
      if (!isClosed) {
        isClosed = true
        gandivaFilter.close()
        selectionVector.foreach(_.getBuffer.close())
        allocator.close()
      }
    }
  }

  override def output: Seq[Attribute] = child.output

  override lazy val metrics = Map("time" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time in [ns]"))
}

