package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import io.netty.buffer.ArrowBuf
import nl.tudelft.ewi.abs.nonnenmacher.utils.AutoCloseProcessingHelper._
import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.gandiva.evaluator.{Filter, SelectionVector, SelectionVectorInt16}
import org.apache.arrow.gandiva.expression.TreeBuilder.makeCondition
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.{ArrowBatchWithSelection, ColumnarBatchWrapper}

import scala.collection.JavaConverters._

case class GandivaFilterExec(filterExpression: Expression, child: SparkPlan) extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { batchIter =>
      batchIter
        .map(ColumnarBatchWrapper.from)
        .mapAndAutoClose(new GandivaFilter)
        .map(_.toColumnarBatch)
    }
  }

  private class GandivaFilter extends ClosableFunction[ColumnarBatchWrapper, ColumnarBatchWrapper] {

    private val allocator: BufferAllocator = ArrowUtils.rootAllocator.newChildAllocator(s"${this.getClass.getSimpleName}", 0, Long.MaxValue)
    private val gandivaCondition = makeCondition(GandivaExpressionConverter.transform(filterExpression))
    private val gandivaFilter: Filter = Filter.make(ArrowUtils.toArrowSchema(child.schema, conf.sessionLocalTimeZone), gandivaCondition)
    private var selectionVector: SelectionVector = _


    override def apply(batchIn: ColumnarBatchWrapper): ColumnarBatchWrapper = {
      if (selectionVector != null) selectionVector.getBuffer.close()

      val buffers: Seq[ArrowBuf] = batchIn.fieldVectors.flatMap(f => f.getFieldBuffers.asScala)
      selectionVector = newSelectionVector(batchIn.numRows)

      gandivaFilter.evaluate(batchIn.numRows, buffers.asJava, selectionVector)

      ArrowBatchWithSelection(batchIn.fieldVectors, batchIn.numRows, selectionVector)
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
      gandivaFilter.close()
      selectionVector.getBuffer.close()
      allocator.close()
    }
  }

  override def output: Seq[Attribute] = child.output
}

