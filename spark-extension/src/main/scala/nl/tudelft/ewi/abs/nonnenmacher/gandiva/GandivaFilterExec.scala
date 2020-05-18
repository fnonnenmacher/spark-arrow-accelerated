package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import nl.tudelft.ewi.abs.nonnenmacher.utils.AutoCloseProcessingHelper._
import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.gandiva.evaluator.{Filter, SelectionVector, SelectionVectorInt16, SelectionVectorInt32}
import org.apache.arrow.gandiva.expression.TreeBuilder.makeCondition
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.ColumnarBatchArrowConverter.ColumnarBatchToVectorRoot
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._

case class GandivaFilterExec(child: SparkPlan, filterExpression: Expression) extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { batchIter =>
      batchIter.mapAndAutoClose(ColumnarBatchToVectorRoot)
        .mapAndAutoClose(new GandivaFilter)
    }
  }

  private class GandivaFilter extends ClosableFunction[VectorSchemaRoot, ColumnarBatch] {

    private val allocator: BufferAllocator = ArrowUtils.rootAllocator.newChildAllocator(s"${this.getClass.getSimpleName}", 0, Long.MaxValue)
    private val gandivaCondition = makeCondition(GandivaExpressionConverter.transform(filterExpression))
    private val gandivaFilter: Filter = Filter.make(ArrowUtils.toArrowSchema(child.schema, conf.sessionLocalTimeZone), gandivaCondition)

    var batchOut : ColumnarBatch = _;

    override def apply(rootIn: VectorSchemaRoot): ColumnarBatch = {
      val batchIn = new VectorUnloader(rootIn).getRecordBatch

      val selectionVector: SelectionVector = newSelectionVector(rootIn.getRowCount)

      gandivaFilter.evaluate(batchIn, selectionVector)
      batchIn.close()

      ArrowColumnVectorWithAccessibleFieldVectorAndSelectionVec

      if (batchOut != null) batchOut.close()
      batchOut = toRecordBatch(rootIn, selectionVector)
      batchOut
    }

    def newSelectionVector(numRows: Int): SelectionVector = {
      if (numRows > 65536) { //more than 2 bytes to store one index
        val selectionBuffer = allocator.buffer(numRows * 4)
        new SelectionVectorInt32(selectionBuffer) //INT 32 -> 4 Bytes per index => Max 2^32 rows
      } else {
        val selectionBuffer = allocator.buffer(numRows * 2)
        new SelectionVectorInt16(selectionBuffer) //INT 16 -> 2 Bytes per index => Max 2^16 rows
      }
    }

    override def close(): Unit = {
      batchOut.close()
      gandivaFilter.close()
      allocator.close()
    }

    def toRecordBatch(root: VectorSchemaRoot, selectionVector: SelectionVector): ColumnarBatch = {
      val arrowVectors = root.getFieldVectors.asScala.map(x => new ArrowColumnVectorWithAccessibleFieldVectorAndSelectionVec(selectionVector, x)).toArray[ColumnVector]

      //combine all column vectors in ColumnarBatch
      val batch = new ColumnarBatch(arrowVectors)
      batch.setNumRows(selectionVector.getRecordCount)
      batch
    }
  }

  override def output: Seq[Attribute] = child.output
}

