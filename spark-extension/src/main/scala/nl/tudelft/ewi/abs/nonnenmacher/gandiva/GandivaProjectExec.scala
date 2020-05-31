package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import io.netty.buffer.ArrowBuf
import nl.tudelft.ewi.abs.nonnenmacher.utils.AutoCloseProcessingHelper._
import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.gandiva.evaluator.Projector
import org.apache.arrow.gandiva.expression.TreeBuilder
import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.{ArrowBatch, ArrowBatchWithSelection, ColumnarBatchWrapper}

import scala.collection.JavaConverters._

case class GandivaProjectExec(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  lazy val outputs: Seq[Attribute] = projectList.map(_.toAttribute)

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  private def toField(attribute: Attribute): Field = {
    ArrowUtils.toArrowField(attribute.name, attribute.dataType, attribute.nullable, conf.sessionLocalTimeZone);
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val time = longMetric("time")

    child.executeColumnar().mapPartitions { batchIter =>

      var start: Long = 0

      batchIter
        .map { x => start = System.nanoTime(); x }
        .map(ColumnarBatchWrapper.from)
        .mapAndAutoClose(new GandivaProjection)
        .map(_.toColumnarBatch)
        .map { x => time += System.nanoTime() - start; x }
    }
  }

  private class GandivaProjection extends ClosableFunction[ColumnarBatchWrapper, ColumnarBatchWrapper] {

    private val selectionVectorType = if (child.isInstanceOf[GandivaFilterExec]) SelectionVectorType.SV_INT16 else SelectionVectorType.SV_NONE
    private val allocator: BufferAllocator = ArrowUtils.rootAllocator.newChildAllocator(s"${this.getClass.getSimpleName}", 0, Long.MaxValue)
    private val treeNodes = projectList.map(GandivaExpressionConverter.transform)
    private val expressionTrees = treeNodes.zip(outputs).map { case (node, attr) => TreeBuilder.makeExpression(node, toField(attr)) }
    private val gandivaProjector: Projector = Projector.make(ArrowUtils.toArrowSchema(child.schema, conf.sessionLocalTimeZone), expressionTrees.asJava, selectionVectorType)
    private val rootOut = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(schema, conf.sessionLocalTimeZone), allocator)

    override def apply(batchIn: ColumnarBatchWrapper): ColumnarBatchWrapper = {
      val buffers: Seq[ArrowBuf] = batchIn.fieldVectors.flatMap(f => f.getFieldBuffers.asScala)

      rootOut.clear()

      batchIn match {
        case ArrowBatch(_, numRows) => {
          rootOut.setRowCount(numRows) //allocate memory for  all field vectors!
          val vectors = rootOut.getFieldVectors.asScala.map(_.asInstanceOf[ValueVector])
          gandivaProjector.evaluate(numRows, buffers.asJava, vectors.asJava)
        }
        case ArrowBatchWithSelection(_, numRows, selectionVector) => {
          rootOut.setRowCount(selectionVector.getRecordCount)
          val vectors = rootOut.getFieldVectors.asScala.map(_.asInstanceOf[ValueVector]) //allocate memory for  all field vectors!
          gandivaProjector.evaluate(numRows, buffers.asJava, selectionVector, vectors.asJava)
        }
      }

      ColumnarBatchWrapper(rootOut)
    }

    override def close(): Unit = {
      gandivaProjector.close()
      rootOut.close()
      allocator.close()
    }
  }

  override def output: Seq[Attribute] = outputs

  override lazy val metrics = Map("time" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time in [ns]"))

}

