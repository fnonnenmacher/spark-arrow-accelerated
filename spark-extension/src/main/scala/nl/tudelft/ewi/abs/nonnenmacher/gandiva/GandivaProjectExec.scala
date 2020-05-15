package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import nl.tudelft.ewi.abs.nonnenmacher.utils.AutoCloseProcessingHelper._
import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.gandiva.evaluator.Projector
import org.apache.arrow.gandiva.expression.TreeBuilder
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot, VectorUnloader}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.ColumnarBatchArrowConverter.{ColumnarBatchToVectorRoot, VectorRootToColumnarBatch}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

case class GandivaProjectExec(child: SparkPlan, projectionList: Seq[NamedExpression]) extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  lazy val outputs: Seq[Attribute] = {
    projectionList.map(expr => AttributeReference(expr.name, expr.dataType)())
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  private def toField(attribute: Attribute): Field = {
    ArrowUtils.toArrowField(attribute.name, attribute.dataType, attribute.nullable, conf.sessionLocalTimeZone);
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    child.executeColumnar().mapPartitions { batchIter =>

      batchIter.mapAndAutoClose(ColumnarBatchToVectorRoot)
        .mapAndAutoClose(new GandivaProjection)
        .map(VectorRootToColumnarBatch)
    }
  }

  private class GandivaProjection extends ClosableFunction[VectorSchemaRoot, VectorSchemaRoot] {

    private val allocator: BufferAllocator = ArrowUtils.rootAllocator.newChildAllocator(s"${this.getClass.getSimpleName}", 0, Long.MaxValue)
    private val treeNodes = projectionList.map(GandivaExpressionConverter.transform)
    private val expressionTrees = treeNodes.zip(outputs).map { case (node, attr) => TreeBuilder.makeExpression(node, toField(attr)) }
    private val gandivaProjector: Projector = Projector.make(ArrowUtils.toArrowSchema(child.schema, conf.sessionLocalTimeZone), expressionTrees.asJava)
    private val rootOut = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(schema, conf.sessionLocalTimeZone), allocator)

    override def apply(rootIn: VectorSchemaRoot): VectorSchemaRoot = {
      val batchIn = new VectorUnloader(rootIn).getRecordBatch

      //allocate memory for  all field vectors!
      rootOut.setRowCount(rootIn.getRowCount)

      val vectors = rootOut.getFieldVectors.asScala.map(_.asInstanceOf[ValueVector]).asJava
      gandivaProjector.evaluate(batchIn, vectors)

      batchIn.close()

      rootOut
    }

    override def close(): Unit = {
      gandivaProjector.close()
      rootOut.close()
      allocator.close()
    }
  }

  override def output: Seq[Attribute] = outputs
}

