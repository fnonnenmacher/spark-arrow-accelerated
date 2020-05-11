package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import org.apache.arrow.gandiva.evaluator.Projector
import org.apache.arrow.gandiva.expression.TreeBuilder
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot, VectorUnloader}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.sql.{ArrowColumnVectorWithAccessibleFieldVector, vectorized}

import scala.collection.JavaConverters._

case class GandivaProjectExec(child: SparkPlan, projectionList: Seq[NamedExpression]) extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  lazy val outputs : Seq[Attribute]= {
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

      //TODO CLOSE
      val allocator: BufferAllocator = ArrowUtils.rootAllocator.newChildAllocator(s"${this.getClass.getSimpleName}", 0, Long.MaxValue)
      val treeNodes = projectionList.map(GandivaExpressionConverter.transform)

      //TODO RELEASE
      val expressionTrees = treeNodes.zip(outputs).map { case (node, attr) => TreeBuilder.makeExpression(node, toField(attr)) }
      val gandivaProjector: Projector = Projector.make(ArrowUtils.toArrowSchema(child.schema, conf.sessionLocalTimeZone), expressionTrees.asJava)

      val root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(schema, conf.sessionLocalTimeZone), allocator)

      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        gandivaProjector.close()

        root.close()
        allocator.close()
      }

      batchIter.map { batch =>

        val recordBatch = toRecordBatch(batch);

        root.setRowCount(batch.numRows()) //allocate for all field vectors!

        val vectors = root.getFieldVectors.asScala.map(_.asInstanceOf[ValueVector]).asJava
        gandivaProjector.evaluate(recordBatch, vectors)

        recordBatch.close()
        val resultBatch = toResultBatch(root)

        TaskContext.get().addTaskCompletionListener[Unit] ( _ => resultBatch.close())

        resultBatch
      }
    }
  }

  private def toRecordBatch(batch: ColumnarBatch): ArrowRecordBatch = {
    val fieldVectors = (0 until batch.numCols).map(batch.column).map {
      case ArrowColumnVectorWithAccessibleFieldVector(fieldVector) => fieldVector
      case _ => throw new IllegalStateException(s"${getClass.getSimpleName} does only support columnar data in arrow format.")
    }
    val root = new VectorSchemaRoot(fieldVectors.asJava)
    new VectorUnloader(root).getRecordBatch
  }

  private def toResultBatch(root: VectorSchemaRoot): ColumnarBatch = {
    // map arrow field vectors to spark ArrowColumnVector
    val arrowVectors = root.getFieldVectors.asScala.map(x => new vectorized.ArrowColumnVector(x)).toArray[ColumnVector]

    //combine all column vectors in ColumnarBatch
    new ColumnarBatch(arrowVectors, root.getRowCount);
  }

  override def output: Seq[Attribute] = outputs
}

