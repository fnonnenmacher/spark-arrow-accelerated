package nl.tudelft.ewi.abs.nonnenmacher.partial.projection

import nl.tudelft.ewi.abs.nonnenmacher.JNIProcessorFactory
import nl.tudelft.ewi.abs.nonnenmacher.utils.AutoCloseProcessingHelper._
import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.ColumnarBatchArrowConverter.{ColumnarBatchToVectorRoot, VectorRootToColumnarBatch}
import org.apache.spark.sql.ColumnarBatchWithSelectionVector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

case class FPGAProjectExec(child: SparkPlan, name: String, outputs: Seq[Attribute])
  extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    child.executeColumnar().mapPartitions(batchIter => {

      batchIter
        .map(ColumnarBatchWithSelectionVector.from)
        .mapAndAutoClose(new PFGAProjection)
        .map(_.toColumnarBatch)
    })

  }
    private class PFGAProjection extends ClosableFunction[ColumnarBatchWithSelectionVector, ColumnarBatchWithSelectionVector] {

      private val inputSchema = ArrowUtils.toArrowSchema(child.schema, null) //TODO
      private val outputSchema = ArrowUtils.toArrowSchema(schema, null) //TODO

      private val processor = JNIProcessorFactory.threeIntAddingProcessor(inputSchema, outputSchema);


      override def apply(batch: ColumnarBatchWithSelectionVector): ColumnarBatchWithSelectionVector = {
        val root = new VectorSchemaRoot(batch.fieldVectors.asJava)
        root.setRowCount(batch.fieldVectorRows)
        val rootOut = processor.apply(root)

        new sql.ColumnarBatchWithSelectionVector(rootOut.getFieldVectors.asScala, rootOut.getRowCount,batch.selectionVector)
      }

      override def close(): Unit = processor.close()
    }

  override def output: Seq[Attribute] = outputs
}

object FPGAProjectExec {
  def apply(child: SparkPlan, fpgaModule: FPGAModule): FPGAProjectExec = {
    val outputType = ArrowUtils.fromArrowField(fpgaModule.output)
    val attrOut = AttributeReference(fpgaModule.output.getName, outputType)()
    FPGAProjectExec(child, fpgaModule.name, Seq(attrOut))
  }
}