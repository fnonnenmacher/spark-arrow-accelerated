package nl.tudelft.ewi.abs.nonnenmacher.partial.projection

import nl.tudelft.ewi.abs.nonnenmacher.JNIProcessorFactory
import nl.tudelft.ewi.abs.nonnenmacher.columnar.ArrowColumnarConverters._
import nl.tudelft.ewi.abs.nonnenmacher.utils.AutoCloseProcessingHelper._
import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkArrowUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class FPGAProjectExec(child: SparkPlan, name: String, outputs: Seq[Attribute])
  extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    child.executeColumnar().mapPartitions(batchIter => {
      val fpgaProjection = new FPGAProjection

      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        fpgaProjection.close()
      }

      batchIter
        .map(_.toArrow)
        .mapAndAutoClose(fpgaProjection)
        .map(_.toBatch)
    })
  }

  private class FPGAProjection extends ClosableFunction[VectorSchemaRoot, VectorSchemaRoot] {

    private var isClosed = false
    private val inputSchema = SparkArrowUtils.toArrowSchema(child.schema, null) //TODO
    private val outputSchema = SparkArrowUtils.toArrowSchema(schema, null) //TODO
    private val processor = JNIProcessorFactory.threeIntAddingProcessor(inputSchema, outputSchema);


    override def apply(root: VectorSchemaRoot): VectorSchemaRoot = {
      processor.apply(root)
    }

    override def close(): Unit = {
      if (!isClosed) {
        isClosed = true
        processor.close()
      }
    }
  }

  override def output: Seq[Attribute] = outputs
}

object FPGAProjectExec {
  def apply(child: SparkPlan, fpgaModule: FPGAModule): FPGAProjectExec = {
    val outputType = SparkArrowUtils.fromArrowField(fpgaModule.output)
    val attrOut = AttributeReference(fpgaModule.output.getName, outputType)()
    FPGAProjectExec(child, fpgaModule.name, Seq(attrOut))
  }
}