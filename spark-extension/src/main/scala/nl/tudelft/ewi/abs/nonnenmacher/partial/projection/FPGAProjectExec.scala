package nl.tudelft.ewi.abs.nonnenmacher.partial.projection

import nl.tudelft.ewi.abs.nonnenmacher.JNIProcessorFactory
import nl.tudelft.ewi.abs.nonnenmacher.utils.AutoCloseProcessingHelper._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.ColumnarBatchArrowConverter.{ColumnarBatchToVectorRoot, VectorRootToColumnarBatch}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

case class FPGAProjectExec(child: SparkPlan, name: String, outputs: Seq[Attribute])
  extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {


    child.executeColumnar().mapPartitions(batchIter => {

      val inputSchema = ArrowUtils.toArrowSchema(child.schema, null) //TODO
      val outputSchema = ArrowUtils.toArrowSchema(schema, null) //TODO

      val processor = JNIProcessorFactory.threeIntAddingProcessor(inputSchema, outputSchema);

      batchIter
        .mapAndAutoClose(ColumnarBatchToVectorRoot)
        .mapAndAutoClose(processor)
        .map(VectorRootToColumnarBatch)
    })
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