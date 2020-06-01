package nl.tudelft.ewi.abs.nonnenmacher


import java.util.function.UnaryOperator

import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.gandiva.evaluator.VectorExpander
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{BaseVariableWidthVector, VectorSchemaRoot, VectorUnloader}

import scala.collection.JavaConverters._

class JNIProcessor(ptr: Long, val outputSchema: Schema) extends UnaryOperator[VectorSchemaRoot] with ClosableFunction[VectorSchemaRoot,VectorSchemaRoot] {

  private lazy val allocator = GlobalAllocator.newChildAllocator(this.getClass)
  private lazy val rootOut = VectorSchemaRoot.create(outputSchema, allocator)

  override def apply(rootIn: VectorSchemaRoot): VectorSchemaRoot = {
    val buffersIn = BufferDescriptor(rootIn)

    //allocate output memory
    rootOut.setRowCount(rootIn.getRowCount)
    val buffersOut = BufferDescriptor(rootOut)

    process(ptr, rootIn.getRowCount, buffersIn.addresses, buffersIn.sizes, expanderOf(rootOut), buffersOut.addresses, buffersOut.sizes)

    buffersIn.close()
    buffersOut.close()

    rootOut
  }

  @native private def process(ptr: Long, rowNumbers: Int, inBufAddrs: Array[Long], inBufSized: Array[Long], javaExpander: VectorExpander, outBufAddrs: Array[Long], outBufSized: Array[Long]);

  @native private def close(ptr: Long): Unit;

  private case class BufferDescriptor(root: VectorSchemaRoot) {
    def close(): Any = recordBatch.close();

    lazy val recordBatch: ArrowRecordBatch = new VectorUnloader(root).getRecordBatch
    lazy val rowCount: Int = root.getRowCount
    lazy val addresses: Array[Long] = recordBatch.getBuffers.asScala.map(_.memoryAddress()).toArray
    lazy val sizes: Array[Long] = recordBatch.getBuffersLayout.asScala.map(_.getSize()).toArray

  }

  private def expanderOf(root: VectorSchemaRoot): VectorExpander = {
    val variableWidthVectors = root.getFieldVectors.asScala
      .filter(_.isInstanceOf[BaseVariableWidthVector])
      .map(_.asInstanceOf[BaseVariableWidthVector])
      .toArray
    new VectorExpander(variableWidthVectors)
  }

  override def close(): Unit = {
    rootOut.close()
    close(ptr)
  };
}
