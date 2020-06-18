package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.gandiva.evaluator.NativeLibraryLoader
import org.apache.arrow.gandiva.expression.ArrowTypeHelper
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}

import scala.collection.JavaConverters._

class FletcherReductionProcessor(schema: Schema) extends ClosableFunction[VectorSchemaRoot,Long]{

  private val procId: Long = {
    NativeLibraryLoader.load()
    val schemaAsBytes = ArrowTypeHelper.arrowSchemaToProtobuf(schema).toByteArray
    initFletcherReductionProcessor(schemaAsBytes)
  }

  def apply(rootIn: VectorSchemaRoot): Long = {
    val buffersIn = BufferDescriptor(rootIn)
    val r = reduce(procId, buffersIn.rowCount, buffersIn.addresses, buffersIn.sizes)
    buffersIn.close()
    r
  }

  @native private def initFletcherReductionProcessor(schemaAsBytes: Array[Byte]): Long

  @native private def reduce(procId: Long, rowNumbers: Int, inBufAddrs: Array[Long], inBufSized: Array[Long]): Long;

  @native private def close(procId: Long) :Unit;

  private case class BufferDescriptor(root: VectorSchemaRoot) {
    def close(): Any = recordBatch.close();

    lazy val recordBatch: ArrowRecordBatch = new VectorUnloader(root).getRecordBatch
    lazy val rowCount: Int = root.getRowCount
    lazy val addresses: Array[Long] = recordBatch.getBuffers.asScala.map(_.memoryAddress()).toArray
    lazy val sizes: Array[Long] = recordBatch.getBuffersLayout.asScala.map(_.getSize()).toArray
  }

  override def close(): Unit = {
    close(procId)
  }
}
