package nl.tudelft.ewi.abs.nonnenmacher

import java.util.logging.Logger

import io.netty.buffer.ArrowBuf
import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.gandiva.evaluator.NativeLibraryLoader
import org.apache.arrow.gandiva.expression.ArrowTypeHelper
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}

import scala.collection.JavaConverters._

class FletcherReductionProcessor(schema: Schema) extends ClosableFunction[VectorSchemaRoot, Long] {

  private val log = Logger.getLogger(classOf[FletcherReductionProcessor].getName)

  private var isClosed = false
  private val procId: Long = {
    NativeLibraryLoader.load()
    val schemaAsBytes = ArrowTypeHelper.arrowSchemaToProtobuf(schema).toByteArray
    initFletcherReductionProcessor(schemaAsBytes)
  }

  def apply(rootIn: VectorSchemaRoot): Long = {
    val buffersIn = BufferDescriptor(rootIn)

    buffersIn.assertAre64ByteAligned()

    val r = reduce(procId, buffersIn.rowCount, buffersIn.addresses, buffersIn.sizes)
    buffersIn.close()
    r
  }

  @native private def initFletcherReductionProcessor(schemaAsBytes: Array[Byte]): Long

  @native private def reduce(procId: Long, rowNumbers: Int, inBufAddrs: Array[Long], inBufSized: Array[Long]): Long;

  @native private def close(procId: Long): Unit;

  private case class BufferDescriptor(root: VectorSchemaRoot) {
    def close(): Any = recordBatch.close();

    lazy val recordBatch: ArrowRecordBatch = new VectorUnloader(root).getRecordBatch
    lazy val buffers: Seq[ArrowBuf] = recordBatch.getBuffers.asScala
    lazy val rowCount: Int = root.getRowCount
    lazy val addresses: Array[Long] = buffers.map(_.memoryAddress()).toArray
    lazy val sizes: Array[Long] = buffers.map(_.readableBytes()).toArray


    def assertAre64ByteAligned(): Unit = {

      val sb = new StringBuilder()
      var isAligned = true

      buffers.foreach { b =>
        sb.append(s"Addr: ${b.memoryAddress().toHexString} % 64 = ${b.memoryAddress() % 64} ")
        sb.append(s"Capacity: ${b.capacity()} % 64 = ${b.capacity() % 64} ")
        sb.append("\n")

        isAligned &= b.memoryAddress() % 64 == 0
        isAligned &= b.capacity() % 64 == 0
      }

      if (!isAligned) {
        log.warning("Buffers are not aligned. \n" + sb.toString())
      }
    }
  }

  override def close(): Unit = {
    if (!isClosed) {
      isClosed = true
      close(procId)
    }
  }
}
