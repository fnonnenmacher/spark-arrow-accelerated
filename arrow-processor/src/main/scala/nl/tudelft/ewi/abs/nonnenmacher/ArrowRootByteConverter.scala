package nl.tudelft.ewi.abs.nonnenmacher

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels.newChannel

import nl.tudelft.ewi.abs.nonnenmacher.GlobalAllocator.newChildAllocator
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}


object ArrowRootByteConverter {

  private val allocator = newChildAllocator(ArrowRootByteConverter.getClass)

  def convert(root: VectorSchemaRoot): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root, null, newChannel(out))
    writer.writeBatch()
    out.toByteArray
  }

  def convert(data: Array[Byte]): VectorSchemaRoot = {

    val inputStream = new ByteArrayInputStream(data)
    val streamReader = new ArrowStreamReader(inputStream, allocator)
    streamReader.loadNextBatch();
    streamReader.getVectorSchemaRoot
  }
}
