package nl.tudelft.ewi.abs.nonnenmacher

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels
import java.nio.channels.Channels.newChannel

import nl.tudelft.ewi.abs.nonnenmacher.GlobalAllocator.newChildAllocator
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}


object ArrowRootByteConverter {

  private val allocator = newChildAllocator(ArrowRootByteConverter.getClass)

  def convert(data: Array[Byte]): VectorSchemaRoot = {

    val inputStream = new ByteArrayInputStream(data)
    val streamReader = new ArrowStreamReader(inputStream, allocator)
    streamReader.loadNextBatch();
    streamReader.getVectorSchemaRoot
  }

  def convert(root: VectorSchemaRoot): Array[Byte] = {
    val out  = new ByteArrayOutputStream()
    val writeChannel = new WriteChannel(Channels.newChannel(out))
    val recordBatch = new VectorUnloader(root).getRecordBatch
    MessageSerializer.serialize(writeChannel, root.getSchema); //TODO: Really necessary to transfer. Maybe schema is clear
    MessageSerializer.serialize(writeChannel, recordBatch);
    recordBatch.close()
    out.toByteArray
  }
}
