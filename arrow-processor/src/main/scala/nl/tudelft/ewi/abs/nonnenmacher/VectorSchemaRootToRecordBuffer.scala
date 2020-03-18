package nl.tudelft.ewi.abs.nonnenmacher

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels.newChannel

import GlobalAllocator.newChildAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{FieldVector, UInt1Vector, VectorLoader, VectorSchemaRoot, VectorUnloader}


object VectorSchemaRootToRecordBuffer {

  private val allocator = newChildAllocator(VectorSchemaRootToRecordBuffer.getClass)


  def convert(root: VectorSchemaRoot): Array[Byte] = {

    // Unload record batch from vector
    val unloader = new VectorUnloader(root)
    val recordBatch = unloader.getRecordBatch

    val root2 = VectorSchemaRoot.create(root.getSchema, allocator)
    val out = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root2, null, newChannel(out))
    val loader = new VectorLoader(root2)
    loader.load(recordBatch)

    writer.writeBatch()
    out.toByteArray
  }
}
