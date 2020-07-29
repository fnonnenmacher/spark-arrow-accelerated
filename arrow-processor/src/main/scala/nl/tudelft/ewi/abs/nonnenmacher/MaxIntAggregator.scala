package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.gandiva.evaluator.NativeLibraryLoader
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}

import scala.collection.JavaConverters._

class MaxIntAggregator {

  def aggregate(rootIn: VectorSchemaRoot): Array[Int] = {
    NativeLibraryLoader.load()


    val buffersIn = BufferDescriptor(rootIn)
    val results = new Array[Int](rootIn.getFieldVectors.size)

    agg(buffersIn.rowCount, buffersIn.addresses, buffersIn.sizes, results)

    buffersIn.close()
    results
  }

  @native private def agg(rowNumbers: Int, inBufAddrs: Array[Long], inBufSized: Array[Long], results :Array[Int]): Unit

  private case class BufferDescriptor(root: VectorSchemaRoot) {
    def close(): Any = recordBatch.close();

    lazy val recordBatch: ArrowRecordBatch = new VectorUnloader(root).getRecordBatch
    lazy val rowCount: Int = root.getRowCount
    lazy val addresses: Array[Long] = recordBatch.getBuffers.asScala.map(_.memoryAddress()).toArray
    lazy val sizes: Array[Long] = recordBatch.getBuffersLayout.asScala.map(_.getSize()).toArray
  }
}
