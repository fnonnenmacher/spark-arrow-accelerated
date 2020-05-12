package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.NativeParquetReader.BufferDescriptor
import org.apache.arrow.gandiva.evaluator.VectorExpander
import org.apache.arrow.gandiva.expression.ArrowTypeHelper
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{BaseVariableWidthVector, VectorSchemaRoot, VectorUnloader}

import scala.collection.JavaConverters._

class NativeParquetReader private(val ptr: Long, val outputSchema: Schema, val numRows: Int) extends Iterator[VectorSchemaRoot] {

  private val allocator = GlobalAllocator.newChildAllocator(this.getClass)

  private var _hasNext = true;

  override def hasNext: Boolean = _hasNext

  @native private def readNext(ptr: Long, obj: Object, bufAddrs: Array[Long], bufSizes: Array[Long]): Int;

  @native private def close(ptr: Long): Unit;

  override def next(): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(outputSchema, allocator)
    root.setRowCount(numRows)

    val expander: VectorExpander = expanderOf(root)

    val outputBuffers = new BufferDescriptor(root)

    val numRowsRead: Int = readNext(ptr, expander, outputBuffers.addresses, outputBuffers.sizes);

    if (numRowsRead < numRows) {
      _hasNext = false
      //      root.setRowCount(numRowsRead)
      close(ptr)
    }

    root
  }

  private def expanderOf(root: VectorSchemaRoot) = {
    val variableWidthVectors = root.getFieldVectors.asScala
      .filter(_.isInstanceOf[BaseVariableWidthVector])
      .map(_.asInstanceOf[BaseVariableWidthVector])
      .toArray
    val expander = new VectorExpander(variableWidthVectors)
    expander
  }
}

object NativeParquetReader {
  private lazy val initializer: Initializer = {
    System.loadLibrary("protobuf")
    System.loadLibrary("arrow")
    System.loadLibrary("parquet")
    System.loadLibrary("plasma")
    System.loadLibrary("fletcher_echo")
    System.loadLibrary("fletcher")
    System.loadLibrary("arrow-processor-native")
    new Initializer()
  }


  def apply(fileName: String, outputSchema: Schema, numRows: Int): NativeParquetReader = {

    val schemaAsBytes = ArrowTypeHelper.arrowSchemaToProtobuf(outputSchema).toByteArray
    val processId = initializer.init(fileName, schemaAsBytes, numRows)

    new NativeParquetReader(processId, outputSchema, numRows)
  }

  private class Initializer {
    @native def init(fileName: String, schemaAsBytes: Array[Byte], numRows: Int): Long
  }

  private class BufferDescriptor(root: VectorSchemaRoot) {
    lazy val recordBatch: ArrowRecordBatch = new VectorUnloader(root).getRecordBatch
    lazy val rowCount: Int = root.getRowCount
    lazy val addresses: Array[Long] = recordBatch.getBuffers.asScala.map(_.memoryAddress()).toArray
    lazy val sizes: Array[Long] = recordBatch.getBuffersLayout.asScala.map(_.getSize()).toArray
  }
}
