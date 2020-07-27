package nl.tudelft.ewi.abs.nonnenmacher


import io.netty.buffer.ArrowBuf
import org.apache.arrow.gandiva.evaluator.NativeLibraryLoader
import org.apache.arrow.gandiva.expression.ArrowTypeHelper
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.Schema

import scala.collection.JavaConverters._
import scala.collection.mutable

class NativeParquetReader(val fileName: String, val inputSchema: Schema, val outputSchema: Schema, val batchSize: Int) extends Iterator[VectorSchemaRoot] {

  private val allocator = GlobalAllocator.newChildAllocator(this.getClass)
  private val memoryPool = new JavaMemoryPoolServer(allocator)

  private val ptr: Long = {
    NativeLibraryLoader.load()
    val inputSchemaBytes = ArrowTypeHelper.arrowSchemaToProtobuf(inputSchema).toByteArray
    val outputSchemaBytes = ArrowTypeHelper.arrowSchemaToProtobuf(outputSchema).toByteArray
    initNativeParquetReader(memoryPool, fileName, inputSchemaBytes, outputSchemaBytes, batchSize)
  }

  private val fieldCount = outputSchema.getFields.size();

  private var isFinished = false
  private var preLoadedBatch: Option[VectorSchemaRoot] = Option.empty
  private var currentBatch: Option[VectorSchemaRoot] = Option.empty

  override def hasNext: Boolean = {
    if (isFinished) return false
    if (preLoadedBatch.isDefined) return true //next batch already loaded

    currentBatch.foreach(_.close()) //release current batch
    preLoadedBatch = readNextBatchIfAvailable()

    if (preLoadedBatch.isDefined) true
    else {
      isFinished = true
      close()
      false
    }
  }

  override def next(): VectorSchemaRoot = {
    hasNext //loads next batch in case it is not yet loaded
    val res = preLoadedBatch.getOrElse(throw new IllegalAccessException("The Iterator is already closed"))
    currentBatch = preLoadedBatch
    preLoadedBatch = Option.empty
    res
  }

  def close(): Unit = {
    currentBatch.foreach(_.close())
    close(ptr)
    allocator.close()
  }

  /**
   * The arrow::Iterator implementation in C++ does not supports the hasNext functionality, instead the method "Next()"
   * returns an end marker, when all entries are consumed. To make it compatible with the Java/Scala Iterator we wrap this
   * functionality here. [[NativeParquetReader.hasNext]] reads the next batch and stores it internally.
   * [[NativeParquetReader.next()]] is then providing the preloaded batch
   *
   */
  def readNextBatchIfAvailable(): Option[VectorSchemaRoot] = {
    val resultLengths: Array[Long] = Array.ofDim(fieldCount)
    val resultNullCounts: Array[Long] = Array.ofDim(fieldCount)
    val bufferAddresses: Array[Long] = Array.ofDim(fieldCount * 3)

    val res = readNext(ptr, resultLengths, resultNullCounts, bufferAddresses)

    if (!res) {
      //All entries read
      return Option.empty
    }

    val vectors: mutable.Buffer[FieldVector] = outputSchema.getFields.asScala.zipWithIndex.map { case (field, i) =>
      val validityBuffer: ArrowBuf = memoryPool.getBufferByAddress(bufferAddresses(i * 3)).orNull
      val valueBuffer: ArrowBuf = memoryPool.getBufferByAddress(bufferAddresses(i * 3 + 1)).getOrElse(throw new IllegalArgumentException())
      val offsetBuffer: ArrowBuf = memoryPool.getBufferByAddress(bufferAddresses(i * 3 + 2)).orNull

      val fieldNode = new ArrowFieldNode(resultLengths(i), resultNullCounts(i))

      val vector = field.createVector(allocator)

      vector match {
        case b: BaseFixedWidthVector => vector.loadFieldBuffers(fieldNode, List(validityBuffer, valueBuffer).asJava)
        case b: BaseVariableWidthVector => vector.loadFieldBuffers(fieldNode, List(validityBuffer, valueBuffer, offsetBuffer).asJava)
        case _ => throw new IllegalArgumentException(s"${field.getFieldType} not supported.")
      }

      vector
    }
    Option(new VectorSchemaRoot(vectors.asJava))
  }

  @native def initNativeParquetReader(jMemoryPool: JavaMemoryPoolServer, fileName: String, inputSchemaBytes: Array[Byte], outputSchemaBytes: Array[Byte], numRows: Int): Long

  @native private def readNext(ptr: Long, lengths: Array[Long], nullCounts: Array[Long], bufAddrs: Array[Long]): Boolean;

  @native private def close(ptr: Long): Unit;

}
