package nl.tudelft.ewi.abs.nonnenmacher

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel}

import nl.tudelft.ewi.abs.nonnenmacher.GlobalAllocator.newChildAllocator
import nl.tudelft.ewi.abs.nonnenmacher.PlasmaFacade.ObjectId
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.ipc.{ArrowStreamReader, WriteChannel}
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}


object ArrowRootByteConverter {

  private val allocator = newChildAllocator(ArrowRootByteConverter.getClass)

  def convert(data: Array[Byte]): VectorSchemaRoot = {

    val inputStream = new ByteArrayInputStream(data)
    val streamReader = new ArrowStreamReader(inputStream, allocator)
    streamReader.loadNextBatch();
    streamReader.getVectorSchemaRoot
  }

  def convert(root: VectorSchemaRoot): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val writeChannel = new WriteChannel(Channels.newChannel(out))
    val recordBatch = new VectorUnloader(root).getRecordBatch
    MessageSerializer.serialize(writeChannel, root.getSchema); //TODO: Really necessary to transfer. Maybe schema is clear
    MessageSerializer.serialize(writeChannel, recordBatch);
    recordBatch.close()
    out.toByteArray
  }
}

class ArrayIteratorChannel(val iterator: Iterator[Array[Byte]]) extends ReadableByteChannel {

  private var currentChunk: Array[Byte] = iterator.next()
  private var indexCounter=0
  private var _isOpen = true;

  private def loadNextChunk():Unit = {
    if (iterator.hasNext) {
      currentChunk = iterator.next();
      indexCounter = 0;
    }else {
      _isOpen = false;
    }
  }

  override def read(dst: ByteBuffer): Int = {

    val bytes2Copy = Math.min(dst.remaining() ,currentChunk.length)
    dst.put(currentChunk, indexCounter, bytes2Copy)
    indexCounter += bytes2Copy;

    if (indexCounter>=currentChunk.length){ //current chunk processed, load next chunk
      loadNextChunk();
    }

    bytes2Copy
  }

  override def isOpen: Boolean = _isOpen

  override def close(): Unit = {}
}

class ArrowDeserializer(val iterator: Iterator[ObjectId]) extends Iterator[VectorSchemaRoot] {

  private val allocator = newChildAllocator(ArrowRootByteConverter.getClass)

  private lazy val arrayIteratorChannel = {
    val dataIter = iterator.map(PlasmaFacade.get)
    new ArrayIteratorChannel(dataIter)
  }
  private lazy val streamReader = {
    new ArrowStreamReader(arrayIteratorChannel, allocator)
  }

  override def hasNext: Boolean = arrayIteratorChannel.isOpen

  override def next(): VectorSchemaRoot = {
    val r = streamReader.loadNextBatch()
    streamReader.getVectorSchemaRoot
  }
}