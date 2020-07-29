package nl.tudelft.ewi.abs.nonnenmacher

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.{BaseAllocator, BufferAllocator}

import scala.collection.mutable

class JavaMemoryPoolServer(val allocator: BufferAllocator) {

  private val minAllocationSize= 4096;
  private val buffers: mutable.Map[Long, ArrowBuf] = mutable.Map()
  private val emptyBuffer = allocator.getEmpty

  def getBufferByAddress(address: Long): Option[ArrowBuf] = buffers.get(address)

  def allocate(size: Long): Long = {
    if (size == 0) {
      return allocator.getEmpty.memoryAddress();
    }

    val buffer = allocator.buffer(Math.max(BaseAllocator.nextPowerOfTwo(size), minAllocationSize))
    buffers.put(buffer.memoryAddress, buffer)
    buffer.memoryAddress()
  }

  def reallocate(oldAddr: Long, newSize: Long): Long = {

    // Reallocating of empty buffer
    if (oldAddr.equals(emptyBuffer.memoryAddress())){
      return allocate(newSize)
    }

    val oldBuffer = buffers.remove(oldAddr).get
    val oldCapacity = oldBuffer.capacity()

    val newBuffer = if (oldCapacity < newSize) {
//      println(s"Reallocate called. From $oldCapacity to $newSize")

      val newCapacity = BaseAllocator.nextPowerOfTwo(Math.max(newSize, oldCapacity * 2))

      val newBuf = allocator.buffer(newCapacity)
      newBuf.setBytes(0, oldBuffer, 0, oldBuffer.capacity())
      oldBuffer.close()
      newBuf
    } else {
      oldBuffer
    }

    buffers.put(newBuffer.memoryAddress, newBuffer)
    newBuffer.memoryAddress()
  }

  def free(bufAddr: Long): Unit = {
    val buffer = buffers.remove(bufAddr)
    if (buffer.isDefined) buffer.get.close()
  }

  def bytesAllocated(): Long = {
    allocator.getAllocatedMemory
  }

  def maxMemory(): Long = {
    allocator.getPeakMemoryAllocation
  }

  def close(): Unit = {
    buffers.values.foreach(_.close())
  }
}
