package nl.tudelft.ewi.abs.nonnenmacher

import io.netty.buffer.PooledByteBufAllocator
import org.apache.arrow.memory.rounding.{DefaultRoundingPolicy, RoundingPolicy}
import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}

object GlobalAllocator {
  def getAllocatedMemory(): Long = allocator.getAllocatedMemory

  def close(): Unit = allocator.close()

  private val allocator = new RootAllocator(AllocationListener.NOOP, Long.MaxValue, Alignment64BitRoundingPolicy)

  def newChildAllocator(name: String, initReservation: Long = 0): BufferAllocator = {
    allocator.newChildAllocator(name, initReservation, Long.MaxValue)
  }

  def newChildAllocator(clazz: Class[_], initReservation: Long): BufferAllocator =
    newChildAllocator(clazz.getName, initReservation)

  def newChildAllocator(clazz: Class[_]): BufferAllocator =
    newChildAllocator(clazz.getName)
}

object Alignment64BitRoundingPolicy extends RoundingPolicy {

  private val memoryCacheAlignment = {
    val field = classOf[PooledByteBufAllocator].getDeclaredField("DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT")
    field.setAccessible(true)
    field.get(null).asInstanceOf[Int]
  }

  override def getRoundedSize(requestSize: Long): Long = if (requestSize == 0)
    0
  else
    DefaultRoundingPolicy.INSTANCE.getRoundedSize(roundUpToMultipleOfChunkSize(requestSize))

  private def roundUpToMultipleOfChunkSize(size: Long): Long = {
    if (size % memoryCacheAlignment == 0) {
      return size //size is already multiple of memoryCacheAlignment, no rounding needed
    }
    ((size / memoryCacheAlignment) + 1) * memoryCacheAlignment
  }
}