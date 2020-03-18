package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

object GlobalAllocator {

  private val allocator = new RootAllocator(Long.MaxValue);

  def newChildAllocator(name: String, initReservation: Long = 0): BufferAllocator = {
    allocator.newChildAllocator(name, initReservation, Long.MaxValue)
  }

  def newChildAllocator(clazz: Class[_], initReservation: Long): BufferAllocator =
    newChildAllocator(clazz.getName, initReservation)

  def newChildAllocator(clazz: Class[_]): BufferAllocator =
    newChildAllocator(clazz.getName)
}
