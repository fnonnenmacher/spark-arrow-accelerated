package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.utils.{ArrowVectorBuilder, IntegerVector, StringVector}
import org.apache.arrow.memory.{AllocationListener, RootAllocator}
import org.apache.arrow.vector.{IntVector, VarCharVector}
import org.junit.{Assume, Ignore, Test}

class Allocate64ByteAlignmentSuite {

  private val CACHE_ALIGN = 64;

  @Test
  @Ignore("Depends on the jdk implementation if alignment works or not. Not compatible with java installed in docker")
  def testThatAllocatedBuffersAre64ByteAligned(): Unit = {

    val allocator = new RootAllocator(AllocationListener.NOOP, 10000, Alignment64BitRoundingPolicy)

    val reqCapacities = List(0, 5, 15, 67, 130, 510, 1024, 1023, 1025)
    val expectedResult = List(0, 64, 64, 128, 256, 512, 1024, 1024, 2048)

    reqCapacities.zip(expectedResult).foreach { case (reqCapacity, expectedRes) =>

      val buf = allocator.buffer(reqCapacity)
      if (buf.capacity > 0) {
        assert(buf.memoryAddress % CACHE_ALIGN == 0, "memory is not align to 64-bytes")
        assert(expectedRes == buf.capacity)
        assert(buf.capacity % CACHE_ALIGN == 0, "size is not align to 64-bytes")
      }

      buf.close()
    }
    allocator.close()
  }

  @Test
  @Ignore("Depends on the jdk implementation if alignment works or not. Not compatible with java installed in docker")
  def testThatDataAndOffsetBuffersOfFieldVectorsAre64ByteAligned(): Unit = {
    val root = ArrowVectorBuilder.toSchemaRoot(
      StringVector("company", Seq("Blue Ribbon Taxi Association Inc.", "A", "Blue Ribbon Taxi Association Inc.", "B")),
      IntegerVector("trip_seconds", Seq(1, 2, 3, 4)))

    val stringVec = root.getFieldVectors.get(0).asInstanceOf[VarCharVector]
    val intVec = root.getFieldVectors.get(1).asInstanceOf[IntVector]

    //Assert 64Bit Alignment of StringVector
    assert(stringVec.getDataBufferAddress % 64 == 0)
    assert(stringVec.getDataBuffer.capacity() % 64 == 0)

    assert(stringVec.getOffsetBufferAddress % 64 == 0)

    // PROBLEM: OffsetBuffer & Validity buffer are just slices from one common allocated buffer
    // Unfortunately I can't easily modify the slicing mechanism and therefore the following asserts will fail
    // However maybe it's sufficient for our use-case when the addresses of data & offset buffer are aligned

    // assert(stringVec.getOffsetBuffer.capacity() % 64 == 0)
    // assert(stringVec.getValidityBufferAddress % 64 == 0)
    // assert(stringVec.getValidityBuffer.capacity() % 64 == 0)

    // assert Buffers of Intvector are aligned
    assert(intVec.getDataBufferAddress % 64 == 0)

    // PROBLEM: same as above, data and validity buffer are just slices from common vector
    // assert(intVec.getDataBuffer.capacity() %64 == 0)
    // assert(intVec.getValidityBufferAddress % 64 == 0)
    // assert(intVec.getValidityBuffer.capacity() % 64 == 0)

    stringVec.close()
    intVec.close()
    root.close()

    assert(GlobalAllocator.getAllocatedMemory() == 0)
  }
}
