package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BaseFixedWidthVector, BigIntVector, FieldVector, UInt1Vector, VectorSchemaRoot}

import scala.collection.JavaConverters._

abstract class VectorSchemaRootBuilder[T, U <: BaseFixedWidthVector] {

  protected val allocator: BufferAllocator = GlobalAllocator.newChildAllocator(classOf[VectorSchemaRootBuilder[T, U]])


  def from(name: String, values: Seq[T]): VectorSchemaRoot = {
    val vector: U = initVector(name);

    // Fill apache arrow data structure
    vector.allocateNew(values.size)
    values.zipWithIndex.foreach(value => setValue(vector, value._2, value._1))
    vector.setValueCount(values.size)

    // Create Schema Root
    new VectorSchemaRoot(Iterable(vector.asInstanceOf[FieldVector]).asJava)
  }

  protected def initVector(name: String): U

  protected def setValue(vector: U, index: Int, value: T): Unit

}

object ByteVectorSchemaRootBuilder extends VectorSchemaRootBuilder[Byte, UInt1Vector] {
  override protected def initVector(name: String): UInt1Vector = new UInt1Vector(name, allocator);

  override protected def setValue(vector: UInt1Vector, index: Int, value: Byte): Unit = vector.set(index, value)
}

object LongVectorSchemaRootBuilder extends VectorSchemaRootBuilder[Long, BigIntVector] {
  override protected def initVector(name: String): BigIntVector = new BigIntVector(name, allocator);

  override protected def setValue(vector: BigIntVector, index: Int, value: Long): Unit = vector.set(index, value)
}

