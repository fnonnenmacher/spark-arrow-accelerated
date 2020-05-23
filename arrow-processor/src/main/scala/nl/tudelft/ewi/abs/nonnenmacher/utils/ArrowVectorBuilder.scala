package nl.tudelft.ewi.abs.nonnenmacher.utils

import nl.tudelft.ewi.abs.nonnenmacher.GlobalAllocator
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.util.Text

import scala.collection.JavaConverters._

/**
 * Simple Facade to build Arrow vectors more easily
 *
 */
object ArrowVectorBuilder {

  val allocator: BufferAllocator = GlobalAllocator.newChildAllocator(ArrowVectorBuilder.getClass)

  def toSchemaRoot(vectors: Vector[_, _ <: FieldVector]*): VectorSchemaRoot = {
    val arrowVectors: Seq[FieldVector] = vectors.map(_.toArrowVector)
    new VectorSchemaRoot(arrowVectors.asJava)
  }
}

abstract class Vector[T, S <: FieldVector](val name: String, val values: Seq[T]) {

  protected def initVector(name: String): S

  protected def setValue(vector: S, index: Int, value: T): Unit

  def toArrowVector: S = {
    val vector: S = initVector(name);

    vector match {
      case fixedWidth: BaseFixedWidthVector => fixedWidth.allocateNew(values.size)
    }

    // Fill apache arrow data structure
    values.zipWithIndex.foreach(value => setValue(vector, value._2, value._1))
    vector.setValueCount(values.size)
    vector
  }
}

case class ByteVector(override val name: String, override val values: Seq[Byte]) extends Vector[Byte, UInt1Vector](name, values) {
  override protected def initVector(name: String): UInt1Vector = new UInt1Vector(name, ArrowVectorBuilder.allocator);

  override protected def setValue(vector: UInt1Vector, index: Int, value: Byte): Unit = vector.set(index, value)
}

case class LongVector(override val name: String, override val values: Seq[Long]) extends Vector[Long, BigIntVector](name, values) {
  override protected def initVector(name: String): BigIntVector = new BigIntVector(name, ArrowVectorBuilder.allocator);

  override protected def setValue(vector: BigIntVector, index: Int, value: Long): Unit = vector.set(index, value)
}

case class IntegerVector(override val name: String, override val values: Seq[Int]) extends Vector[Int, IntVector](name, values) {
  override protected def initVector(name: String): IntVector = new IntVector(name, ArrowVectorBuilder.allocator);

  override protected def setValue(vector: IntVector, index: Int, value: Int): Unit = vector.set(index, value)
}

case class StringVector(override val name: String, override val values: Seq[String]) extends Vector[String, VarCharVector](name, values) {
  override protected def initVector(name: String): VarCharVector = new VarCharVector(name, ArrowVectorBuilder.allocator);

  override protected def setValue(vector: VarCharVector, index: Int, value: String): Unit = vector.setSafe(index, new Text(value))
}