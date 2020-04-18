package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.{BigIntVector, IntVector, UInt1Vector}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowVectorBuilderTest extends FunSuite {

  test("that long vectors are created correctly"){
    val arrowVector = LongVector("long-field", Seq(1,2,3)).toArrowVector

    assert(arrowVector.getName == "long-field")
    assert(arrowVector.get(0) === 1L)
    assert(arrowVector.get(1) === 2L)
    assert(arrowVector.get(2) === 3L)
  }

  test("that int vectors are created correctly"){
    val arrowVector = IntegerVector("int-field", Seq(4,5,6)).toArrowVector

    assert(arrowVector.getName == "int-field")
    assert(arrowVector.get(0) === 4)
    assert(arrowVector.get(1) === 5)
    assert(arrowVector.get(2) === 6)
  }

  test("that byte vectors are created correctly"){
    val arrowVector = ByteVector("byte-field", Seq(7,8,9)).toArrowVector

    assert(arrowVector.getName == "byte-field")
    assert(arrowVector.get(0) === 7)
    assert(arrowVector.get(1) === 8)
    assert(arrowVector.get(2) === 9)
  }

  test("that multiple vectors are combined and added to schema root") {

    val longs = Seq(1L, 2L, 3L)
    val ints = Seq(4, 5, 6)
    val bytes: Seq[Byte] = Seq(7, 8, 9);
    val root = ArrowVectorBuilder.toSchemaRoot(LongVector("long-field", longs),
      IntegerVector("int-field", ints),
      ByteVector("byte-field", bytes))

    assert(root.getFieldVectors.size() === 3)
    assert(root.getRowCount === 3)

    val longVector = root.getVector(0).asInstanceOf[BigIntVector]
    assert(longVector.getName == "long-field")
    assert(longVector.get(0) === 1L)
    assert(longVector.get(1) === 2L)
    assert(longVector.get(2) === 3L)

    val intVector = root.getVector(1).asInstanceOf[IntVector]
    assert(intVector.getName == "int-field")
    assert(intVector.get(0) === 4)
    assert(intVector.get(1) === 5)
    assert(intVector.get(2) === 6)

    val byteVector = root.getVector(2).asInstanceOf[UInt1Vector]
    assert(byteVector.getName == "byte-field")
    assert(byteVector.get(0) === 7)
    assert(byteVector.get(1) === 8)
    assert(byteVector.get(2) === 9)
  }
}
