package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.{BigIntVector, UInt1Vector}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowRootByteConverterTest extends FunSuite {

  test("Convert vector schema root to bytes and back") {

    val root = ArrowVectorBuilder.toSchemaRoot(LongVector("vec1", Seq(1,2,3)), ByteVector("vec2", Seq(7,8,9)))

    val bytes = ArrowRootByteConverter.convert(root);

    val rootRes = ArrowRootByteConverter.convert(bytes);

    assert(rootRes.getFieldVectors.size() == 2)

    val resVec1 = rootRes.getVector(0).asInstanceOf[BigIntVector]
    assert(resVec1.getName == "vec1")
    assert(resVec1.get(0) == 1)
    assert(resVec1.get(1) == 2)
    assert(resVec1.get(2) == 3)

    val resVec2 = rootRes.getVector(1).asInstanceOf[UInt1Vector]
    assert(resVec2.getName == "vec2")
    assert(resVec2.get(0) == 7)
    assert(resVec2.get(1) == 8)
    assert(resVec2.get(2) == 9)
  }
}
