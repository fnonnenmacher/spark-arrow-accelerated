package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.{BigIntVector, IntVector, VarCharVector}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowProcessorTest extends FunSuite {

  test("test reading of big parquet file to verify sending of multiple batches works correctly") {
    val iter = ArrowProcessor.readParquet("data/big-example.parquet");

    val allInts = iter.flatMap { root =>
      val intVector = root.getVector("int-field").asInstanceOf[IntVector]
      Range(0, root.getRowCount).map(intVector.get)
    }.toSeq

    assert(allInts.size == 1e6)
    assert(allInts.head == 0)
    assert(allInts(456) == 456)
    assert(allInts.last == 999999)
  }

  test("test reading of parquet file") {

    val iter = ArrowProcessor.readParquet("data/example.parquet");

    val rootRes = iter.next()

    assert(rootRes.getFieldVectors.size() == 3)

    val intVector = rootRes.getVector("int-field").asInstanceOf[IntVector]
    val intData = Range(0, rootRes.getRowCount).map(intVector.get)
    assert(intData == Seq(0, 1, 2, 3, 4))

    val longVector = rootRes.getVector("long-field").asInstanceOf[BigIntVector]
    val longData = Range(0, rootRes.getRowCount).map(longVector.get)
    assert(longData == Seq(0, 1, 4, 9, 16))

    val stringVector = rootRes.getVector("string-field").asInstanceOf[VarCharVector]
    val stringData = Range(0, rootRes.getRowCount).map(stringVector.getObject(_).toString)
    assert(stringData == Seq("number-0", "number-1", "number-2", "number-3", "number-4"))
  }

  ignore("calculate the sum of an example vector") {

    val root = ArrowVectorBuilder.toSchemaRoot(LongVector("some-values", Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))

    val res = ArrowProcessor.sum(root)

    assert(res === 55)
  }

  test("add up three vectors") {

    val v1 = IntegerVector("in1", Seq(1, 2, 3))
    val v2 = IntegerVector("in2", Seq(10, 20, 30))
    val v3 = IntegerVector("in3", Seq(100, 200, 300))
    val root = ArrowVectorBuilder.toSchemaRoot(v1, v2, v3)

    val itRes = ArrowProcessor.addThreeVectors(Iterator(root))

    val rootRes = itRes.next()
    assert(rootRes.getFieldVectors.size() == 1)

    val resVec1 = rootRes.getVector(0).asInstanceOf[IntVector]
    assert(resVec1.getName == "out")
    assert(resVec1.get(0) == 111)
    assert(resVec1.get(1) == 222)
    assert(resVec1.get(2) == 333)
  }
}
