package nl.tudelft.ewi.abs.nonnenmacher

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.nio.channels.Channels.newChannel
import java.util.Arrays.asList

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.pojo.Field.nullable
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.{BigIntVector, FieldVector, IntVector, UInt1Vector, UInt8Vector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ArrowProcessorTest extends FunSuite {


  test("calculate the sum of an example vector") {

    val root = ArrowVectorBuilder.toSchemaRoot(LongVector("some-values", Seq(1,2,3,4,5,6,7,8,9,10)))

    val res = ArrowProcessor.sum(root)

    assert(res === 55)
  }

  test("add up three vectors") {

    val v1 = IntegerVector("in1", Seq(1, 2, 3))
    val v2 = IntegerVector("in2", Seq(10, 20, 30))
    val v3 = IntegerVector("in3", Seq(100, 200, 300))
    val root = ArrowVectorBuilder.toSchemaRoot(v1, v2, v3)

    val rootRes = ArrowProcessor.addThreeVectors(root)
    assert(rootRes.getFieldVectors.size() == 1)

    val resVec1 = rootRes.getVector(0).asInstanceOf[IntVector]
    assert(resVec1.getName == "out")
    assert(resVec1.get(0) == 111)
    assert(resVec1.get(1) == 222)
    assert(resVec1.get(2) == 333)
  }
}
