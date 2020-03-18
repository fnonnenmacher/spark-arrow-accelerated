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
import org.apache.arrow.vector.{BigIntVector, FieldVector, UInt1Vector, UInt8Vector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ArrowProcessorTest extends FunSuite {

  test("that a correct Arrow Root is created ") {

    val input = Seq(1L, 2L, 3L)
    val root = LongVectorSchemaRootBuilder.from("field-name", input)

    assert(root.getFieldVectors.size() === 1)
    assert(root.getRowCount === 3)
    assert(root.getVector(0).getField.getName == "field-name")

    assert(root.getVector(0).isInstanceOf[BigIntVector])
    assert(root.getVector(0).asInstanceOf[BigIntVector].get(0) === 1L)
    assert(root.getVector(0).asInstanceOf[BigIntVector].get(1) === 2L)
    assert(root.getVector(0).asInstanceOf[BigIntVector].get(2) === 3L)
  }

  test("calculate the some of an example vector") {

    val arrowProcessor = new ArrowProcessor()

    val root = LongVectorSchemaRootBuilder.from("some-values", Seq(1,2,3,4,5,6,7,8,9,10))

    val res = arrowProcessor.sum(root)

    assert(res === 55)
  }
}
