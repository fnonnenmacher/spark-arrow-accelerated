package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.utils.{ArrowVectorBuilder, IntegerVector}
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.Field.nullable
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ThreeIntAddingSuite extends FunSuite {

  test("add up three vectors") {

    val v1 = IntegerVector("in1", Seq(1, 2, 3))
    val v2 = IntegerVector("in2", Seq(10, 20, 30))
    val v3 = IntegerVector("in3", Seq(100, 200, 300))
    val rootIn = ArrowVectorBuilder.toSchemaRoot(v1, v2, v3)

    val schemaOut = new Schema(Seq(nullable("out", MinorType.INT.getType)).asJava)

    val proc = JNIProcessorFactory.threeIntAddingProcessor(rootIn.getSchema, schemaOut)

    val rootRes = proc.apply(rootIn)

    val resVec1 = rootRes.getVector(0).asInstanceOf[IntVector]
    assert(resVec1.getName == "out")
    assert(resVec1.get(0) == 111)
    assert(resVec1.get(1) == 222)
    assert(resVec1.get(2) == 333)

    rootIn.close()
    proc.close()

    assert(GlobalAllocator.getAllocatedMemory() == 0)
  }
}
