package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.utils.{ArrowVectorBuilder, LongVector}
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.Field.nullable
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class FletcherSumSuite extends FunSuite {

    test("calculate the sum of an example vector") {

      val root = ArrowVectorBuilder.toSchemaRoot(LongVector("some-values", Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))

      val proc = JNIProcessorFactory.fletcherEchoSumProcessor(root.getSchema, root.getSchema)

      val rootOut = proc.apply(root)

      val resVector = rootOut.getVector(0).asInstanceOf[BigIntVector]
      assert(resVector.get(0) == 55L)

      root.close()
      proc.close()

      assert(GlobalAllocator.getAllocatedMemory() == 0)
    }
}
