package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.utils.{ArrowVectorBuilder, IntegerVector}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FletcherReductionSuite extends FunSuite {

  test("check if calling works correctly and hard coded value is returned") {

    val root = ArrowVectorBuilder.toSchemaRoot(IntegerVector("some-values", Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))

    val proc = new FletcherReductionProcessor(root.getSchema)

    val reducedSum = proc.apply(root)

    assert(reducedSum == 13L)

    root.close()
    proc.close()

    assert(GlobalAllocator.getAllocatedMemory() == 0)
  }
}
