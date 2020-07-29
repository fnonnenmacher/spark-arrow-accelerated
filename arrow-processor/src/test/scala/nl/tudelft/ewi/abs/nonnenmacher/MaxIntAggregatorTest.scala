package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.utils.{ArrowVectorBuilder, IntegerVector}
import org.apache.arrow.vector.IntVector
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MaxIntAggregatorTest extends FunSuite {
  test("calculate the sum of an example vector") {

    val root = ArrowVectorBuilder.toSchemaRoot(IntegerVector("values", Seq(1, 2, 3, 4, 55, 6, 7, 8, 9, 10)))

    val aggregator = new MaxIntAggregator()

    val res = aggregator.aggregate(root)

    assert(res.head == 55)

    root.close()

    assert(GlobalAllocator.getAllocatedMemory() == 0)
  }
}
