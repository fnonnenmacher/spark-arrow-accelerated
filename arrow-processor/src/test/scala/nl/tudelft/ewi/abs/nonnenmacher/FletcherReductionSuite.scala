package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.utils.{ArrowVectorBuilder, IntegerVector, StringVector}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FletcherReductionSuite extends FunSuite {

  test("check if calling works correctly and hard coded value is returned") {

    val root = ArrowVectorBuilder.toSchemaRoot(
      StringVector("company", Seq("Blue Ribbon Taxi Association Inc.", "A", "Blue Ribbon Taxi Association Inc.", "B")),
      IntegerVector("trip_seconds", Seq(1, 2, 3, 4)))

    val fletcherReductionProcessor = new FletcherReductionProcessor(root.getSchema)

    val reducedSum = fletcherReductionProcessor.apply(root)

    assert(reducedSum == 13L)

    println("RECEIVED RESULT:" + reducedSum)

    root.close()
    fletcherReductionProcessor.close()

    assert(GlobalAllocator.getAllocatedMemory() == 0)
  }
}
