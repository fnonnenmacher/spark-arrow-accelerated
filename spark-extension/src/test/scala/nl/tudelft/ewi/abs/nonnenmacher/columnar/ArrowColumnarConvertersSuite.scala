package nl.tudelft.ewi.abs.nonnenmacher.columnar

import nl.tudelft.ewi.abs.nonnenmacher.columnar.ArrowColumnarConverters._
import nl.tudelft.ewi.abs.nonnenmacher.utils.ArrowVectorBuilder.toSchemaRoot
import nl.tudelft.ewi.abs.nonnenmacher.utils.IntegerVector
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowColumnarConvertersSuite extends FunSuite {

  test("extract value vector from column vector") {

    val root1 = toSchemaRoot(IntegerVector("my-vec", Seq(1, 2, 3)))

    val batch = root1.toBatch
    val root2 = batch.toArrow

    assert(root1.getFieldVectors.get(0) == root2.getFieldVectors.get(0))

    root1.close()
    root2.close()
  }
}