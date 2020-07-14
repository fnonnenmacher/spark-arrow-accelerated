package nl.tudelft.ewi.abs.nonnenmacher.columnar

import nl.tudelft.ewi.abs.nonnenmacher.utils.IntegerVector
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VectorSchemaRootUtilSuite extends FunSuite {

  test("extract value vector from column vector") {

    val vec = IntegerVector("my-vec", Seq(1,2,3)).toArrowVector

    val res = VectorSchemaRootUtil.extractVector(new ArrowColumnVector(vec))

    assert(vec == res)

    vec.close()
  }
}