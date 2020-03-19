package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FabiansSuite extends FunSuite {

//  test("sumOnJNI") {
//    val spark = SparkSession
//      .builder()
//      .withExtensions(_.injectPlannerStrategy(_ => NativeSumStrategy))
//      .appName("Spark SQL basic example")
//      .config("spark.master", "local")
//      .getOrCreate()
//
//    val df = spark.range(12).toDF("value")
//      .repartition(2);
//
//    val res = df.agg(sum(col("value")))
//
//    assert(res.first().get(0) == (1 until 12).sum)
//
//    res.explain(extended = true)
//  }

  test("whatever"){
    val a = NativeSumStrategy;
    assert( a != null)
  }
}
