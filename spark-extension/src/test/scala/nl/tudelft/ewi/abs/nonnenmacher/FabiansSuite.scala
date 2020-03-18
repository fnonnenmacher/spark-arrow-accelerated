package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.scalatest.{FunSuite, Ignore}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FabiansSuite extends FunSuite {

  test("sumOnJNI") {
    val spark = SparkSession
      .builder()
      .withExtensions(_.injectPlannerStrategy(_ => NativeSumStrategy))
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val df = spark.range(12).toDF("value")
      .repartition(2);

    val res = df.agg(sum(col("value")) )

    assert(res.first().get(0) == (1 until 12).sum)

    res.explain(extended = true)
  }
}
