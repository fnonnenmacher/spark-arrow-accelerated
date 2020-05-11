package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import nl.tudelft.ewi.abs.nonnenmacher.ArrowFieldDefinitionHelper.nullableInt
import org.apache.spark.sql.execution.datasources.MyFileSourceStrategy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ArrowColumnarConversionRule, DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class GandivaProjectionSuite extends FunSuite {

  test("example addition of three values is executed on cpp code") {

    val spark = SparkSession
      .builder()
      .withExtensions(ProjectionOnGandivaExtension())
      .withExtensions(_.injectColumnar(_ => ArrowColumnarConversionRule))
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    // Deactivates whole stage codegen, helpful for debugging
    // spark.conf.set("spark.sql.codegen.wholeStage", false)

    val tuples = List((1, 1, 1, 1),
      (2, 2, 2, 2),
      (3, 3, 3, 3),
      (4, 4, 4, 4),
      (5, 5, 5, 5),
      (6, 6, 6, 6))

    import spark.implicits._

    val df = spark.createDataset(tuples)
      .toDF("a", "b", "c", "d")
      .repartition(2) // Enforces a separate Projection step
    // otherwise Spark optimizes the projection and combines it with the data generation

    val res = df.select((col("a") + col("b") + col("c") * col("d")) * 4)

    println("Logical Plan:")
    println(res.queryExecution.optimizedPlan)
    println("Spark Plan:")
    println(res.queryExecution.sparkPlan)
    println("Executed Plan:")
    println(res.queryExecution.executedPlan)

    // Shows generated code, helpful for debugging
    // println(res.queryExecution.debug.codegen())

    //Verify the expected results are correct
    val results: Array[Int] = res.collect().map(_.getInt(0));
    assert(results.length == 6)
    println("Result: " + results.toSet)
    assert(Set(12, 32, 60, 96, 140, 192).subsetOf(results.toSet))
  }

  test("biggernumbers"){

    val spark = SparkSession
      .builder()
      .withExtensions(ProjectionOnGandivaExtension())
      .withExtensions(_.injectColumnar(_ => ArrowColumnarConversionRule))
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val df = spark.range(1e6.toLong).rdd.map( x => (x, (1e6-x).toLong, x*2))
      .toDF("a", "b", "c")
      .select(col("a")*col("b"), col("b")+col("c"), col("c")*2)

    df.take(10).foreach(println(_))
    println(df.count())
  }
}
