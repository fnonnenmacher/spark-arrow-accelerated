package nl.tudelft.ewi.abs.nonnenmacher.partial.projection

import nl.tudelft.ewi.abs.nonnenmacher.SparkSessionGenerator
import nl.tudelft.ewi.abs.nonnenmacher.columnar.ArrowColumnarExtension
import nl.tudelft.ewi.abs.nonnenmacher.partial.projection.ArrowFieldDefinitionHelper.nullableInt
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class PartialProjectionOnFpgaSuite extends FunSuite with SparkSessionGenerator {

  override def withExtensions: Seq[SparkSessionExtensions => Unit] = {
    //Define FPGAModules
    val in1: In = In(nullableInt("in1"))
    val in2: In = In(nullableInt("in2"))
    val in3: In = In(nullableInt("in3"))

    val sumOfThree = FPGAModule("sumOfThree", query = in1 + in2 + in3, output = nullableInt("out"))

    Seq(ProjectionOnFPGAExtension(sumOfThree), ArrowColumnarExtension)
  }

  test("example addition of three values is executed on cpp code") {

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
      .repartition(3) // Enforces a separate Projection step
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

    assertArrowMemoryIsFreed()
  }
}
