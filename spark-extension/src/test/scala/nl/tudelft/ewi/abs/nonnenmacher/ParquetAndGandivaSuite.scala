package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.gandiva.{GandivaFilterExec, GandivaProjectExec, ProjectionOnGandivaExtension}
import nl.tudelft.ewi.abs.nonnenmacher.parquet.NativeParquetSourceScanExec
import org.apache.spark.sql.execution.datasources.NativeParquetReaderStrategy
import org.apache.spark.sql.{ArrowColumnarExtension, DataFrame, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParquetAndGandivaSuite extends FunSuite with SparkSessionGenerator {

  override def withExtensions: Seq[SparkSessionExtensions => Unit] =
    Seq(_.injectPlannerStrategy(x => NativeParquetReaderStrategy(true)),
      ProjectionOnGandivaExtension(),
      ArrowColumnarExtension())

  test("read from parquet format") {

    spark.conf.set("spark.sql.codegen.wholeStage", false)

    val sqlDF: DataFrame = spark.sql("SELECT `string-field`, `int-field` *2 FROM parquet.`example.parquet` WHERE `long-field`>2 OR `long-field` < 0")

    sqlDF.printSchema()
    println("Direct Plan:")
    println(sqlDF.queryExecution)
    println("Logical Plan:")
    println(sqlDF.queryExecution.optimizedPlan)
    println("Spark Plan:")
    println(sqlDF.queryExecution.sparkPlan)
    println("Executed Plan:")
    println(sqlDF.queryExecution.executedPlan)

    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[NativeParquetSourceScanExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[GandivaFilterExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[GandivaProjectExec]).isDefined)

    val res = sqlDF.collect().map(r => (r(0), r(1))).toList

    assert(res.size==3)
    assert(res.contains(("number-2", 4)))
    assert(res.contains(("number-3", 6)))
    assert(res.contains(("number-4", 8)))
  }
}