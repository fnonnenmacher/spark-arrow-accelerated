package nl.tudelft.ewi.abs.nonnenmacher.max.aggrgation

import nl.tudelft.ewi.abs.nonnenmacher.SparkSessionGenerator
import nl.tudelft.ewi.abs.nonnenmacher.columnar.ArrowColumnarExtension
import nl.tudelft.ewi.abs.nonnenmacher.max.aggregation.SimpleMaxAggregationExtension
import nl.tudelft.ewi.abs.nonnenmacher.parquet.ArrowParquetReaderExtension
import org.apache.spark.sql.{DataFrame, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleMaxAggregationSuite extends FunSuite with SparkSessionGenerator {

  override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(ArrowParquetReaderExtension, ArrowColumnarExtension, SimpleMaxAggregationExtension)

  test("read from parquet format") {

    val sqlDF: DataFrame = spark.sql("SELECT MAX(`x`), MAX(`N2x`), MAX(`Nx3`) FROM parquet.`../data/500-million-int-triples-uncompressed.parquet`")

    sqlDF.printSchema()
    println("Direct Plan:")
    println(sqlDF.queryExecution)
    println("Logical Plan:")
    println(sqlDF.queryExecution.optimizedPlan)
    println("Spark Plan:")
    println(sqlDF.queryExecution.sparkPlan)
    println("Executed Plan:")
    println(sqlDF.queryExecution.executedPlan)

    assert(sqlDF.first()(0) == 999999)

    assertArrowMemoryIsFreed()
  }
}
