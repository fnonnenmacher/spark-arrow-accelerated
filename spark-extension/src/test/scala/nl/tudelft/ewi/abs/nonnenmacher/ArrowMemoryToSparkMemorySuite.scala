package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.parquet.NativeParquetSourceScanExec
import org.apache.spark.sql.execution.datasources.NativeParquetReaderExtension
import org.apache.spark.sql.{MeasureColumnarProcessingExec, MeasureColumnarProcessingExtension, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowMemoryToSparkMemorySuite extends FunSuite with SparkSessionGenerator {

  override def withExtensions: Seq[SparkSessionExtensions => Unit] =
    Seq(NativeParquetReaderExtension(true), MeasureColumnarProcessingExtension)

  test("read three fields") {

    val sqlDF = spark.sql(s"SELECT `x`, `N2x`, `N3x` FROM parquet.`../data/5-million-int-triples-snappy.parquet`")
      .agg("x" -> "max",
        "N2x" -> "max",
        "N3x" -> "max")


    println("Executed Plan:")
    println(sqlDF.queryExecution.executedPlan)

    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[NativeParquetSourceScanExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[MeasureColumnarProcessingExec]).isDefined)

    println(sqlDF.first())

    assertArrowMemoryIsFreed()
  }
}