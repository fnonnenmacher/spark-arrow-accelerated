package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.fletcher.example.FletcherReductionExampleExtension
import nl.tudelft.ewi.abs.nonnenmacher.parquet.NativeParquetSourceScanExec
import org.apache.spark.sql.execution.datasources.NativeParquetReaderExtension
import org.apache.spark.sql.{FletcherReductionExampleExec, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class FletcherReductionExampleSuite extends FunSuite with SparkSessionGenerator {

  override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(NativeParquetReaderExtension(), FletcherReductionExampleExtension)

  ignore("convert network data to parquet file") {

    //write it as uncompressed file
    spark.conf.set("spark.sql.parquet.compression.codec", value = "uncompressed")

    // CSV file can be downloaded here: https://www.kaggle.com/jsrojas/ip-network-traffic-flows-labeled-with-87-apps
    // Adapt then the path to this file

    spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true")) //inferSchema is an expensive operation, but because it's just converted once we don't care
      .csv("/Users/fabian/Downloads/Dataset-Unicauca-Version2-87Atts.csv")
      .select("`Flow.ID`", "`Flow.Duration`")
      .repartition(1) //we want to have everything in 1 file
      //      .limit(100000) OPTIONAL
      .write.parquet("../data/network-traffic")
  }

  test("filter on regex and sum up int column") {

    val query =
      """ SELECT cast(`Flow.Duration` as bigint) as `Flow.Duration`
        | FROM parquet.`../data/network-traffic-100000.parquet`
        | WHERE `Flow.ID` rlike '.*443.*' """.stripMargin

    val sqlDF = spark.sql(query).agg("`Flow.Duration`" -> "sum")

    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[NativeParquetSourceScanExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[FletcherReductionExampleExec]).isDefined)

    // DEBUG
    // println("Executed Plan:")
    // println(sqlDF.queryExecution.executedPlan)

    println(sqlDF.first()(0))

    assertArrowMemoryIsFreed()
  }
}
