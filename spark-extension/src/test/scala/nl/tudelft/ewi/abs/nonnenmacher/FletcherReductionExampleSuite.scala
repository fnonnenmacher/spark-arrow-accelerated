package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.fletcher.example.FletcherReductionExampleExtension
import nl.tudelft.ewi.abs.nonnenmacher.parquet.NativeParquetSourceScanExec
import org.apache.spark.sql.execution.datasources.NativeParquetReaderExtension
import org.apache.spark.sql.{FletcherReductionExampleExec, SparkSession, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class FletcherReductionExampleSuite extends FunSuite with SparkSessionGenerator {

  override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(NativeParquetReaderExtension(), FletcherReductionExampleExtension)

  ignore("convert taxi csv files to parquet file") {

    val codec = "uncompressed"

    val spark = SparkSession
      .builder()
      .appName(this.styleName)
      .config("spark.master", "local")
      .getOrCreate()

    //write it as uncompressed file
    spark.conf.set("spark.sql.parquet.compression.codec", codec)

    spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true")) //inferSchema is an expensive operation, but because it's just converted once we don't care
      .csv("/path/to/data/*.csv")
      .repartition(1) //we want to have everything in 1 file
      .write.parquet(s"../data/taxi-$codec")

    spark.close()
  }

  test("filter on regex and sum up int column") {

    //set batch size
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 64000)

    val query =
      """ SELECT cast(`trip_seconds` as bigint) as `trip_seconds`
        | FROM parquet.`../data/taxi-uncompressed-10000.parquet`
        | WHERE `company` rlike '.*Taxi.*' """.stripMargin

    val sqlDF = spark.sql(query).agg("`trip_seconds`" -> "sum")

    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[NativeParquetSourceScanExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[FletcherReductionExampleExec]).isDefined)

    // DEBUG
    // println("Executed Plan:")
    // println(sqlDF.queryExecution.executedPlan)

    println(sqlDF.first()(0))

    assertArrowMemoryIsFreed()
  }
}
