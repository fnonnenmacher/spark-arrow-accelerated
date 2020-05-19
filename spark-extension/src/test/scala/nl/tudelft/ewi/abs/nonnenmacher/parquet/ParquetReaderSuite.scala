package nl.tudelft.ewi.abs.nonnenmacher.parquet

import nl.tudelft.ewi.abs.nonnenmacher.{GlobalAllocator, SparkSessionGenerator}
import nl.tudelft.ewi.abs.nonnenmacher.gandiva.ProjectionOnGandivaExtension
import org.apache.spark.sql.execution.datasources.NativeParquetReaderStrategy
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.{ArrowColumnarExtension, DataFrame, SparkSession, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite}


@RunWith(classOf[JUnitRunner])
class ParquetReaderSuite extends FunSuite with BeforeAndAfterEach with SparkSessionGenerator{

  override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(_.injectPlannerStrategy(x => NativeParquetReaderStrategy))

  ignore("creates example parquet file for tests") {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.parquet.writeLegacyFormat", value = true)
    spark.conf.set("spark.sql.parquet.compression.codec", value = "uncompressed")

    spark.range(1e6.toLong).rdd.map(x => (x.toInt, x * x, s"number-$x")).toDF("int-field", "long-field", "string-field")
      .write.parquet("test-example.parquet")
  }

  test("read from parquet format") {

    spark.conf.set("spark.sql.codegen.wholeStage", false)

    //TODO reading ``string-field doe not yet work!!!
    val sqlDF: DataFrame = spark.sql("SELECT `int-field` FROM parquet.`example.parquet` WHERE `long-field`>2 OR `long-field` < 0")

    sqlDF.printSchema()
    println("Direct Plan:")
    println(sqlDF.queryExecution)
    println("Logical Plan:")
    println(sqlDF.queryExecution.optimizedPlan)
    println("Spark Plan:")
    println(sqlDF.queryExecution.sparkPlan)
    println("Executed Plan:")
    println(sqlDF.queryExecution.executedPlan)

    println(sqlDF.columns.mkString(", "))
    sqlDF.foreach(println(_))
  }

  // Close and delete the temp file
  override def afterEach() {
    //Check that all previously allocated memory is released
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == 0)
    assert(GlobalAllocator.getAllocatedMemory == 0)
  }
}  