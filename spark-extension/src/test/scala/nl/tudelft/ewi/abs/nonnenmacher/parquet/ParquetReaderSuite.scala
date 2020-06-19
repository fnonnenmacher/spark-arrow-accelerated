package nl.tudelft.ewi.abs.nonnenmacher.parquet

import nl.tudelft.ewi.abs.nonnenmacher.SparkSessionGenerator
import org.apache.spark.sql.execution.datasources.NativeParquetReaderExtension
import org.apache.spark.sql.{DataFrame, SparkSession, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite}


@RunWith(classOf[JUnitRunner])
class ParquetReaderSuite extends FunSuite with BeforeAndAfterEach with SparkSessionGenerator {

  override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(NativeParquetReaderExtension())

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

    assertArrowMemoryIsFreed()
  }

  test("read from parquet format") {

    val sqlDF: DataFrame = spark.sql("SELECT `string-field` FROM parquet.`../data/big-example.parquet` WHERE `long-field`>2 OR `long-field` < 0")

    sqlDF.printSchema()
    println("Direct Plan:")
    println(sqlDF.queryExecution)
    println("Logical Plan:")
    println(sqlDF.queryExecution.optimizedPlan)
    println("Spark Plan:")
    println(sqlDF.queryExecution.sparkPlan)
    println("Executed Plan:")
    println(sqlDF.queryExecution.executedPlan)

    //    println(sqlDF.columns.mkString(", "))
    println(sqlDF.count())

    assertArrowMemoryIsFreed()
  }

  ignore("benchmark") {
    val max = spark.sql(s"SELECT * " +
      s"FROM parquet.`../data/500million-int-triples-snappy.parquet`")
      .count()
    println(max)

    assertArrowMemoryIsFreed()
  }
}  