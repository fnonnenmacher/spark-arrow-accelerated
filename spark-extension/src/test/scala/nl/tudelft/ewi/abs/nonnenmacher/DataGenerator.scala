package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DataGenerator extends FunSuite {

  val MILLION = 1000000

  ignore("generate500MillionIntTriples") {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.parquet.compression.codec", value = "uncompressed")

    val rand = new Random();

    spark.range(500 * MILLION).rdd.map(_ => (rand.nextInt(), rand.nextInt(), rand.nextInt()))
      .toDF("x1", "x2", "x3")
      .write.parquet("../data/500-million-int-triples-uncompressed")
  }

  ignore("abc"){
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val res = spark.sql("SELECT MAX(`value`) FROM parquet.`../data/500-million-ints-uncompressed.parquet`")

    println("Executed Plan:")
    println(res.queryExecution.executedPlan)

    println(res.explain())
    res.queryExecution.debug.codegen()
  }

  test("generate50MillionTimesTenInts") {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.parquet.compression.codec", value = "uncompressed")

    val rand = new Random();
    def tenRandomInts(x: Any) = {
      (rand.nextInt(), rand.nextInt(), rand.nextInt(), rand.nextInt(), rand.nextInt(), rand.nextInt(), rand.nextInt(), rand.nextInt(), rand.nextInt(), rand.nextInt())
    }

    spark.range(50*MILLION).rdd.map(tenRandomInts).toDF("x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x10")
      .write.parquet("../data/50million-times-10-ints.parquet")
  }
}
