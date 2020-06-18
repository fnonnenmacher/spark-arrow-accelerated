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

    //    spark.conf.set("spark.sql.parquet.writeLegacyFormat", value = true)
    spark.conf.set("spark.sql.parquet.compression.codec", value = "snappy")

    val rand = new Random();
    def randomTriple(x: Any) = {
      (rand.nextInt(), rand.nextInt(), rand.nextInt())
    }

    spark.range(5 * MILLION).rdd.map(randomTriple).toDF("x", "N2x", "N3x")
      .write.parquet("../data/5-million-int-triples-snappy")
  }

  ignore("generateAMillionTimesTenInts") {
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

    spark.range(MILLION).rdd.map(tenRandomInts).toDF("x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x10")
      .write.parquet("../data/million-times-10-ints.parquet")
  }
}
