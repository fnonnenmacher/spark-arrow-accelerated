package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.sql.SparkSession
import java.io.File

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DataGenerator extends FunSuite{

  val MILLION = 1000000

  ignore("generate5MillionIntTriples") {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.parquet.writeLegacyFormat", value = true)
    spark.conf.set("spark.sql.parquet.compression.codec", value = "uncompressed")

    val rand = new Random();
    def randomTriple(x: Any) = { (rand.nextInt(), rand.nextInt(), rand.nextInt()) }

    spark.range(5*MILLION).rdd.map(randomTriple).toDF("x", "N2x", "N3x")
      .write.parquet("../data/5million-int-triples.parquet")
  }
}
