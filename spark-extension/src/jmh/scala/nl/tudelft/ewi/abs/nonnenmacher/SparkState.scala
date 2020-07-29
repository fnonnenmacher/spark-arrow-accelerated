package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup.writeResults
import org.apache.spark.sql.SparkSession
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.BenchmarkParams

abstract class SparkState {

  var spark: SparkSession = _

  var sparkSetup: String
  var batchSize: Int

  @Setup(Level.Trial)
  def doSetup(): Unit = {
    spark = SparkSetup.initSpark(sparkSetup, batchSize)
  }

  @Setup(Level.Invocation)
  def clearCache(): Unit = {
    spark.catalog.clearCache()
    spark.sqlContext.clearCache()
  }

  @TearDown(Level.Trial)
  def doTearDown(params: BenchmarkParams): Unit = {
    writeResults(params)
    spark.close()
  }
}
