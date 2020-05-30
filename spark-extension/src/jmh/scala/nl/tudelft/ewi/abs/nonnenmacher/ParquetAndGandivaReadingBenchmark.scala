package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.ParquetAndGandivaReadingBenchmark.MyState
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.apache.spark.sql.SparkSession
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class ParquetAndGandivaReadingBenchmark {

  @Benchmark
  def dremio1(myState: MyState): Unit = {
    val max = myState.spark.sql(s"SELECT `x` + `N2x` + `N3x` AS sum FROM parquet.`$rootDir/data/5million-int-triples.parquet`")
      .agg("sum" -> "max").collect.head

    println("MAX:" + max)
  }

//  @Benchmark
  def dremio2(myState: MyState): Unit = {
    val res = myState.spark.sql("SELECT" +
      " `x` + `N2x` + `N3x` AS s1," +
      " `x` * `N2x` - `N3x` AS s2," +
      " 3 * `x` + 2* `N2x` + `N3x` AS s3," +
      " `x` >= `N2x` - `N3x` AS c1," +
      " `x` +  `N2x` = `N3x` AS c2 " +
      s"FROM parquet.`$rootDir/data/5million-int-triples.parquet`")
      .agg("s1" -> "sum",
        "s2" -> "sum",
        "s3" -> "sum",
        "c1" -> "count",
        "c2" -> "count",
      ).collect().head

    println("Results:" + res)
//    println(res.queryExecution.debug.codegen())
  }
}

object ParquetAndGandivaReadingBenchmark {

  @State(Scope.Thread)
  class MyState {

    var spark: SparkSession = _

    @Param(Array("PLAIN", "PARQUET_ONLY", "PARQUET_AND_GANDIVA"))
    var sparkSetup: String = _

    @Param(Array("16000", "32000", "64000", "128000"))
    var batchSize: Int = _

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
    def doTearDown(): Unit = {
      spark.close()
    }
  }

}
