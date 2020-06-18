package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.ParquetAndGandivaReadingBenchmark.MyState
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.apache.spark.sql.SparkSession
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.{BenchmarkParams, Blackhole}

@BenchmarkMode(Array(Mode.SingleShotTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 50, time = 1)
@Measurement(iterations = 50, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class ParquetAndGandivaReadingBenchmark {

//    @Benchmark
  def dremio1(blackhole: Blackhole, myState: MyState): Unit = {
    val max = myState.spark.sql(s"SELECT `x` + `N2x` + `N3x` AS sum " +
      s"FROM parquet.`$rootDir/data/${myState.numElementsInMillion}-million-int-triples-${myState.compression}.parquet`")


      .agg("sum" -> "max").first()

    blackhole.consume(max)
  }

  //  @Benchmark
  def dremio2(blackhole: Blackhole, myState: MyState): Unit = {
    val res = myState.spark.sql("SELECT" +
      " `x` + `N2x` + `N3x` AS s1," +
      " `x` * `N2x` - `N3x` AS s2," +
      " 3 * `x` + 2* `N2x` + `N3x` AS s3," +
      " `x` >= `N2x` - `N3x` AS c1," +
      " `x` +  `N2x` = `N3x` AS c2 " +
      s"FROM parquet.`$rootDir/data/5-million-int-triples.parquet`")
      .agg("s1" -> "sum",
        "s2" -> "sum",
        "s3" -> "sum",
        "c1" -> "count",
        "c2" -> "count",
      ).first()

    blackhole.consume(res)
  }

  //  @Benchmark
  def tenInts(blackhole: Blackhole, myState: MyState): Unit = {

    val max = myState.spark.sql(s"SELECT `x1` + `x2` + `x3` + `x4` + `x5` + `x6` + `x7` + `x8` + `x9` + `x10` AS sum " +
      s"FROM parquet.`$rootDir/data/million-times-10-ints-${myState.compression}.parquet` ")
      .agg("sum" -> "max").limit(1).first()

    blackhole.consume(max)
  }
}

object ParquetAndGandivaReadingBenchmark {

  @State(Scope.Thread)
  class MyState {

    var spark: SparkSession = _

    @Param(Array(PLAIN, PARQUET_ONLY, PARQUET_AND_GANDIVA, WITH_MAX_AGGREGATION))
    var sparkSetup: String = _

    @Param(Array("64000"))
    var batchSize: Int = _

    @Param(Array("snappy"))
    var compression: String = _

    @Param(Array("5", "50", "500")) //"1", "5", "10", "50", "100", "500"))
    var numElementsInMillion: Int = 0
    //    @Param(Array("true", "false"))
    var codegen: Boolean = true

    @Setup(Level.Trial)
    def doSetup(): Unit = {
      spark = SparkSetup.initSpark(sparkSetup, batchSize, codegen)
    }

    @Setup(Level.Invocation)
    def clearCache(): Unit = {
      spark.catalog.clearCache()
      spark.sqlContext.clearCache()
    }

    @TearDown(Level.Trial)
    def doTearDown(params: BenchmarkParams): Unit = {
      writeResults(params)
      //      println("DONE! You have 3 min to check http://192.168.0.102:4040/SQL/")
      //      Thread.sleep(3* 60*1000)
      spark.close()
    }
  }

}