package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.FletcherExampleBenchmark.MyState
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.apache.spark.sql.SparkSession
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.{BenchmarkParams, Blackhole}

@BenchmarkMode(Array(Mode.SingleShotTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 30, time = 1)
@Measurement(iterations = 30, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class FletcherExampleBenchmark {

  @Benchmark
  def dremio1(blackhole: Blackhole, myState: MyState): Unit = {
    val res = myState.spark.sql(s""" SELECT SUM(`number`) as `number`
                        | FROM parquet.`/work/fnonnenmacher/data/chicago-taxi/taxi-uncompressed.parquet`
                        | WHERE `string` rlike 'Blue Ribbon Taxi Association Inc.' """.stripMargin)

    blackhole.consume(res.first())
  }
}

object FletcherExampleBenchmark {

  @State(Scope.Thread)
  class MyState {

    var spark: SparkSession = _

    @Param(Array(PLAIN, FLETCHER_EXAMPLE))
    var sparkSetup: String = _

    @Param(Array("64000", "128000", "256000", "512000"))
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
    def doTearDown(params: BenchmarkParams): Unit = {
      writeResults(params)
      spark.close()
    }
  }
}

