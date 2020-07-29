package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.FletcherBenchmark.TestState
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkMode(Array(Mode.SingleShotTime))
@OutputTimeUnit(MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class FletcherBenchmark {

//  @Benchmark
  def maxOfsumOf10(blackhole: Blackhole, myState: TestState): Unit = {
    val res = myState.spark.sql(s""" SELECT SUM(`number`) as `number`
                                   | FROM parquet.`$rootDir/data/taxi-uncompressed.parquet`
                                   | WHERE `string` rlike 'Blue Ribbon Taxi Association Inc.' """.stripMargin)
    blackhole.consume(res.collect())
  }
}
object FletcherBenchmark {

  @State(Scope.Thread)
  private class TestState extends SparkState {

    @Param(Array(VANILLA))
    var sparkSetup: String = _

    var batchSize = 10000000

    @Param(Array("10", "50", "100", "150"))
    var size :Int =_
  }
}

