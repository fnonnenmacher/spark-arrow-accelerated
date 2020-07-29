package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.GandivaBenchmark.TestState
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkMode(Array(Mode.SingleShotTime))
@OutputTimeUnit(MILLISECONDS)
@Warmup(iterations = 20, time = 1)
@Measurement(iterations = 20, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class GandivaBenchmark {

//  @Benchmark
  def maxOfsumOf10(blackhole: Blackhole, myState: TestState): Unit = {
    val max = myState.spark.sql(s"SELECT `x1` + `x2` + `x3` + `x4` + `x5` + `x6` + `x7` + `x8` + `x9` + `x10` AS sum FROM parquet.`$rootDir/data/50million-times-10-ints.parquet` ")
      .agg("sum" -> "max")
    blackhole.consume(max.collect())
  }
}

object GandivaBenchmark {

  @State(Scope.Thread)
  private class TestState extends SparkState {

    @Param(Array(VANILLA, PARQUET_AND_GANDIVA, PARQUET_AND_GANDIVA_WITH_MAX_AGGREGATION))
    var sparkSetup: String = _

    var batchSize = 100000
  }
}