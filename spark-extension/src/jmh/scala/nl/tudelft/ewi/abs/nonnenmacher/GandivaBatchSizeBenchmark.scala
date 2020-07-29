package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.GandivaBatchSizeBenchmark.TestState
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkMode(Array(Mode.SingleShotTime))
@OutputTimeUnit(MILLISECONDS)
@Warmup(iterations = 20, time = 1)
@Measurement(iterations = 20, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class GandivaBatchSizeBenchmark {


//  @Benchmark
  def maxOfsumOf10(blackhole: Blackhole, myState: TestState): Unit = {
    val max = myState.spark.sql(s"SELECT `x1` + `x2` + `x3` + `x4` + `x5` + `x6` + `x7` + `x8` + `x9` + `x10` AS sum FROM parquet.`$rootDir/data/50million-times-10-ints.parquet` ")
      .agg("sum" -> "max")
    blackhole.consume(max.collect())
  }
}

object GandivaBatchSizeBenchmark {

  @State(Scope.Thread)
  private class TestState extends SparkState {

    @Param(Array(PARQUET_AND_GANDIVA_WITH_MAX_AGGREGATION))
    var sparkSetup: String = _

    @Param(Array("500", "1000", "5000", "10000", "50000", "100000", "500000"))
    var batchSize = 100000
  }
}

