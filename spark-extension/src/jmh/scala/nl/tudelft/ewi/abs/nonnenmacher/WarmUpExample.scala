package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.WarmUpExample.TestState
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkMode(Array(Mode.SingleShotTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 0)
@Measurement(iterations = 50, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class WarmUpExample {

//  @Benchmark
  def read1MillionInts(blackhole: Blackhole, state: TestState): Unit = {
    val res = state.spark.sql(s"SELECT MAX(`value`) FROM parquet.`$rootDir/data/500-million-ints-uncompressed.parquet`")
    blackhole.consume(res.collect())
  }
}

object WarmUpExample {

  @State(Scope.Thread)
  private class TestState extends SparkState {

    @Param(Array(VANILLA))
    var sparkSetup: String = _

    var batchSize = 10000
  }
}



