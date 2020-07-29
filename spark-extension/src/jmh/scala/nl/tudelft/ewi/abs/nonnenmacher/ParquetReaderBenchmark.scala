package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.ParquetReaderBenchmark.TestState
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SingleShotTime))
@Warmup(iterations = 20)
@Measurement(iterations = 20, timeUnit = MILLISECONDS)
class ParquetReaderBenchmark {

//  @Benchmark
  def maxOf3IntValues(blackhole: Blackhole, state: TestState): Unit = {
    val res = state.spark.sql(s"SELECT MAX(`x`), MAX(`N2x`), MAX(`Nx3`) FROM parquet.`$rootDir/data/500-million-int-triples-uncompressed.parquet`")
    blackhole.consume(res.collect())
  }
}

object ParquetReaderBenchmark {

  @State(Scope.Thread)
  private class TestState extends SparkState {

    @Param(Array("uncompressed"))
    var compression: String = _

//    @Param(Array(VANILLA))
    @Param(Array(VANILLA, ARROW_PARQUET, ARROW_PARQUET_WITH_MAX_AGGREGATION))
    var sparkSetup: String = _

    var batchSize = 100000
  }
}





