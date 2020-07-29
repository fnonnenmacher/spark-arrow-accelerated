package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.ParquetReaderBatchSizeBenchmark.TestState
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SingleShotTime))
@Warmup(iterations = 20)
@Measurement(iterations = 20, timeUnit = MILLISECONDS)
class ParquetReaderBatchSizeBenchmark {

//  @Benchmark
  def maxOf3IntValues(blackhole: Blackhole, state: TestState): Unit = {
    val res = state.spark.sql(s"SELECT MAX(`x`), MAX(`N2x`), MAX(`Nx3`) FROM parquet.`$rootDir/data/500-million-int-triples-uncompressed.parquet`")
    blackhole.consume(res.collect())
  }
}

private object ParquetReaderBatchSizeBenchmark {

  @State(Scope.Thread)
  private class TestState extends SparkState {

    @Param(Array(ARROW_PARQUET_WITH_MAX_AGGREGATION))
    var sparkSetup: String = _

    @Param(Array("500", "1000", "5000", "10000", "50000", "100000", "500000"))
    var batchSize: Int = _
  }
}