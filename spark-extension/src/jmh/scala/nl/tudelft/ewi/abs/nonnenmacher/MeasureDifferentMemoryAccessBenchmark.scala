package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.MeasureDifferentMemoryAccessBenchmark.MyState
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.apache.spark.sql.SparkSession
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.{BenchmarkParams, Blackhole}

@BenchmarkMode(Array(Mode.SingleShotTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 100, time = 1)
@Measurement(iterations = 100, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class MeasureDifferentMemoryAccessBenchmark {

  //@Benchmark
  def access3Fields(blackhole: Blackhole, myState: MyState): Unit = {
    val row = myState.spark.sql(s"SELECT `x` + `N2x` + `N3x` AS sum FROM parquet.`$rootDir/data/${myState.numElementsInMillion}-million-int-triples-snappy.parquet`")
      .agg("sum" -> "max").first()
    blackhole.consume(row)
  }
}

object MeasureDifferentMemoryAccessBenchmark {

  @State(Scope.Thread)
  class MyState {

    var spark: SparkSession = _

    @Param(Array(PLAIN, PARQUET_ONLY, PARQUET_AND_MEMORY_CONVERSION))
    var sparkSetup: String = _

    @Param(Array("64000"))
    var batchSize: Int = _

    @Param(Array("snappy"))
    var compression: String = _

    @Param(Array("5")) //"1", "5", "10", "50", "100", "500"))
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
      spark.close()
    }
  }
}


