package nl.tudelft.ewi.abs.nonnenmacher


import java.util.Random
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.ParquetReadingBenchmark.MyState
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openjdk.jmh.annotations._
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class ParquetReadingBenchmark {

//  @Benchmark
  def readAllFields(myState: MyState): Unit = {
    val allFields = myState.spark.sql(s"SELECT * FROM parquet.`$rootDir/data/big-example.parquet`").collect();
    println(allFields.length)
  }

//  @Benchmark
  def readSingleIntField(myState: MyState): Unit = {
    val singleField = myState.spark.sql(s"SELECT `int-field` FROM parquet.`$rootDir/data/big-example.parquet`").collect()
    println("SUM:"+  singleField.length)
  }

//  @Benchmark
  def readSingleIntFieldAndCalcSum(myState: MyState): Unit = {
    val sum = myState.spark.sql(s"SELECT `int-field` FROM parquet.`$rootDir/data/big-example.parquet`")
      .agg("int-field"->"sum").collect()
    println("SUM:"+  sum.head)
  }

//  @Benchmark
  def read5MillionIntTriples(myState: MyState): Unit = {
    val rows = myState
      .spark.sql(s"SELECT * FROM parquet.`$rootDir/data/5million-int-triples.parquet`").collect()

    println("HEAD:"+rows.head)
    println("Length:"+ rows.length)
  }

//  @Benchmark
  def readSingleIntOf5MillionIntTriples(myState: MyState): Unit = {
    val rows = myState.spark.sql(s"SELECT * FROM parquet.`$rootDir/data/5million-int-triples.parquet`")
      .collect()

    println("HEAD:"+rows.head)
    println("Length:"+ rows.length)
  }

//  @Benchmark
  def readSingleIntOf5MillionIntTriplesAndCalcSum(myState: MyState): Unit = {
    val rows = myState.spark.sql(s"SELECT * FROM parquet.`$rootDir/data/5million-int-triples.parquet`")
      .agg("x"->"sum", "N2x"->"sum", "N3x"->"sum").collect()

    println("HEAD:"+rows.head)
    println("Length:"+ rows.length)
  }
}

object ParquetReadingBenchmark {

  @State(Scope.Thread)
  class MyState {

    var spark: SparkSession = _

    @Param(Array("PLAIN", "PARQUET_ONLY")) //
    var sparkSetup: String = _

    @Param(Array( "32000", "64000"))
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