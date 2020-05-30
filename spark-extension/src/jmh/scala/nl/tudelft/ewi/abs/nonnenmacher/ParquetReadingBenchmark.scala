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
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 20, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class ParquetReadingBenchmark {

//  @Benchmark
  def readAllFields(myState: MyState): Unit = {
    val sqlDF: DataFrame = myState.spark.sql(s"SELECT * FROM parquet.`$rootDir/data/big-example.parquet`");
    println(sqlDF.collect().length)
  }

//  @Benchmark
  def readIntFieldAndCalcSum(myState: MyState): Unit = {
    val i = new Random().nextInt() %100

    val sqlDF: DataFrame = myState.spark.sql(s"SELECT `int-field` + $i AS s FROM parquet.`$rootDir/data/big-example.parquet`").agg("s"->"sum")
    println("SUM:"+  sqlDF.collect().head)
  }

//  @Benchmark
  def read5MillionIntTriples(myState: MyState): Unit = {
    val sqlDF: DataFrame = myState.spark.sql(s"SELECT * FROM parquet.`$rootDir/data/5million-int-triples.parquet`");

    val rows = sqlDF.collect()
    println("HEAD:"+rows.head)
    println("Length:"+ rows.length)
  }

//  @Benchmark
  def readFieldsOf5MillionIntTriples(myState: MyState): Unit = {
    val i = new Random().nextInt() %100
    val sqlDF: DataFrame = myState.spark.sql(s"SELECT * FROM parquet.`$rootDir/data/5million-int-triples.parquet`")
      .agg("x"->"sum", "N2x"->"sum", "N3x"->"sum")

    val rows = sqlDF.collect()
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

    @Setup(Level.Trial)
    def doSetup(): Unit = {
      spark = SparkSetup.initSpark(sparkSetup)
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