package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.apache.arrow.gandiva.evaluator.NativeLibraryLoader
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._

@BenchmarkMode(Array(Mode.SingleShotTime))
@OutputTimeUnit(MILLISECONDS)
@Warmup(iterations = 20, time = 1)
@Measurement(iterations = 20, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class ParquetReaderInIsolationBenchmark {

  @Benchmark
  def loadWholeFile: Unit = {
    NativeLibraryLoader.load()

    val stringField: Field = notNullable("string", MinorType.VARCHAR.getType)
    val numberField: Field = notNullable("number", MinorType.BIGINT.getType)

    val inputSchema = new Schema(List(stringField, numberField).asJava)

    val proxy = new ParquetReaderEvaluator;
    proxy.readWholeFileWithDefaultMemoryPool(s"$rootDir/data/taxi-uncompressed.parquet", inputSchema, 100000)
  }

  def notNullable(name: String, arrowType: ArrowType): Field = {
    new Field(name, notNull(arrowType), null)
  }

  def notNull(arrowType: ArrowType): FieldType = {
    new FieldType(true, arrowType, null, null)
  }
}
