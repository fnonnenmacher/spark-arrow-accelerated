package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.gandiva.evaluator.NativeLibraryLoader
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class PerfomanceTestOfParquetReaderSuite extends FunSuite {

  test("read tax-dataset"){
    NativeLibraryLoader.load()
    val proxy = new ParquetReaderEvaluator;

    val stringField: Field =  notNullable("string", MinorType.VARCHAR.getType)
    val numberField: Field = notNullable("number", MinorType.BIGINT.getType)

    val inputSchema = new Schema(List(stringField, numberField).asJava)

    val start = System.currentTimeMillis()
    proxy.readWholeFileWithDefaultMemoryPool("../data/taxi-uncompressed.parquet",inputSchema, 100000 )
    val end = System.currentTimeMillis()
    println(s"Duration: ${end-start}")
  }

  def notNullable(name: String, arrowType: ArrowType) : Field={
    new Field(name, notNull(arrowType), null)
  }

  def notNull(arrowType: ArrowType): FieldType = {
    new FieldType(true, arrowType, null, null)
  }
}


