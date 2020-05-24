package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.{BigIntVector, IntVector, VarCharVector}
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class NativeParquetReaderSuite extends FunSuite {

  val fileName = "data/big-example.parquet"

  val intField: Field = Field.nullable("int-field", MinorType.INT.getType)
  val longField: Field = Field.nullable("long-field", MinorType.BIGINT.getType)
  val stringField: Field = Field.nullable("string-field", MinorType.VARCHAR.getType)

  val inputSchema = new Schema(List(intField, longField, stringField).asJava)

  test("read int, long and string from parquet file") {
    val reader = new NativeParquetReader(fileName, inputSchema, schema(intField, longField, stringField), 50000)

    val root = reader.next();

    //all three fields are loaded
    assert(root.getFieldVectors.size() == 3)

    assert(root.getVector(intField.getName).isInstanceOf[IntVector])
    assert(root.getVector(intField.getName).asInstanceOf[IntVector].get(10) == 10) //value in parquet file is index

    assert(root.getVector(longField.getName).isInstanceOf[BigIntVector])
    assert(root.getVector(longField.getName).asInstanceOf[BigIntVector].get(10) == 100) //value in parquet file is index*index

    assert(root.getVector(stringField.getName).isInstanceOf[VarCharVector])
    assert(root.getVector(stringField.getName).asInstanceOf[VarCharVector].getObject(10).toString == "number-10") //value in parquet file is "number-$index"
  }

  test("that reading can be limited to subset of fields") {
    val reader = new NativeParquetReader(fileName, inputSchema, schema(longField), 50000)

    val root = reader.next();

    //all three fields are loaded
    assert(root.getFieldVectors.size() == 1)

    assert(root.getVector(longField.getName).isInstanceOf[BigIntVector])
    assert(root.getVector(intField.getName) == null)
    assert(root.getVector(stringField.getName) == null)
  }

  test("that the whole iterator can be processed") {
    //read only the int-field
    val reader = new NativeParquetReader(fileName, inputSchema, schema(intField), 60000)

    val allIntsCombined = reader.flatMap { root =>
      val vec = root.getVector(intField.getName).asInstanceOf[IntVector]
      (0 until root.getRowCount).map(vec.get)
    }.toList

    // parquet contains in total 1 million elements
    assert(allIntsCombined.size == 1000000)
    assert(allIntsCombined(999999) == 999999)
  }

  private def schema(fields: Field*): Schema = {
    new Schema(fields.asJava)
  }
}
