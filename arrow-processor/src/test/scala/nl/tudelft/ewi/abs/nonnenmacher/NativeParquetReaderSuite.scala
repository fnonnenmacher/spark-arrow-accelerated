package nl.tudelft.ewi.abs.nonnenmacher

import java.util

import nl.tudelft.ewi.abs.nonnenmacher.utils.{ArrowVectorBuilder, IntegerVector, LongVector}
import org.apache.arrow.gandiva.evaluator.Projector
import org.apache.arrow.gandiva.expression.{ExpressionTree, TreeBuilder, TreeNode}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.arrow.vector.{BigIntVector, IntVector, ValueVector, VarCharVector, VectorUnloader}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class NativeParquetReaderSuite extends FunSuite {

  test("read from parquet with memory mapping"){
    val in1: Field = Field.nullable("int-field", MinorType.INT.getType)
    val in2: Field = Field.nullable("long-field", MinorType.BIGINT.getType)
    val in3: Field = Field.nullable("string-field", MinorType.VARCHAR.getType)

    val inputSchema = new Schema(List(in1, in2, in3).asJava)
    val outputSchema = new Schema(List(in1, in2).asJava)

    val reader = JNIProcessorFactory.parquetReader("data/big-example.parquet", inputSchema, outputSchema, 2000)

    val root = reader.next()

    println(root.getRowCount)
    val resVec1 = root.getVector(1).asInstanceOf[BigIntVector]
    println(">"+resVec1.get(999));
  }
}
