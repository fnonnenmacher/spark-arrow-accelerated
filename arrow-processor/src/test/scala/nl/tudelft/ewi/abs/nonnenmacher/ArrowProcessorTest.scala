package nl.tudelft.ewi.abs.nonnenmacher

import java.util

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
class ArrowProcessorTest extends FunSuite {

  test("test reading of big parquet file to verify sending of multiple batches works correctly") {
    val iter = ArrowProcessor.readParquet("data/big-example.parquet", Array(0));

    val allInts = iter.flatMap { root =>
      val intVector = root.getVector("int-field").asInstanceOf[IntVector]
      assert(root.getFieldVectors.size() == 1) //only 1 vector, because of passed field indices
      Range(0, root.getRowCount).map(intVector.get)
    }.toSeq

    assert(allInts.size == 1e6)
    assert(allInts.head == 0)
    assert(allInts(456) == 456)
    assert(allInts.last == 999999)
  }

  test("test reading of parquet file") {

    val iter = ArrowProcessor.readParquet("data/example.parquet", Array(1, 2, 0));

    val rootRes = iter.next()

    assert(rootRes.getFieldVectors.size() == 3)
    //order of fields should be as requested in paramterA rray(1,2, 0)
    assert(rootRes.getFieldVectors.asScala.map(_.getField.getName) == List("long-field", "string-field", "int-field"))

    val intVector = rootRes.getVector("int-field").asInstanceOf[IntVector]
    val intData = Range(0, rootRes.getRowCount).map(intVector.get)
    assert(intData == Seq(0, 1, 2, 3, 4))

    val longVector = rootRes.getVector("long-field").asInstanceOf[BigIntVector]
    val longData = Range(0, rootRes.getRowCount).map(longVector.get)
    assert(longData == Seq(0, 1, 4, 9, 16))

    val stringVector = rootRes.getVector("string-field").asInstanceOf[VarCharVector]
    val stringData = Range(0, rootRes.getRowCount).map(stringVector.getObject(_).toString)
    assert(stringData == Seq("number-0", "number-1", "number-2", "number-3", "number-4"))
  }

  ignore("calculate the sum of an example vector") {

    val root = ArrowVectorBuilder.toSchemaRoot(LongVector("some-values", Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))

    val res = ArrowProcessor.sum(root)

    assert(res === 55)
  }

  test("add up three vectors") {

    val v1 = IntegerVector("in1", Seq(1, 2, 3))
    val v2 = IntegerVector("in2", Seq(10, 20, 30))
    val v3 = IntegerVector("in3", Seq(100, 200, 300))
    val root = ArrowVectorBuilder.toSchemaRoot(v1, v2, v3)

    val itRes = ArrowProcessor.addThreeVectors(Iterator(root))

    val rootRes = itRes.next()
    assert(rootRes.getFieldVectors.size() == 1)

    val resVec1 = rootRes.getVector(0).asInstanceOf[IntVector]
    assert(resVec1.getName == "out")
    assert(resVec1.get(0) == 111)
    assert(resVec1.get(1) == 222)
    assert(resVec1.get(2) == 333)
  }

  test("add up three vectors new") {

    val v1 = IntegerVector("in1", Seq(1, 2, 3))
    val v2 = IntegerVector("in2", Seq(10, 20, 30))
    val v3 = IntegerVector("in3", Seq(100, 200, 300))
    val root = ArrowVectorBuilder.toSchemaRoot(v1, v2, v3)

    val rb = new VectorUnloader(root).getRecordBatch

    val itRes = ArrowProcessor.addThreeVectors2(Iterator((root.getSchema, rb)))

    val rootRes = itRes.next()
    assert(rootRes.getFieldVectors.size() == 1)

    val resVec1 = rootRes.getVector(0).asInstanceOf[IntVector]
    assert(resVec1.getName == "out")
    assert(resVec1.get(0) == 111)
    assert(resVec1.get(1) == 222)
    assert(resVec1.get(2) == 333)
  }

  test("add up three vectors with Gandiva") {

    val int32 = new ArrowType.Int(32, true);
    val in1: Field = Field.nullable("in1", int32)
    val in2: Field = Field.nullable("in2", int32)
    val in3: Field = Field.nullable("in3", int32)

    val out: Field = Field.nullable("out", int32)

    val args: util.List[TreeNode] = new util.ArrayList[TreeNode]

    // in1 + in2 + in3
    val add1: TreeNode = TreeBuilder.makeFunction("add", List(TreeBuilder.makeField(in1), TreeBuilder.makeField(in2)).asJava, int32)
    val add: TreeNode = TreeBuilder.makeFunction("add", List(add1, TreeBuilder.makeField(in3)).asJava, int32)
    val expr: ExpressionTree = TreeBuilder.makeExpression(add, out)

    val cols = List(in1, in2, in3)
    val schema: Schema = new Schema(cols.asJava)

    val eval: Projector = Projector.make(schema, List(expr).asJava)

    val v1 = IntegerVector("in1", Seq(1, 2, 3))
    val v2 = IntegerVector("in2", Seq(10, 20, 30))
    val v3 = IntegerVector("in3", Seq(100, 200, 300))
    val root = ArrowVectorBuilder.toSchemaRoot(v1, v2, v3)

    val recordBatch: ArrowRecordBatch = new VectorUnloader(root).getRecordBatch

    val outVector = new IntVector("out", GlobalAllocator.newChildAllocator(classOf[ArrowProcessorTest]))

    outVector.allocateNew(root.getRowCount)

    val output: List[ValueVector] = List(outVector)
    eval.evaluate(recordBatch, output.asJava)

    assert(outVector.get(0) == 111)
    assert(outVector.get(1) == 222)
    assert(outVector.get(2) == 333)
    println(outVector.get(0))
  }

  test("read from parquet with memory mapping"){
    val in1: Field = Field.nullable("int-field", MinorType.INT.getType)
    val in2: Field = Field.nullable("long-field", MinorType.BIGINT.getType)
    val in3: Field = Field.nullable("string-field", MinorType.VARCHAR.getType)

    val schema = new Schema(List(in1, in2, in3).asJava)

    val reader = NativeParquetReader("data/big-example.parquet", schema, 1000)

    val root = reader.next()

    println(root.getRowCount)
    val resVec1 = root.getVector(2).asInstanceOf[VarCharVector]
    println(">"+resVec1.getObject(999).toString);
  }
}
