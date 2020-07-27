package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import nl.tudelft.ewi.abs.nonnenmacher.GlobalAllocator
import nl.tudelft.ewi.abs.nonnenmacher.utils.{ArrowVectorBuilder, IntegerVector}
import org.apache.arrow.gandiva.evaluator.{Filter, Projector, SelectionVectorInt16}
import org.apache.arrow.gandiva.expression.TreeBuilder._
import org.apache.arrow.gandiva.expression.{Condition, ExpressionTree}
import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.arrow.vector.{IntVector, ValueVector, VectorUnloader}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class GandivaExampleSuit extends FunSuite {

  private val allocator = GlobalAllocator.newChildAllocator(classOf[GandivaExampleSuit]);

  test("that all Gandiva dependencies are set-up correctly and it can execute a projection") {
    // In this test basically no code from my work is executed and it just checks Gandiva's functionality

    // arrow field definition of input
    val in1: Field = Field.nullable("in1", MinorType.INT.getType)
    val in2: Field = Field.nullable("in2", MinorType.INT.getType)

    // arrow field definition of output
    val out: Field = Field.nullable("out", MinorType.INT.getType)

    // defining the gandiva expression, which should be executed on Gandiva
    val gandivaProjectExpression: ExpressionTree = makeExpression(makeFunction("add", List(makeField(in1), makeField(in2)).asJava, MinorType.INT.getType), out)
    val gandivaFilterExpression: Condition = makeCondition(makeFunction("greater_than", List(makeField(in1), makeLiteral(new Integer(1))).asJava, ArrowType.Bool.INSTANCE))

    // initializing the gandiva projection
    val inputSchema: Schema = new Schema(List(in1, in2).asJava)
    val filter: Filter = Filter.make(inputSchema, gandivaFilterExpression)
    val projector: Projector = Projector.make(inputSchema, List(gandivaProjectExpression).asJava, SelectionVectorType.SV_INT16)

    // arrow input data - field names 
    val v1 = IntegerVector(in1.getName, Seq(1, 2, 3))
    val v2 = IntegerVector(in2.getName, Seq(10, 20, 30))

    // convert input data into recordbatch 
    val root = ArrowVectorBuilder.toSchemaRoot(v1, v2)
    val recordBatch: ArrowRecordBatch = new VectorUnloader(root).getRecordBatch

    //allocate selection vector
    val selectionVector = new SelectionVectorInt16(allocator.buffer(root.getRowCount * 2)) //INT 16 -> 2 Bytes per index => Max 2^16 rows

    // execute filter expression
    filter.evaluate(recordBatch, selectionVector)

    // allocate memroy for the output vector
    val outVector = new IntVector("out", allocator)
    outVector.allocateNew(root.getRowCount)
    val outVectors: List[ValueVector] = List(outVector)

    //execute Gandiva projection
    projector.evaluate(recordBatch, selectionVector, outVectors.asJava)

    println(outVector.getValueCount);
    // verify output data
    assert(outVector.get(0) == 22)
    assert(outVector.get(1) == 33)

    selectionVector.getBuffer.close()
    root.close()
    recordBatch.close()
    outVector.close()
    projector.close()

    assert(GlobalAllocator.getAllocatedMemory() == 0)
  }
}