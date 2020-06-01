package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.utils.{ArrowVectorBuilder, IntegerVector, LongVector, StringVector}
import org.apache.arrow.vector.{BigIntVector, IntVector, VarCharVector}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CopyProcessorSuite extends FunSuite {

  test("that a integer field vector gets correctly copied by the processor"){

    val intVector = IntegerVector("my-vector", Seq(1, 2, 3))
    val rootIn = ArrowVectorBuilder.toSchemaRoot(intVector)

    val proc = JNIProcessorFactory.copyProcessor(rootIn.getSchema);

    val rootOut = proc.apply(rootIn);

    assert(rootOut.getRowCount == 3)
    assert(rootOut.getFieldVectors.size() == 1)

    val resVector = rootOut.getVector(0).asInstanceOf[IntVector]
    assert(resVector.get(0) == 1)
    assert(resVector.get(1) == 2)
    assert(resVector.get(2) == 3)

    rootIn.close()
    proc.close()

    assert(GlobalAllocator.getAllocatedMemory() == 0)
  }

  test("that a long field vector gets correctly copied by the processor"){

    val longVector = LongVector("long-vector", Seq(1, 2, 3))
    val rootIn = ArrowVectorBuilder.toSchemaRoot(longVector)

    val proc = JNIProcessorFactory.copyProcessor(rootIn.getSchema);

    val rootOut = proc.apply(rootIn);

    assert(rootOut.getRowCount == 3)
    assert(rootOut.getFieldVectors.size() == 1)

    val resVector = rootOut.getVector(0).asInstanceOf[BigIntVector]
    assert(resVector.get(0) == 1L)
    assert(resVector.get(1) == 2L)
    assert(resVector.get(2) == 3L)

    rootIn.close()
    proc.close()

    assert(GlobalAllocator.getAllocatedMemory() == 0)
  }


  test("that a string field vector with varibale size gets correctly copied by the processor"){

    val stringVector = StringVector("string-vector", Seq("Hallo", "Welt", "!"))
    val rootIn = ArrowVectorBuilder.toSchemaRoot(stringVector)

    val proc = JNIProcessorFactory.copyProcessor(rootIn.getSchema);

    val rootOut = proc.apply(rootIn);

    assert(rootOut.getRowCount == 3)
    assert(rootOut.getFieldVectors.size() == 1)

    val resVector = rootOut.getVector(0).asInstanceOf[VarCharVector]
    assert(resVector.getObject(0).toString == "Hallo")
    assert(resVector.getObject(1).toString == "Welt")
    assert(resVector.getObject(2).toString == "!")

    rootIn.close()
    proc.close()

    assert(GlobalAllocator.getAllocatedMemory() == 0)
  }
}