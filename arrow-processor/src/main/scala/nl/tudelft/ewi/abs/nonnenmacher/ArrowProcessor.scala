package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.VectorSchemaRoot

class ArrowProcessor {

  lazy val processorJni = {
    System.loadLibrary("fletcher_echo")
    Thread.sleep(1000)
    System.loadLibrary("fletcher")
    System.loadLibrary("plasma")
    System.loadLibrary("arrow")
    Thread.sleep(1000)
    System.loadLibrary("arrow-processor-native");
    new ArrowProcessorJni();
  }

  def sum(root: VectorSchemaRoot): Long = {

    val recordBuffer = VectorSchemaRootToRecordBuffer.convert(root)
    val objectId = PlasmaFacade.create(recordBuffer);

    val result = processorJni.sum(objectId);
//    val result=3
    PlasmaFacade.delete(objectId);
    result
  }

}
