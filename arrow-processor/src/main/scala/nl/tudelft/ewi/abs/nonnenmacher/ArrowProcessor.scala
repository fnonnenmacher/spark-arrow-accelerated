package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.VectorSchemaRoot

class ArrowProcessor {

  lazy val processorJni = {
    System.loadLibrary("arrow")
    System.loadLibrary("plasma")
    System.loadLibrary("arrow-processor-native");
    new ArrowProcessorJni();
  }

  def sum(root: VectorSchemaRoot): Long = {

    val recordBuffer = VectorSchemaRootToRecordBuffer.convert(root)
    val objectId = PlasmaFacade.create(recordBuffer);

    val result = processorJni.sum(objectId);

    PlasmaFacade.delete(objectId);
    result
  }

}
