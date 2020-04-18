package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.PlasmaFacade.randomObjectId
import org.apache.arrow.vector.VectorSchemaRoot

object ArrowProcessor {

  lazy val processorJni = {
    System.loadLibrary("fletcher_echo")
    System.loadLibrary("fletcher")
    System.loadLibrary("plasma")
    System.loadLibrary("arrow")
    System.loadLibrary("arrow-processor-native");
    new ArrowProcessorJni();
  }

  /**
   * first try to call c++ code, currently not called from Spark
   *
   * @param root
   * @return
   */
  def sum(root: VectorSchemaRoot): Long = {

    val recordBuffer = ArrowRootByteConverter.convert(root)
    val objectId = PlasmaFacade.create(recordBuffer);

    val result = processorJni.sum(objectId);
    PlasmaFacade.delete(objectId);
    result
  }

  def addThreeVectors(root: VectorSchemaRoot): VectorSchemaRoot = {

    val recordBuffer = ArrowRootByteConverter.convert(root)
    val objectId = PlasmaFacade.create(recordBuffer);
    val objectIdOut = randomObjectId()

    processorJni.addingThreeValues(objectId, objectIdOut);

    val resData = PlasmaFacade.get(objectIdOut);
    ArrowRootByteConverter.convert(resData)
  }
}
