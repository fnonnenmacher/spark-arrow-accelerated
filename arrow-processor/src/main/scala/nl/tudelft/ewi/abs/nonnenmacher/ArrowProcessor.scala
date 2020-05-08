package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.VectorSchemaRoot
import nl.tudelft.ewi.abs.nonnenmacher.AutoCloseProcessingHelper._
import nl.tudelft.ewi.abs.nonnenmacher.PlasmaFacade.writeToPlasma

object ArrowProcessor {

  private lazy val processorJni = {
    System.loadLibrary("fletcher_echo")
    System.loadLibrary("fletcher")
    System.loadLibrary("plasma")
    System.loadLibrary("arrow")
    System.loadLibrary("arrow-processor-native");
    new ArrowProcessorJni();
  }

  def readParquet(fileName: String, fieldNames: Array[Int]): Iterator[VectorSchemaRoot] = {
    NativeRecordBatchIterator(fileName, fieldNames)
      .map {PlasmaFacade.get}
      .wrap( x =>  new ArrowDeserializer(x))
  }

  def sum(root: VectorSchemaRoot): Long = {

    val recordBuffer = ArrowRootByteConverter.convert(root)
    val objectId = PlasmaFacade.create(recordBuffer);

    val result = processorJni.sum(objectId);
    PlasmaFacade.delete(objectId);
    result
  }

  def addThreeVectors(iter: Iterator[VectorSchemaRoot]): Iterator[VectorSchemaRoot] =
    iter
      .map {ArrowRootByteConverter.convert}
      .mapAndAutoClose(writeToPlasma())
      .mapAndAutoClose(ThreeIntAdderProcessor())
      .map {PlasmaFacade.get}
      .wrap( x => new ArrowDeserializer(x))
}
