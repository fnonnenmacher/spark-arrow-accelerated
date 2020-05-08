package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.VectorSchemaRoot

import scala.collection.JavaConverters._

import nl.tudelft.ewi.abs.nonnenmacher.AutoCloseProcessingHelper._

object ArrowProcessor {

  private lazy val processorJni = {
    System.loadLibrary("fletcher_echo")
    System.loadLibrary("fletcher")
    System.loadLibrary("plasma")
    System.loadLibrary("arrow")
    System.loadLibrary("arrow-processor-native");
    new ArrowProcessorJni();
  }

  def readParquet(fileName: String): Iterator[VectorSchemaRoot] = {
    val pointer = processorJni.readParquete(fileName)
    val nativeIter = new NativeRecordBatchIterator(pointer).asScala
    new ArrowDeserializer(nativeIter.map(PlasmaFacade.get))
  }

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

    val objectIdOut = processorJni.addingThreeValues(objectId);

    val resData = PlasmaFacade.get(objectIdOut);
    ArrowRootByteConverter.convert(resData)
  }

  def addThreeVectors(iter: Iterator[VectorSchemaRoot]): Iterator[VectorSchemaRoot] = {
    iter
      .map {ArrowRootByteConverter.convert}
      .map {PlasmaFacade.create}
      .mapAndAutoClose(ThreeIntAdderProcessor())
      .map {PlasmaFacade.get}
      .wrap( new ArrowDeserializer(_))
    }
}
