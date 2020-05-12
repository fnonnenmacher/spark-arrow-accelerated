package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.VectorSchemaRoot
import nl.tudelft.ewi.abs.nonnenmacher.AutoCloseProcessingHelper._
import nl.tudelft.ewi.abs.nonnenmacher.PlasmaFacade.writeToPlasma
import org.apache.arrow.flatbuf.RecordBatch
import org.apache.arrow.gandiva.expression.ArrowTypeHelper
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema

import scala.collection.JavaConverters._

object ArrowProcessor {

  private lazy val processorJni = {
    System.loadLibrary("arrow")
    System.loadLibrary("parquet")
    System.loadLibrary("plasma")
    System.loadLibrary("fletcher_echo")
    System.loadLibrary("fletcher")
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


  def addThreeVectors2(iter: Iterator[(Schema, ArrowRecordBatch)]): Iterator[VectorSchemaRoot] = {

    iter.map{ case (schema, rb) =>
      val numRows = rb.getLength
      val bufAddrs: Array[Long] = rb.getBuffers.asScala.map( _.memoryAddress()).toArray
      val bufSizes: Array[Long] = rb.getBuffersLayout.asScala.map( _.getSize()).toArray

      val schemaAsBytes = ArrowTypeHelper.arrowSchemaToProtobuf(schema).toByteArray

      val id = ThreeIntAdderProcessor()(schemaAsBytes, numRows, bufAddrs, bufSizes);

      id
    }.map {PlasmaFacade.get}
      .wrap( x => new ArrowDeserializer(x))
  }
}
