package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.PlasmaFacade.ObjectId
import org.apache.arrow.gandiva.ipc.GandivaTypes.Schema

class ThreeIntAdderProcessor private(private val ptr: Long) extends ClosableFunction[ObjectId, ObjectId] {

  override def apply(objectId: ObjectId): ObjectId = process(ptr, objectId)

  override def close(): Unit = close(ptr);

  @native private def process(ptr_native: Long, objectId: Array[Byte]): Array[Byte];

  @native private def close(ptr_native: Long): Unit;

  def apply(schema: Array[Byte], numRows: Int, bufAddrs: Array[Long], bufSizes: Array[Long]): ObjectId = process(ptr, schema, numRows, bufAddrs, bufSizes)

  @native private def process(ptr_native: Long, schema: Array[Byte], numRows: Int, bufAddrs: Array[Long], bufSizes: Array[Long]): Array[Byte];
}

object ThreeIntAdderProcessor {

  private lazy val initializer: Initializer = {
//    System.loadLibrary("protobuf")
    System.loadLibrary("arrow")
    System.loadLibrary("parquet")
    System.loadLibrary("plasma")
    System.loadLibrary("fletcher_echo")
    System.loadLibrary("fletcher")
    System.loadLibrary("arrow-processor-native");
    new Initializer();
  }

  def apply(): ThreeIntAdderProcessor = new ThreeIntAdderProcessor(initializer.init());

  private class Initializer {

    @native def init(): Long;
  }

}


