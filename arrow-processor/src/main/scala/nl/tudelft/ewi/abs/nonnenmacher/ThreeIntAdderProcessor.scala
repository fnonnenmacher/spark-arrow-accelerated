package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.PlasmaFacade.ObjectId

class ThreeIntAdderProcessor private(private val ptr: Long) extends ClosableFunction[ObjectId, ObjectId] {

  override def apply(objectId: ObjectId): ObjectId = process(ptr, objectId)

  override def close(): Unit = close(ptr);

  @native private def process(ptr_native: Long, objectId: Array[Byte]): Array[Byte];

  @native private def close(ptr_native: Long): Unit;
}

object ThreeIntAdderProcessor {

  private lazy val initializer: Initializer = {
    System.loadLibrary("fletcher_echo")
    System.loadLibrary("fletcher")
    System.loadLibrary("plasma")
    System.loadLibrary("arrow")
    System.loadLibrary("arrow-processor-native");
    new Initializer();
  }

  def apply(): ThreeIntAdderProcessor = new ThreeIntAdderProcessor(initializer.init());

  private class Initializer {

    @native def init(): Long;
  }

}


