package nl.tudelft.ewi.abs.nonnenmacher

/**
 * Hint: Use Java iterator instead of scala iterator, that it is known by jni
 */
class NativeRecordBatchIterator(val ptr:Long) extends java.util.Iterator[Array[Byte]] {

//  val ptr: Long = initiate();

  override def hasNext: Boolean = hasNext(ptr);

  override def next(): Array[Byte] = {

    val objectId = next(ptr);

    return objectId;
  }

//  @native private def initiate(): Long;

  @native private def hasNext(ptr_native: Long): Boolean

  @native private def next(ptr_native: Long): Array[Byte];

  @native private def destroy(): Unit;
}

