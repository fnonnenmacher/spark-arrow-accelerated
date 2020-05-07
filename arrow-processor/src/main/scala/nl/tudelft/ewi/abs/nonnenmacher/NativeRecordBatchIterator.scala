package nl.tudelft.ewi.abs.nonnenmacher

/**
 * Hint: Use Java iterator instead of scala iterator, so that JNI understands it
 */
class NativeRecordBatchIterator(val ptr:Long) extends java.util.Iterator[Array[Byte]] {

  private var isClosed = false;

  override def hasNext: Boolean = {
    if (isClosed){
      return false;
    }
    val _hasNext : Boolean = hasNext(ptr)

    if(!_hasNext){ //All elements processed -> close/release now all C++ objects
      close(ptr)
      isClosed = true;
    }
    _hasNext
  }

  override def next(): Array[Byte] = next(ptr);

  @native private def hasNext(ptr_native: Long): Boolean

  @native private def next(ptr_native: Long): Array[Byte];

  @native private def close(ptr_native: Long): Unit;
}

