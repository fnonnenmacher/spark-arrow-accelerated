package nl.tudelft.ewi.abs.nonnenmacher.utils

object AutoCloseProcessingHelper {

  implicit class ProcessableIterator[T, U](iter: Iterator[T]) {
    def mapAndAutoClose(closableFunction: ClosableFunction[T, U]): Iterator[U] = {
      new IteratorWrapper[T, U](iter, closableFunction)
    }

    def wrap( f: Iterator[T] => Iterator[U]) : Iterator[U] = {
      f.apply(iter)
    }
  }

  private class IteratorWrapper[T, U](private val predecessor: Iterator[T],
                                      private val proc: ClosableFunction[T, U]) extends AutoCloseableIterator[U] {

    override def next(): U = proc.apply(predecessor.next());

    override protected def internalHasNext: Boolean = predecessor.hasNext

    override def close(): Unit = proc.close()
  }
}

trait AutoCloseableIterator[T] extends Iterator[T] with AutoCloseable {
  protected def internalHasNext: Boolean;

  private var isClosed = false;

  override final def hasNext: Boolean = {
    if (isClosed) return false

    val _hasNext = internalHasNext

    if (!_hasNext) {
      close();
      isClosed = true;
    }
    _hasNext
  }
}

/**
 * use java function instead of scala alternative, that it get's understood by jni
 */
trait ClosableFunction[A, B] extends java.util.function.Function[A, B] with AutoCloseable {

}
