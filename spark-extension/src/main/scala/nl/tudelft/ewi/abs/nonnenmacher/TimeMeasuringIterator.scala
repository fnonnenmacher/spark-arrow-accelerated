package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.sql.execution.metric.SQLMetric

class TimeMeasuringIterator[T](val innerIterator: Iterator[T], metric: SQLMetric) extends Iterator[T] {

  override def hasNext: Boolean = {
    val start = System.nanoTime()
    val r = innerIterator.hasNext
    metric += System.nanoTime() - start
    r
  }

  override def next(): T = {
    val start = System.nanoTime()
    val r = innerIterator.next()
    metric += System.nanoTime() - start
    r
  }
}
