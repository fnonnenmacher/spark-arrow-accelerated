package org.apache.spark.sql

import java.util.NoSuchElementException

import javax.annotation.Nullable
import org.apache.arrow.gandiva.evaluator.SelectionVector
import org.apache.arrow.vector.FieldVector
import org.apache.spark.sql.ColumnVectorAccessor._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._

/**
 * Spark's [[ColumnarBatch]], [[ColumnVector]] and [[ArrowColumnVector]] implementations have two big disadvantages for
 * my use-case.
 * 1) It does not allow the user to access the internal Arrow FieldVectors
 * 2) There is no possibility to add a "Selection Vector" similar to Gandiva's output
 * Unfortunately the classes are final and cannot be overwritten easily. However with custom implementations
 * of [[ColumnVector]] I can at least work-around it. This workaround however just works for my adaptions separately and
 * won't be compatible with other Spark columnar processor's e.g. the internal Parquet reader
 *
 * My workaround defines a convention for the column vectors stored in a Spark [[ColumnarBatch]]. The First vector is a
 * [[SelectionColumnVector]] which wraps the Gandiva selection wrapper, the other vectors are a [[ArrowColumnVectorWithFieldVector]]
 * which provides in comparison to Spark's implementation an accessor for the internal [[FieldVector]]. All ColumnarBatches,
 * which do not apply with this convention will lead to an exception.
 */
case class ColumnarBatchWithSelectionVector(fieldVectors: Seq[FieldVector], fieldVectorRows: Int, @Nullable selectionVector: SelectionVector) {
  val getRecordCount: Long = if (selectionVector != null) selectionVector.getRecordCount else fieldVectorRows

  /**
   * Creates a row iterator by applying the selection vector. Only rows from the selection vector are used, all other data is ignored.
   *
   * @return row iterator
   */
  def rowIterator: Iterator[InternalRow] = {
    val columnVectors: Array[ColumnVector] = fieldVectors.map(ArrowColumnVectorWithFieldVector).toArray
    val batchWithoutSelectionVector = new ColumnarBatch(columnVectors, fieldVectorRows)

    if (selectionVector !=null){

    selectionVector
      .indices
      .map(batchWithoutSelectionVector.getRow)
    }else{
      batchWithoutSelectionVector.rowIterator().asScala
    }
  }

  def toColumnarBatch: ColumnarBatch = {
    val columnVectors = new Array[ColumnVector](fieldVectors.size + 1)
    columnVectors(0) = SelectionColumnVector(selectionVector)
    fieldVectors.zipWithIndex.foreach { case (fieldVector, i) =>
      columnVectors(i + 1) = ArrowColumnVectorWithFieldVector(fieldVector)
    }
    new ColumnarBatch(columnVectors, fieldVectorRows)
  }
}

object ColumnarBatchWithSelectionVector {
  def from(columnarBatch: ColumnarBatch): ColumnarBatchWithSelectionVector = {
    if (!columnarBatch.column(0).isInstanceOf[SelectionColumnVector]) {
      throw new IllegalArgumentException("ColumnVector 0 of the ColumnarBatch is not a SelectionVector and cannot be processed therefore.")
    }

    val selectionVector = columnarBatch.column(0).asInstanceOf[SelectionColumnVector].selectionVector

    val fieldVectors = (1 until columnarBatch.numCols())
      .map(columnarBatch.column)
      .map {
        case ArrowColumnVectorWithFieldVector(fieldVector) => fieldVector
        case _ => throw new IllegalArgumentException("ColumnarBatch contains a vector which does not allow to access the arrow field vector")
      }
    new ColumnarBatchWithSelectionVector(fieldVectors, columnarBatch.numRows(), selectionVector)
  }
}


object ColumnVectorAccessor {

  implicit class IterableSelectionVector(selectionVector: SelectionVector) {
    def indices: Iterator[Int] = new Iterator[Int] {
      var i = 0

      override def hasNext: Boolean = i < selectionVector.getRecordCount

      override def next(): Int = {
        if (i >= selectionVector.getRecordCount) throw new NoSuchElementException
        val nextIndex = selectionVector.getIndex(i)
        i = i + 1
        nextIndex
      }
    }
  }

}