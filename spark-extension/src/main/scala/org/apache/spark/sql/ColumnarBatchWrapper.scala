package org.apache.spark.sql

import java.util.NoSuchElementException

import org.apache.arrow.gandiva.evaluator.SelectionVector
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._

abstract class ColumnarBatchWrapper(val fieldVectors: Array[FieldVector], val numRows: Int) {

  def toColumnarBatch: ColumnarBatch

  def rowIterator: Iterator[InternalRow]
}

object ColumnarBatchWrapper {
  def from: ColumnarBatch => ColumnarBatchWrapper = {
    case ArrowBatch(fieldVectors, numRows) => ArrowBatch(fieldVectors, numRows)
    case ArrowBatchWithSelection(fieldVectors, numRows, selectionVector) => ArrowBatchWithSelection(fieldVectors, numRows, selectionVector)
    case _ => throw new IllegalArgumentException("ColumnarBatch cannot be converted to ColumnarBatchWrapper")
  }

  def apply(root: VectorSchemaRoot): ColumnarBatchWrapper = {
    val fieldVectors: Array[FieldVector] = root.getFieldVectors.asScala.toArray
    ArrowBatch(fieldVectors, root.getRowCount)
  }

  def apply(root: VectorSchemaRoot, selectionVector: SelectionVector): ColumnarBatchWrapper = {
    val fieldVectors: Array[FieldVector] = root.getFieldVectors.asScala.toArray
    ArrowBatchWithSelection(fieldVectors, root.getRowCount, selectionVector)
  }
}

case class ArrowBatch(override val fieldVectors: Array[FieldVector], override val numRows: Int) extends ColumnarBatchWrapper(fieldVectors, numRows) {
  override def toColumnarBatch: ColumnarBatch = {
    val columnVectors: Array[ColumnVector] = fieldVectors.map(ArrowColumnVectorWithFieldVector)
    new ColumnarBatch(columnVectors, numRows)
  }

  override def rowIterator: Iterator[InternalRow] = toColumnarBatch.rowIterator().asScala
}

object ArrowBatch extends {
  def unapply(wrapper: ColumnarBatchWrapper): Option[(Array[FieldVector], Int)] = {
    wrapper match {
      case arrowBatch: ArrowBatch => Option(arrowBatch.fieldVectors, arrowBatch.numRows)
      case _ => Option.empty
    }
  }

  def unapply(columnarBatch: ColumnarBatch): Option[(Array[FieldVector], Int)] = {
    val columnVectors = (0 until columnarBatch.numCols)
      .map(columnarBatch.column)

    val matches = columnVectors.forall(_.isInstanceOf[ArrowColumnVectorWithFieldVector])
    if (!matches) return Option.empty

    val fieldVectors = columnVectors.map(_.asInstanceOf[ArrowColumnVectorWithFieldVector].vector).toArray
    Option.apply(fieldVectors, columnarBatch.numRows)
  }
}

case class ArrowBatchWithSelection(override val fieldVectors: Array[FieldVector], override val numRows: Int, selectionVector: SelectionVector = null) extends ColumnarBatchWrapper(fieldVectors, numRows) {

  def toColumnarBatch: ColumnarBatch = {
    val columnVectors: Array[ColumnVector] = (selectionVector +: fieldVectors).map {
      case s: SelectionVector => SelectionColumnVector(s)
      case f: FieldVector => ArrowColumnVectorWithFieldVector(f)
    }
    new ColumnarBatch(columnVectors, numRows)
  }

  override def rowIterator: Iterator[InternalRow] = new Iterator[InternalRow] {
    private var i = 0
    private val columnarBatch = {
      val columnVectors: Array[ColumnVector] = fieldVectors.map(ArrowColumnVectorWithFieldVector)
      new ColumnarBatch(columnVectors, numRows)
    }

    override def hasNext: Boolean = i < selectionVector.getRecordCount

    override def next(): InternalRow = {
      println(s"$i of ${selectionVector.getRecordCount}")
      if (i >= selectionVector.getRecordCount) throw new NoSuchElementException
      val nextIndex = selectionVector.getIndex(i)
      i = i + 1
      columnarBatch.getRow(nextIndex)
    }
  }
}

object ArrowBatchWithSelection {

  def unapply(columnarBatch: ColumnarBatch): Option[(Array[FieldVector], Int, SelectionVector)] = {
    if (!columnarBatch.column(0).isInstanceOf[SelectionColumnVector]) return Option.empty
    val columnVectors = (1 until columnarBatch.numCols)
      .map(columnarBatch.column)
    val matches = columnVectors.forall(_.isInstanceOf[ArrowColumnVectorWithFieldVector])
    if (!matches) return Option.empty

    val selectionVector = columnarBatch.column(0).asInstanceOf[SelectionColumnVector].selectionVector
    val fieldVectors = columnVectors.map(_.asInstanceOf[ArrowColumnVectorWithFieldVector].vector).toArray
    Option.apply(fieldVectors, columnarBatch.numRows, selectionVector)
  }

  def unapply(wrapper: ColumnarBatchWrapper): Option[(Array[FieldVector], Int, SelectionVector)] = {
    wrapper match {
      case batch: ArrowBatchWithSelection => Option(batch.fieldVectors, batch.numRows, batch.selectionVector)
      case _ => Option.empty
    }
  }
}

