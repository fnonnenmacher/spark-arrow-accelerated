package org.apache.spark.sql

import javax.annotation.Nullable
import org.apache.arrow.gandiva.evaluator.SelectionVector
import org.apache.spark.sql.types.{DataType, Decimal, IntegerType, ShortType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarArray, ColumnarBatch, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Wrapper of [[SelectionVector]] to be able to store it as [[ColumnVector]] in  a [[ColumnarBatch]]
 *
 * @param selectionVector Gandiva's selection wrapper which get's wrapped
 */
case class SelectionColumnVector(@Nullable val selectionVector: SelectionVector) extends ColumnVector(IntegerType) {

  override def close(): Unit = if (selectionVector!=null) selectionVector.getBuffer.close()

  override def hasNull: Boolean = false

  override def numNulls(): Int = 0

  override def isNullAt(rowId: Int): Boolean = false

  override def getBoolean(rowId: Int): Boolean = throw new UnsupportedOperationException

  override def getByte(rowId: Int): Byte = throw new UnsupportedOperationException

  override def getShort(rowId: Int): Short = throw new UnsupportedOperationException

  override def getInt(rowId: Int): Int = if (selectionVector!=null) selectionVector.getIndex(rowId) else rowId

  override def getLong(rowId: Int): Long = throw new UnsupportedOperationException

  override def getFloat(rowId: Int): Float = throw new UnsupportedOperationException

  override def getDouble(rowId: Int): Double = throw new UnsupportedOperationException

  override def getArray(rowId: Int): ColumnarArray = throw new UnsupportedOperationException

  override def getMap(ordinal: Int): ColumnarMap = throw new UnsupportedOperationException

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = throw new UnsupportedOperationException

  override def getUTF8String(rowId: Int): UTF8String = throw new UnsupportedOperationException

  override def getBinary(rowId: Int): Array[Byte] = throw new UnsupportedOperationException

  override def getChild(ordinal: Int): ColumnVector = throw new UnsupportedOperationException
}
