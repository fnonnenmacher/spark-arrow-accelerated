package org.apache.spark.sql

import org.apache.arrow.vector.FieldVector
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarArray, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Spark's [[ArrowColumnVector]] implementation unfortunately does not allow to access the internal arrow [[FieldVector]]
 * which stores all necessary data in the arrow format. Currently this explained because this class is only used to convert
 * arrow field vectors into Spark rows. Not the other way around as I need to do it.
 *
 * Therefore, this class is a small wrapper around Spark's implementation it provides an accessor for the fieldVector and
 * delegates all other calls to the Spark Implementation.
 *
 * @param vector the apache arrow field vector
 */
case class ArrowColumnVectorWithFieldVector(vector: FieldVector) extends ColumnVector(ArrowUtils.fromArrowField(vector.getField)) {

  private val delegate: ArrowColumnVector = new ArrowColumnVector(vector)

  override def close(): Unit = delegate.close()

  override def hasNull: Boolean = delegate.hasNull

  override def numNulls(): Int = delegate.numNulls()

  override def isNullAt(rowId: Int): Boolean = delegate.isNullAt(rowId)

  override def getBoolean(rowId: Int): Boolean = delegate.getBoolean(rowId)

  override def getByte(rowId: Int): Byte = delegate.getByte(rowId)

  override def getShort(rowId: Int): Short = delegate.getShort(rowId)

  override def getInt(rowId: Int): Int = delegate.getInt(rowId)

  override def getLong(rowId: Int): Long = delegate.getLong(rowId)

  override def getFloat(rowId: Int): Float = delegate.getFloat(rowId)

  override def getDouble(rowId: Int): Double = delegate.getDouble(rowId)

  override def getArray(rowId: Int): ColumnarArray = delegate.getArray(rowId)

  override def getMap(ordinal: Int): ColumnarMap = delegate.getMap(ordinal)

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = delegate.getDecimal(rowId, precision, scale)

  override def getUTF8String(rowId: Int): UTF8String = delegate.getUTF8String(rowId)

  override def getBinary(rowId: Int): Array[Byte] = delegate.getBinary(rowId)

  override def getChild(ordinal: Int): ColumnVector = delegate.getChild(ordinal)
}
