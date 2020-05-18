package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import org.apache.arrow.gandiva.evaluator.SelectionVector
import org.apache.arrow.vector.FieldVector
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarArray, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String


case class ArrowColumnVectorWithAccessibleFieldVectorAndSelectionVec(selectionVector: SelectionVector, vector: FieldVector)
  extends ColumnVector(ArrowUtils.fromArrowField(vector.getField)) {

  private val delegate: ArrowColumnVector = new ArrowColumnVector(vector)

  private def internalRowId(rowId: Int): Int = {
      selectionVector.getIndex(rowId)
  }

  override def close(): Unit = {
    selectionVector.getBuffer.close()
    delegate.close()
  }

  override def hasNull: Boolean = delegate.hasNull //TODO

  override def numNulls(): Int = delegate.numNulls() //TODO

  override def isNullAt(rowId: Int): Boolean = delegate.isNullAt(internalRowId(rowId))

  override def getBoolean(rowId: Int): Boolean = delegate.getBoolean(internalRowId(rowId))

  override def getByte(rowId: Int): Byte = delegate.getByte(internalRowId(rowId))

  override def getShort(rowId: Int): Short = delegate.getShort(internalRowId(rowId))

  override def getInt(rowId: Int): Int = delegate.getInt(internalRowId(rowId))

  override def getLong(rowId: Int): Long = delegate.getLong(internalRowId(rowId))

  override def getFloat(rowId: Int): Float = delegate.getFloat(internalRowId(rowId))

  override def getDouble(rowId: Int): Double = delegate.getDouble(internalRowId(rowId))

  override def getArray(rowId: Int): ColumnarArray = delegate.getArray(internalRowId(rowId))

  override def getMap(ordinal: Int): ColumnarMap = delegate.getMap(ordinal)

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = delegate.getDecimal(internalRowId(rowId), precision, scale)

  override def getUTF8String(rowId: Int): UTF8String = delegate.getUTF8String(internalRowId(rowId))

  override def getBinary(rowId: Int): Array[Byte] = delegate.getBinary(internalRowId(rowId))

  override def getChild(ordinal: Int): ColumnVector = throw new IllegalAccessException("No child ColumnVector supported.") //TODO
}
