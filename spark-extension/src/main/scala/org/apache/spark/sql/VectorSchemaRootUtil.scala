package org.apache.spark.sql

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ArrowColumnVectorWithFieldVector, ColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._

object VectorSchemaRootUtil {

  def from(columnarBatch: ColumnarBatch): VectorSchemaRoot = {
    columnarBatch match {
      case VectorSchemaRootUtil(root) => root
      case _ => throw new IllegalArgumentException("ColumnarBatch cannot be converted into a VectorSchemaRoot.")
    }
  }

  def unapply(columnarBatch: ColumnarBatch): Option[VectorSchemaRoot] = {
    val columnVectors = (0 until columnarBatch.numCols).map(columnarBatch.column)

    val matches = columnVectors.forall(_.isInstanceOf[ArrowColumnVectorWithFieldVector])
    if (!matches) return Option.empty

    val fieldVectors = columnVectors.map(_.asInstanceOf[ArrowColumnVectorWithFieldVector].getFieldVector).asJava
    Option.apply(new VectorSchemaRoot(fieldVectors))
  }

  def toBatch(root: VectorSchemaRoot): ColumnarBatch = {
    val columnVectors: Array[ColumnVector] = root
      .getFieldVectors
      .asScala
      .map(new ArrowColumnVectorWithFieldVector(_))
      .toArray
    new ColumnarBatch(columnVectors, root.getRowCount)
  }
}

