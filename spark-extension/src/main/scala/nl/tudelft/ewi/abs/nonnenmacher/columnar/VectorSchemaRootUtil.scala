package nl.tudelft.ewi.abs.nonnenmacher.columnar

import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}

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

    val matches = columnVectors.forall(_.isInstanceOf[ArrowColumnVector])
    if (!matches) return Option.empty

    val fieldVectors = columnVectors.map(_.asInstanceOf[ArrowColumnVector]).map(extractVector).asJava
    Option.apply(new VectorSchemaRoot(fieldVectors))
  }

  def toBatch(root: VectorSchemaRoot): ColumnarBatch = {
    val columnVectors: Array[ColumnVector] = root
      .getFieldVectors
      .asScala
      .map(new ArrowColumnVector(_))
      .toArray
    new ColumnarBatch(columnVectors, root.getRowCount)
  }

  private lazy val accessorField = {
    val accessorField = classOf[ArrowColumnVector].getDeclaredField("accessor")
    accessorField.setAccessible(true)
    accessorField
  }

  private lazy val vectorField = {
    classOf[ArrowColumnVector]
      .getDeclaredClasses
      .foreach(c => println(c.getSimpleName))

    val accessorClass: Class[_] = classOf[ArrowColumnVector].getDeclaredClasses.find(_.getSimpleName == "ArrowVectorAccessor")
      .getOrElse(throw new Exception("Cannot access class 'org.apache.spark.sql.vectorized.ArrowColumnVector.ArrowVectorAccessor' by reflection."))
    val vectorField = accessorClass.getDeclaredField("vector");
    vectorField.setAccessible(true)
    vectorField
  }

  /**
   * extracts field vector of [[ArrowColumnVector]]. Unfortunately the field vector is private and not accesible,
   * therefore we use reflection
   */
  def extractVector(arrowColumnVector: ArrowColumnVector): FieldVector = {
    val accessor = accessorField.get(arrowColumnVector)
    vectorField.get(accessor).asInstanceOf[FieldVector]
  }
}
