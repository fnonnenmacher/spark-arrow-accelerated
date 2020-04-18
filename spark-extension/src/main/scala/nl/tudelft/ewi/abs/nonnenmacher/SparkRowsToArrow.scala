package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Field.nullable
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.arrow.ArrowUtils

import scala.collection.JavaConverters._

object SparkRowsToArrow {

  import org.apache.arrow.vector.types.pojo.ArrowType

  private val ArrowTypeLong = new ArrowType.Int(64, true)
  private val ArrowTypeInteger = new ArrowType.Int(32, true)

  def nullableInt(name: String):Field = {
    nullable(name, ArrowTypeInteger)
  }

  def convert(iterator: Iterator[InternalRow], fields: Seq[Field]): VectorSchemaRoot = {
    // expected arrow schema, Attention no validation if compatible with rows atm
    val schema = new Schema(fields.asJava)

    val root = VectorSchemaRoot.create(schema, ArrowUtils.rootAllocator)

    // writes all rows into vector schema root
    val writer = execution.arrow.ArrowWriter.create(root)
    iterator.foreach(writer.write)
    writer.finish

    root;
  }
}
