package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Field.nullable
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.arrow.ArrowUtils

import scala.collection.JavaConverters._

object SparkRowsToArrow {

  import org.apache.arrow.vector.types.pojo.ArrowType

  val schema = new Schema(List(nullable("value", new ArrowType.Int(64, true))).asJava)

  def convert(iterator: Iterator[InternalRow], partitionId: Int): VectorSchemaRoot = {

    val root = VectorSchemaRoot.create(schema, ArrowUtils.rootAllocator)

    val writer = execution.arrow.ArrowWriter.create(root)
    iterator.foreach(writer.write)
    writer.finish

    root;
  }
}
