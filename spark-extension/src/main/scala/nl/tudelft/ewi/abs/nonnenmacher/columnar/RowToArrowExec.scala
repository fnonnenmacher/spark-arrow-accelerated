package nl.tudelft.ewi.abs.nonnenmacher.columnar

import nl.tudelft.ewi.abs.nonnenmacher.columnar.ArrowColumnarConverters._
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkArrowUtils
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

class RowToArrowExec(override val child: SparkPlan) extends RowToColumnarExec(child) {

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val maxRecordsPerBatch = conf.columnBatchSize

    child.execute().mapPartitions { rowIter =>

      val allocator =
        SparkArrowUtils.rootAllocator.newChildAllocator(s"${this.getClass.getSimpleName}", 0, Long.MaxValue)

      val arrowSchema = SparkArrowUtils.toArrowSchema(schema, conf.sessionLocalTimeZone)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      val arrowWriter = ArrowWriter.create(root)

      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        root.close()
        allocator.close()
      }

      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = rowIter.hasNext || {
          root.close()
          allocator.close()
          false
        }

        override def next(): ColumnarBatch = {

          //reset previously set field vectors
          arrowWriter.reset();

          var rowCount = 0
          while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
            val row = rowIter.next()
            arrowWriter.write(row)
            rowCount += 1
          }
          arrowWriter.finish()

          root.setRowCount(rowCount)
          root.toBatch
        }
      }
    }
  }

  // We have to override equals because subclassing a case class like RowToColumnarExec is not that clean
  // One of the issues is that the generated equals will see RowToColumnarExec and RowToArrowColumnarExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[RowToArrowExec]
  }

  override def hashCode(): Int = super.hashCode()
}
