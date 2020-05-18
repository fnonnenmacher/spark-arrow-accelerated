package nl.tudelft.ewi.abs.nonnenmacher.parquet

import nl.tudelft.ewi.abs.nonnenmacher.JNIProcessorFactory
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.ColumnarBatchArrowConverter.VectorRootToColumnarBatch
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.{ColumnarBatchArrowConverter, vectorized}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.util.collection.BitSet
import org.apache.spark.{Partition, TaskContext}

import scala.collection.JavaConverters._

class FPGAFileSourceScanExec(@transient relation: HadoopFsRelation,
                             output: Seq[Attribute],
                             requiredSchema: StructType,
                             partitionFilters: Seq[Expression],
                             optionalBucketSet: Option[BitSet],
                             dataFilters: Seq[Expression],
                             override val tableIdentifier: Option[TableIdentifier])
  extends FileSourceScanExec(relation, output, requiredSchema, partitionFilters, optionalBucketSet, dataFilters, tableIdentifier) {


  override lazy val supportsColumnar: Boolean = true;

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {

    new RDD[ColumnarBatch](relation.sparkSession.sparkContext, Nil) {

      val fileName: String = relation.location.inputFiles.head.replaceFirst("file://", "")

      override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {

        //TODO: currently only 1 file supported!
        val schema: Schema = ArrowUtils.toArrowSchema(requiredSchema, null) //TODO read from properties

        JNIProcessorFactory
          .parquetReader(fileName, schema, 100) //TODO read from properties
          .map(VectorRootToColumnarBatch)
      }

      override protected def getPartitions: Array[Partition] = Array(new Partition {
        override def index: Int = 0
      })
    }
  }

  /** HACK; because overriding a case class is normally not a good idea. */
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[FPGAFileSourceScanExec]
  }
}