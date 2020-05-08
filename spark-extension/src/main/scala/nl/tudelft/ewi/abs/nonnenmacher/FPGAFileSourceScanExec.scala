package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{RowToArrowColumnarExec, SparkSession, vectorized}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFsRelation, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.util.collection.BitSet

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

    //TODO: currently only 1 file supported!
    val fileName = relation.location.inputFiles.head.replaceFirst("file://","")
    val fieldIndices: Array[Int]= requiredSchema.fieldNames.map(relation.dataSchema.fieldIndex);

    new RDD[ColumnarBatch](relation.sparkSession.sparkContext, Nil) {
      override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {

        ArrowProcessor.readParquet(fileName, fieldIndices).map(toResultBatch)
      }

      override protected def getPartitions: Array[Partition] = Array(new Partition {override def index: Int = 0})
    }
  }

  private def toResultBatch(root: VectorSchemaRoot): ColumnarBatch = {
    // only copy vectors which are really relevant.
    val arrowVectors: Array[ColumnVector] = root.getFieldVectors.asScala.map(x => new vectorized.ArrowColumnVector(x)).toArray;
    new ColumnarBatch(arrowVectors, root.getRowCount)
  }

  /** HACK; because overriding a case class is normally not a good idea. */
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[FPGAFileSourceScanExec]
  }
}