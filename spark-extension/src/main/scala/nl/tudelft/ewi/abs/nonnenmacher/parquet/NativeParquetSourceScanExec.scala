package nl.tudelft.ewi.abs.nonnenmacher.parquet

import nl.tudelft.ewi.abs.nonnenmacher.NativeParquetReader
import nl.tudelft.ewi.abs.nonnenmacher.utils.StartStopMeasurment
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.ColumnarBatchWrapper
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection.BitSet
import org.apache.spark.{Partition, TaskContext}

case class NativeParquetSourceScanExec(@transient relation: HadoopFsRelation,
                                       outputs: Seq[Attribute],
                                       requiredSchema: StructType,
                                       partitionFilters: Seq[Expression],
                                       optionalBucketSet: Option[BitSet],
                                       dataFilters: Seq[Expression],
                                       tableIdentifier: Option[TableIdentifier])
//  extends FileSourceScanExec(relation, outputs, requiredSchema, partitionFilters, optionalBucketSet, dataFilters, tableIdentifier) {
  extends LeafExecNode {

  override lazy val supportsColumnar: Boolean = true;

  private val dataSchema = relation.dataSchema

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val maxRecordsPerBatch = conf.columnBatchSize
    val time = longMetric("time")

    println("maxRecordsPerBatch: " + maxRecordsPerBatch)

    new RDD[ColumnarBatch](relation.sparkSession.sparkContext, Nil) {

      val fileName: String = relation.location.inputFiles.head.replaceFirst("file://", "")

      override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {

        val inputSchema: Schema = ArrowUtils.toArrowSchema(dataSchema, null)
        val schema: Schema = ArrowUtils.toArrowSchema(requiredSchema, null) //TODO read from properties

        //TODO: currently only 1 file supported!
        new Iterator[ColumnarBatch] with StartStopMeasurment {

          val innerIter: Iterator[ColumnarBatch] = new NativeParquetReader(fileName, inputSchema, schema, maxRecordsPerBatch) //TODO read from properties
            .map(ColumnarBatchWrapper(_).toColumnarBatch)

          override def hasNext: Boolean = {
            start()
            val r = innerIter.hasNext
            time += stop()
            r
          }

          override def next(): ColumnarBatch = {
            start()
            val r = innerIter.next()
            time += stop()
            r
          }
        }
      }

      override protected def getPartitions: Array[Partition] = Array(new Partition {
        override def index: Int = 0
      })
    }
  }

//  /** HACK; because overriding a case class is normally not a good idea. */
//  override def equals(other: Any): Boolean = {
//    if (!super.equals(other)) {
//      return false
//    }
//    other.isInstanceOf[NativeParquetSourceScanExec]
//  }

  override lazy val metrics = Map("time" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time in [ns]"))

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  override def output: Seq[Attribute] = outputs
}