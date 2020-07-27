package nl.tudelft.ewi.abs.nonnenmacher.parquet

import nl.tudelft.ewi.abs.nonnenmacher.NativeParquetReader
import nl.tudelft.ewi.abs.nonnenmacher.columnar.ArrowColumnarConverters._
import nl.tudelft.ewi.abs.nonnenmacher.measuring.TimeMeasuringIterator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkArrowUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.apache.spark.{Partition, TaskContext}

case class ArrowParquetSourceScanExec(@transient relation: HadoopFsRelation,
                                      outputs: Seq[Attribute],
                                      requiredSchema: StructType)
  extends LeafExecNode {

  override lazy val supportsColumnar: Boolean = true;

  private val dataSchema = relation.dataSchema

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val maxRecordsPerBatch = conf.columnBatchSize
    val scanTime = longMetric("scanTime")

    new RDD[ColumnarBatch](relation.sparkSession.sparkContext, Nil) {

      val fileName: String = relation.location.inputFiles.head.replaceFirst("file://", "")

      override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {

        val inputSchema: Schema = SparkArrowUtils.toArrowSchema(dataSchema, conf.sessionLocalTimeZone)
        val schema: Schema = SparkArrowUtils.toArrowSchema(requiredSchema, conf.sessionLocalTimeZone)

        val parquetReader: Iterator[ColumnarBatch] = new NativeParquetReader(fileName, inputSchema, schema, maxRecordsPerBatch)
          .map(_.toBatch)

        new TimeMeasuringIterator(parquetReader, scanTime)
      }

      override protected def getPartitions: Array[Partition] = Array(new Partition {
        override def index: Int = 0
      })
    }
  }

  override lazy val metrics = Map("scanTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time in [ns]"))

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  override def output: Seq[Attribute] = outputs

  override def vectorTypes: Option[Seq[String]] = Option(Seq.fill(outputs.size)(classOf[ArrowColumnVector].getName))
}