package nl.tudelft.ewi.abs.nonnenmacher.parquet

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

object ArrowParquetReaderExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions) {
    e.injectColumnar(_ => ArrowParquetReaderStrategy)
  }

  private object ArrowParquetReaderStrategy extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = { p: SparkPlan =>
      p.transformDown {
        case f@FileSourceScanExec(fsRelation, outputAttributes, outputSchema, _, _, _, _) =>
          if (fsRelation.fileFormat.isInstanceOf[ParquetFileFormat])
            ArrowParquetSourceScanExec(fsRelation, outputAttributes, outputSchema)
          else f
      }
    }
  }
}
