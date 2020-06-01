package nl.tudelft.ewi.abs.nonnenmacher

import java.io.{FileWriter, PrintWriter}

import nl.tudelft.ewi.abs.nonnenmacher.gandiva.{GandivaFilterExec, GandivaProjectExec, ProjectionOnGandivaExtension}
import nl.tudelft.ewi.abs.nonnenmacher.parquet.NativeParquetSourceScanExec
import org.apache.spark.sql.execution.datasources.NativeParquetReaderStrategy
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{ArrowColumnarExtension, SparkSession, SparkSessionExtensions}

object SparkSetup {

  //It is with JMH easier to use predefined strings, then a real enum
  final val PLAIN = "PLAIN"
  final val PARQUET_ONLY = "PARQUET_ONLY"
  final val PARQUET_AND_GANDIVA = "PARQUET_AND_GANDIVA"

  private def extensionOf(s: String): Seq[SparkSessionExtensions => Unit] = s match {
    case PLAIN => Seq()
    case PARQUET_ONLY =>
      Seq(_.injectPlannerStrategy(x => NativeParquetReaderStrategy()))
    case PARQUET_AND_GANDIVA =>
      Seq(_.injectPlannerStrategy(x => NativeParquetReaderStrategy(true)),
        ProjectionOnGandivaExtension(),
        ArrowColumnarExtension())
    case _ => throw new IllegalArgumentException(s"Spark configuration $s not defined!")
  }

  def initSpark(sparkConfigName: String, batchSize: Int = 10000, codegen: Boolean = false): SparkSession = {

    val activeSession = SparkSession.getActiveSession
    if (activeSession.isDefined) activeSession.get.close()

    val builder = SparkSession
      .builder();

    extensionOf(sparkConfigName).foreach { extension =>
      builder.withExtensions(extension)
    }

    val spark = builder.appName(sparkConfigName)
      .config("spark.master", "local")
      .config("spark.sql.codegen.wholeStage", codegen)
      .config("spark.sql.inMemoryColumnarStorage.batchSize", batchSize)
      .config("spark.sql.parquet.filterPushdown", false)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.sqlContext.listenerManager.register(new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {

        var scanTime = 0L;
        var gandiva = 0L;

        qe.executedPlan.foreach {
          case fs@FileSourceScanExec(_, _, _, _, _, _, _) => fs.metrics.get("scanTime").foreach(m => scanTime = m.value)
          case ns@NativeParquetSourceScanExec(_, _, _, _, _, _, _) => ns.metrics.get("scanTime").foreach(m => scanTime = (m.value / 1e6).toLong)
          case g@GandivaFilterExec(_, _) => g.metrics.get("time").foreach(m => gandiva += (m.value / 1e6).toLong)
          case g@GandivaProjectExec(_, _) => g.metrics.get("time").foreach(m => gandiva += (m.value / 1e6).toLong)
          case _ =>
        }

        metricsResultWriter.write(s"${qe.sparkSession.sparkContext.appName};$scanTime;$gandiva;${(durationNs / 1e6).toLong}\n")
        metricsResultWriter.flush()
      }

      override def onFailure(funcName: String, qe: QueryExecution, error: Throwable): Unit = {}
    })

    spark
  }

  lazy val rootDir: String = System.getProperty("project.root")

  val metricsResultWriter: PrintWriter = {

    val metricsResultFile: String = System.getProperty("output.metrics")
    if (metricsResultFile == null)
      throw new IllegalArgumentException("System property \"output.metrics\" not set!")

    val writer = new PrintWriter(new FileWriter(metricsResultFile, true))
    writer.write("\nCONFIG;scanTime;gandiva;total\n")
    writer.flush()
    writer
  }
}