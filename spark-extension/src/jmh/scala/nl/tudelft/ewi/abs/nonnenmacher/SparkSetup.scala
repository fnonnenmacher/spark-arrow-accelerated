package nl.tudelft.ewi.abs.nonnenmacher

import java.io.{FileWriter, PrintWriter}

import nl.tudelft.ewi.abs.nonnenmacher.gandiva.{GandivaFilterExec, GandivaProjectExec, ProjectionOnGandivaExtension}
import nl.tudelft.ewi.abs.nonnenmacher.parquet.NativeParquetSourceScanExec
import org.apache.spark.sql.execution.datasources.{NativeParquetReaderExtension, NativeParquetReaderStrategy}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{ArrowMemoryToSparkMemoryExtension, SparkSessionExtensions, _}
import org.openjdk.jmh.infra.BenchmarkParams

import scala.collection.mutable
import scala.collection.JavaConverters._

object SparkSetup {

  final val MILLION: Int = 1000000

  //It is with JMH easier to use predefined strings, then a real enum
  final val PLAIN = "PLAIN"
  final val PARQUET_ONLY = "PARQUET_ONLY"
  final val PARQUET_AND_GANDIVA = "PARQUET_AND_GANDIVA"
  final val WITH_MAX_AGGREGATION = "WITH_MAX_AGGREGATION"
  final val PARQUET_AND_MEMORY_CONVERSION = "PARQUET_AND_MEMORY_CONVERSION"

  private def extensionOf(s: String): Seq[SparkSessionExtensions => Unit] = s match {
    case PLAIN => Seq(MeasureColumnarProcessingExtension)
    case PARQUET_ONLY =>
      Seq(NativeParquetReaderExtension(), MeasureColumnarProcessingExtension)
    case PARQUET_AND_GANDIVA =>
      Seq(NativeParquetReaderExtension(true),
        ProjectionOnGandivaExtension,
        ArrowColumnarExtension)
    case WITH_MAX_AGGREGATION =>
      Seq(NativeParquetReaderExtension(true),
        ProjectionOnGandivaExtension,
        DirtyMaxAggregationExtension)
    case PARQUET_AND_MEMORY_CONVERSION =>
      Seq(NativeParquetReaderExtension(), ArrowMemoryToSparkMemoryExtension)
    case _ => throw new IllegalArgumentException(s"Spark configuration $s not defined!")
  }

  val metrics: mutable.MutableList[Seq[Long]] = mutable.MutableList()

  def initSpark(sparkConfigName: String, batchSize: Int = 10000, codegen: Boolean = true): SparkSession = {

    metrics.clear()

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
      //.config("spark.sql.parquet.filterPushdown", false)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.sqlContext.listenerManager.register(new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {

        var scanTime = 0L;
        var gandiva = 0L;
        var aggregationTime = 0L;
        var processing = 0L;

        qe.executedPlan.foreach {
          case fs@FileSourceScanExec(_, _, _, _, _, _, _) => fs.metrics.get("scanTime").foreach(m => scanTime = m.value)
          case ns@NativeParquetSourceScanExec(_, _, _, _, _, _, _) => ns.metrics.get("scanTime").foreach(m => scanTime = (m.value / MILLION))
          case g@GandivaFilterExec(_, _) => g.metrics.get("time").foreach(m => gandiva += m.value / MILLION)
          case g@GandivaProjectExec(_, _) => g.metrics.get("time").foreach(m => gandiva += m.value / MILLION)
          case g@ColumnarToRowMaxAggregatorExec(_) => {
            g.metrics.get("aggregationTime").foreach(m => aggregationTime = m.value / MILLION)
            g.metrics.get("processing").foreach(m => processing = m.value / MILLION)
          }
          case a@org.apache.spark.sql.ArrowMemoryToSparkMemoryExec(_) => {
            a.metrics.get("conversionTime").foreach(m => processing = m.value / MILLION)
          }
          case a@org.apache.spark.sql.MeasureColumnarProcessingExec(_) => {
            a.metrics.get("columnarProcessing").foreach(m => processing = m.value / MILLION)
          }
          case _ =>
        }

        metrics += Seq[Long](scanTime, gandiva, aggregationTime, processing, durationNs / MILLION)

      }

      override def onFailure(funcName: String, qe: QueryExecution, error: Throwable): Unit = {}
    })

    spark
  }

  lazy val rootDir: String = System.getProperty("project.root")

  lazy val metricsResultWriter: PrintWriter = {

    val metricsResultFile: String = System.getProperty("output.metrics")
    if (metricsResultFile == null)
      throw new IllegalArgumentException("System property \"output.metrics\" not set!")

    new PrintWriter(new FileWriter(metricsResultFile, true))
  }

  lazy val metricsRawResultWriter: PrintWriter = {

    val metricsResultFile: String = System.getProperty("output.metrics.raw")
    if (metricsResultFile == null)
      throw new IllegalArgumentException("System property \"output.metrics.raw\" not set!")

    val writer = new PrintWriter(new FileWriter(metricsResultFile, true))

    writer
  }

  def writeResults(benchmarkParams: BenchmarkParams): Unit = {

    val name = benchmarkParams.getBenchmark.split('.').last
    val params = benchmarkParams.getParamsKeys.asScala.toList.sorted.map(benchmarkParams.getParam)
    val warmupIterations = benchmarkParams.getWarmup.getCount

    //write raw results
    metricsRawResultWriter.write(s"\n$name - ${params.mkString(", ")}:\n")
    metricsRawResultWriter.write("\nIndex;scanTime;gandiva;aggregationTime;processing;total\n")

    metrics.zipWithIndex.foreach { case (m, i) =>
      metricsRawResultWriter.write(s"$i;${m.mkString(";")}\n")
    }
    metricsRawResultWriter.flush()

    //write avg values of metrics
    val measurement = metrics.drop(warmupIterations)

    val measurementIterations = measurement.size
    val m = measurement.reduce { (m1: Seq[Long], m2: Seq[Long]) =>
      val r: Seq[Long] = m1.zip(m2).map(e => e._1 + e._2)
      r
    }.map(_.toFloat / measurementIterations)
    metricsResultWriter.write(s"$name;${params.mkString(";")};${m.mkString(";")}\n".replace(".",","))
    metricsResultWriter.flush()
  }
}