package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.gandiva.ProjectionOnGandivaExtension
import org.apache.spark.sql.execution.datasources.NativeParquetReaderStrategy
import org.apache.spark.sql.{ArrowColumnarExtension, SparkSession, SparkSessionExtensions}

object SparkSetup {

  //It is with JMH easier to use predefined strings, then a real enum
  final val PLAIN = "PLAIN"
  final val PARQUET_ONLY = "PARQUET_ONLY"
  final val PARQUET_AND_GANDIVA = "PARQUET_AND_GANDIVA"

  private def extensionOf(s: String): Seq[SparkSessionExtensions => Unit] = s match {
    case PLAIN => Seq()
    case PARQUET_ONLY =>
      Seq(_.injectPlannerStrategy(x => NativeParquetReaderStrategy()), ArrowColumnarExtension())
    case PARQUET_AND_GANDIVA =>
      Seq(_.injectPlannerStrategy(x => NativeParquetReaderStrategy(true)),
        ProjectionOnGandivaExtension(),
        ArrowColumnarExtension())
    case _ => throw new IllegalArgumentException(s"Spark configuration $s not defined!")
  }

  def initSpark(sparkConfigName: String, batchSize:Int =10000): SparkSession = {

    val activeSession = SparkSession.getActiveSession
    if (activeSession.isDefined) activeSession.get.close()

    val builder = SparkSession
      .builder();

    extensionOf(sparkConfigName).foreach { extension =>
      builder.withExtensions(extension)
    }

    builder.appName(sparkConfigName)
      .config("spark.master", "local")
      .config("spark.sql.codegen.wholeStage", false)
      .config("spark.sql.inMemoryColumnarStorage.batchSize", batchSize)
      .getOrCreate()
  }

  lazy val rootDir: String = System.getProperty("project.root")
}