package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSessionGenerator extends BeforeAndAfterAll {
  this: Suite =>

  lazy val spark: SparkSession = {
    val builder = SparkSession
      .builder();

    withExtensions.foreach { extension =>
      builder.withExtensions(extension)
    }

    builder.appName(this.styleName)
      .config("spark.master", "local")
      .getOrCreate()
  }

  def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq()

  def assertArrowMemoryIsFreed(): Unit = {
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == 0)
    assert(GlobalAllocator.getAllocatedMemory == 0)
  }

  override def afterAll(): Unit = {
    spark.close()
  }
}
