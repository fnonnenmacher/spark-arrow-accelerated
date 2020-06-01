package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.gandiva.{GandivaFilterExec, GandivaProjectExec, ProjectionOnGandivaExtension}
import nl.tudelft.ewi.abs.nonnenmacher.parquet.NativeParquetSourceScanExec
import org.apache.spark.sql.execution.datasources.NativeParquetReaderStrategy
import org.apache.spark.sql.{ArrowColumnarExtension, DataFrame, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParquetAndGandivaSuite extends FunSuite with SparkSessionGenerator {

  override def withExtensions: Seq[SparkSessionExtensions => Unit] =
    Seq(_.injectPlannerStrategy(x => NativeParquetReaderStrategy(true)),
      ProjectionOnGandivaExtension(),
      ArrowColumnarExtension())

  ignore("read from parquet format") {

    spark.conf.set("spark.sql.codegen.wholeStage", false)

    val sqlDF: DataFrame = spark.sql("SELECT `string-field`, `int-field` *2 FROM parquet.`../data/example.parquet` WHERE `long-field`>2 OR `long-field` < 0")

    sqlDF.printSchema()
    println("Direct Plan:")
    println(sqlDF.queryExecution)
    println("Logical Plan:")
    println(sqlDF.queryExecution.optimizedPlan)
    println("Spark Plan:")
    println(sqlDF.queryExecution.sparkPlan)
    println("Executed Plan:")
    println(sqlDF.queryExecution.executedPlan)

    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[NativeParquetSourceScanExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[GandivaFilterExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[GandivaProjectExec]).isDefined)

    val res = sqlDF.collect().map(r => (r(0), r(1))).toList

    assert(res.size == 3)
    assert(res.contains(("number-2", 4)))
    assert(res.contains(("number-3", 6)))
    assert(res.contains(("number-4", 8)))
  }

  ignore("dremio1") {

    val sqlDF = spark.sql(s"SELECT `x` + `N2x` + `N3x` AS sum FROM parquet.`../data/5million-int-triples.parquet`")
      .agg("sum" -> "max")


    println("Executed Plan:")
    println(sqlDF.queryExecution.executedPlan)

    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[NativeParquetSourceScanExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[GandivaProjectExec]).isDefined)

    println(sqlDF.collect.head)

    Thread.sleep(10 * 60 * 1000)
  }

  test("dremio2") {

    spark.conf.set("spark.sql.codegen.wholeStage", false)

    val sqlDF = spark.sql("SELECT" +
      " `x` + `N2x` + `N3x` AS s1," +
      " `x` * `N2x` - `N3x` AS s2," +
      " 3 * `x` + 2* `N2x` + `N3x` AS s3," +
      " `x` >= `N2x` - `N3x` AS c1," +
      " `x` +  `N2x` = `N3x` AS c2 " +
      "FROM parquet.`../data/5million-int-triples.parquet`")
      .agg("s1" -> "sum",
        "s2" -> "sum",
        "s3" -> "sum",
        "c1" -> "count",
        "c2" -> "count",
      )

    println("Executed Plan:")
    println(sqlDF.queryExecution.executedPlan)

    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[NativeParquetSourceScanExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[GandivaProjectExec]).isDefined)

    println(sqlDF.collect.head)
  }

  ignore("maxOfSumOf10Ints") {
    val sqlDF = spark.sql(s"SELECT `x1` + `x2` + `x3` + `x4` + `x5` + `x6` + `x7` + `x8` + `x9` + `x10` AS sum " +
      s"FROM parquet.`../data/million-times-10-ints.parquet` " +
      "WHERE `x1` < `x2` AND `x3` < `x4`")
      .agg("sum" -> "max")

    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[NativeParquetSourceScanExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[GandivaFilterExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[GandivaProjectExec]).isDefined)

    println("Executed Plan:")
    println(sqlDF.queryExecution.executedPlan)

    val max = sqlDF.collect.head
    println("MAX: " + max)
  }
}