package org.apache.spark.sql

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ArrowUtils

/**
 * Wraps [[org.apache.spark.sql.util.ArrowUtils]] to make private fields accessible.
 * These methods have been accessible in beta version of Spark 3.0 and therefore is my implementation based on them.
 */
object SparkArrowUtils {
  def fromArrowField(field: Field): DataType = ArrowUtils.fromArrowField(field)

  val rootAllocator: RootAllocator = ArrowUtils.rootAllocator

  def toArrowType(dt: DataType, timeZoneId: String): ArrowType = ArrowUtils.toArrowType(dt, timeZoneId)

  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = ArrowUtils.toArrowSchema(schema, timeZoneId)

  def toArrowField(name: String, dt: DataType, nullable: Boolean, timeZoneId: String): Field
  = ArrowUtils.toArrowField(name, dt, nullable, timeZoneId)
}
