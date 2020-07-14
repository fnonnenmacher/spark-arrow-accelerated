package nl.tudelft.ewi.abs.nonnenmacher.columnar

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

/**
 * Spark has added columnar processing support to version 3 See https://issues.apache.org/jira/browse/SPARK-27396 for details.
 *
 * Unfortunately, the team has decided to not use Apache Arrow, yet. However, this structure at least the opportunity to
 * override the existing memory mapping with an arrow based implementation.
 *
 */
object ArrowColumnarConversionRule extends ColumnarRule {
  override def postColumnarTransitions: Rule[SparkPlan] = ConvertToArrowColumnsRule();
}
