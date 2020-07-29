package nl.tudelft.ewi.abs.nonnenmacher.max.aggregation

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Max}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.{ColumnarRule, ColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.types.IntegerType

object SimpleMaxAggregationExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions) {
    e.injectColumnar(_ => SimpleMaxAggregationRule)
  }

  object SimpleMaxAggregationRule extends ColumnarRule {

    override def postColumnarTransitions: Rule[SparkPlan] = {
      case plan@HashAggregateExec(requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      ColumnarToRowExec(child)) => {

        if (isApplicable(requiredChildDistributionExpressions,
          groupingExpressions,
          aggregateExpressions,
          aggregateAttributes,
          initialInputBufferOffset,
          resultExpressions)) {

          plan.withNewChildren(Seq(SimpleMaxAggregationExec(child)))
        } else {
          plan.withNewChildren(plan.children.map(postColumnarTransitions(_)))
        }
      }
      case plan => plan.withNewChildren(plan.children.map(postColumnarTransitions(_)))
    }

    /**
     * This max aggregation is only applicable for very simple cases.
     * This function evaluates if the aggregation is simple enough to apply it
     */
    def isApplicable(requiredChildDistributionExpressions: Option[Seq[Expression]],
                     groupingExpressions: Seq[NamedExpression],
                     aggregateExpressions: Seq[AggregateExpression],
                     aggregateAttributes: Seq[Attribute],
                     initialInputBufferOffset: Int,
                     resultExpressions: Seq[NamedExpression]): Boolean = {
//
//      if (requiredChildDistributionExpressions.isDefined) return false
//      if (groupingExpressions.nonEmpty) return false
//      if (aggregateExpressions.size != 1) return false
//      if (aggregateAttributes.size != 1) return false
//      if (resultExpressions.size != 1) return false
//      if (initialInputBufferOffset != 0) return false
//
//      val aggregateFunction = aggregateExpressions.head.aggregateFunction
//
//      if (!aggregateFunction.isInstanceOf[Max]) return false
//      if (!aggregateFunction.asInstanceOf[Max].child.isInstanceOf[AttributeReference]) return false
//      aggregateFunction.asInstanceOf[Max].child.asInstanceOf[AttributeReference].dataType.isInstanceOf[IntegerType]

      true
    }
  }

}