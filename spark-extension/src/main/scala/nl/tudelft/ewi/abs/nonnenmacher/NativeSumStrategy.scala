package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Final, Partial}
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

object NativeSumStrategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    println(plan.getClass)

    plan match {
      case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child) => {
        println("PHYSICAL_AGGREGATE:")
        println("groupingExpressions:" + groupingExpressions)
        println("aggregateExpressions:" + aggExpressions)
        println("resultExpressions:" + resultExpressions)
        println("child:" + child)

        val aggregateExpressions = aggExpressions.map(expr => expr.asInstanceOf[AggregateExpression])
        planAggregateWithoutDistinct(
          groupingExpressions,
          aggregateExpressions,
          resultExpressions,
          planLater(child))
      }
      case _ => Nil
    }
  }


  def planAggregateWithoutDistinct(
                                    groupingExpressions: Seq[NamedExpression],
                                    aggregateExpressions: Seq[AggregateExpression],
                                    resultExpressions: Seq[NamedExpression],
                                    child: SparkPlan): Seq[SparkPlan] = {
    // Check if we can use HashAggregate.

    // 1. Create an Aggregate Operator for partial aggregations.
    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val partialAggregateExpressions = aggregateExpressions.map(_.copy(mode = Partial))
    val partialAggregateAttributes =
      partialAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val partialResultExpressions =
      groupingAttributes ++
        partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

    val partialAggregate = new NativeHashAggregateExec(
      requiredChildDistributionExpressions = None,
      groupingExpressions = groupingExpressions,
      aggregateExpressions = partialAggregateExpressions,
      aggregateAttributes = partialAggregateAttributes,
      initialInputBufferOffset = 0,
      resultExpressions = partialResultExpressions,
      child = child)

    // 2. Create an Aggregate Operator for final aggregations.
    val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))
    // The attributes of the final aggregation buffer, which is presented as input to the result
    // projection:
    val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

    val finalAggregate = HashAggregateExec(
      requiredChildDistributionExpressions = Some(groupingAttributes),
      groupingExpressions = groupingAttributes,
      aggregateExpressions = finalAggregateExpressions,
      aggregateAttributes = finalAggregateAttributes,
      initialInputBufferOffset = groupingExpressions.length,
      resultExpressions = resultExpressions,
      child = partialAggregate)

    finalAggregate :: Nil
  }
}