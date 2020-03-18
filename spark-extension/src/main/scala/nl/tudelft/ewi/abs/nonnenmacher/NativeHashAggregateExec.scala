package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

class NativeHashAggregateExec(
                                 override val requiredChildDistributionExpressions: Option[Seq[Expression]],
                                 override val groupingExpressions: Seq[NamedExpression],
                                 override val aggregateExpressions: Seq[AggregateExpression],
                                 override val aggregateAttributes: Seq[Attribute],
                                 override val initialInputBufferOffset: Int,
                                 override val resultExpressions: Seq[NamedExpression],
                                 override val child: SparkPlan) extends HashAggregateExec(requiredChildDistributionExpressions, groupingExpressions, aggregateExpressions, aggregateAttributes, initialInputBufferOffset, resultExpressions, child) {



  protected override def doProduce(ctx: CodegenContext): String = {

    println("CALLED: DoProduce:")
    val res = super.doProduce(ctx)
    println(res)

    val doAgg = ctx.freshName("native-apache-arrow-sum")
    val doAggFuncName = ctx.addNewFunction(doAgg,
      s"""
         |private void $doAgg() throws java.io.IOException {
         |    agg_bufIsNull_0 = false;
         |    agg_bufValue_0 = 0;
         |
         |    org.apache.arrow.vector.VectorSchemaRoot root = nl.tudelft.nonnenmacher.SparkRowsToArrow.convert(inputadapter_input_0, partitionIndex);
         |    Long res = nl.tudelft.nonnenmacher.ArrowProcessor.sum(root);
         |    agg_bufValue_0 = res;
         |}
       """.stripMargin)

    //DIRTY Solution to get my function called instead of the original one
    //TODO: rewrite the whole code generation
    res.replace("agg_doAggregateWithoutKey_0", doAggFuncName)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    super.doConsume(ctx, input, row)
  }
}
