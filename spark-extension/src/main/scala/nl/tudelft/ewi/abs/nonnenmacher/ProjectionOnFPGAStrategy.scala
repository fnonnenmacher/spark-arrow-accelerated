package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.{SparkSessionExtensions, Strategy}


object ProjectionOnFPGAExtension {
  def apply(modules: FPGAModule*): (SparkSessionExtensions => Unit) = { e: SparkSessionExtensions =>
    e.injectPlannerStrategy(_ => new ProjectionOnFPGAStrategy(modules))
  }
}

class ProjectionOnFPGAStrategy(modules: Seq[FPGAModule]) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case logical.Project(projectList, child) => findFPGAEnabledSubtree(projectList, child)
      case _ => Nil
    }
  }

  def findFPGAEnabledSubtree(projectList: Seq[NamedExpression], child: LogicalPlan): Seq[SparkPlan] = {
    if (projectList.size > 1) { // currently only works with one projection
      return Nil
    }

    if (modules.isEmpty) { //no fpga module defined
      return Nil
    }

    //TODO: Loop
    val fpgaModule = modules.head

    val projectionExpression = projectList.head

    val optCandidate = fpgaModule.findBestReplacementCandidate(projectionExpression)

    if (optCandidate.isEmpty) { //no candidate found
      return Nil
    }

    val (replacementExpr, idmapping) = optCandidate.get


    val preFPGAExpressions = idmapping.map.map { case (name, expr) => Alias(expr, name)() }.toSeq
    val preProjection = planLater(Project(preFPGAExpressions, child))


    val fpgaProjectExec: FPGAProjectExec = FPGAProjectExec(preProjection, fpgaModule)

    val postProjectionExpression = replace(projectionExpression, replacementExpr, fpgaProjectExec.output.head)

    ProjectExec(postProjectionExpression.asInstanceOf[NamedExpression] :: Nil, fpgaProjectExec) :: Nil
  }

  def replace(expr: Expression, toReplace: Expression, replaceWith: Expression): Expression = {

    expr.transformDown(new PartialFunction[Expression, Expression] {
      override def isDefinedAt(x: Expression): Boolean = x fastEquals toReplace

      override def apply(v1: Expression): Expression = replaceWith

    })
  }
}