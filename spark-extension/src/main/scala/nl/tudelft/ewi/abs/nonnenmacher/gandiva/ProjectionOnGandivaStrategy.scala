package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.{SparkSessionExtensions, Strategy}


object ProjectionOnGandivaExtension {
  def apply(): (SparkSessionExtensions => Unit) = { e: SparkSessionExtensions =>
    e.injectPlannerStrategy(_ => new ProjectionOnGandivaStrategy())
  }
}

class ProjectionOnGandivaStrategy() extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    println("HERE: " + plan.getClass)
    plan match {
      case logical.Project(projectList, child) => Seq( GandivaProjectExec(projectList, planLater(child)))
      case logical.Filter(condition, child) => Seq( GandivaFilterExec(condition, planLater(child)))
      case _ => Nil
    }
  }
}