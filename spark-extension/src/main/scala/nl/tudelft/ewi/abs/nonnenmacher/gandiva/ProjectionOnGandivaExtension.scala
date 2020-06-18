package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSessionExtensions, Strategy}


object ProjectionOnGandivaExtension extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectPlannerStrategy(_ => ProjectionOnGandivaStrategy)
  }

  object ProjectionOnGandivaStrategy extends Strategy {

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case logical.Project(projectList, child) => Seq(GandivaProjectExec(projectList, planLater(child)))
        case logical.Filter(condition, child) => Seq(GandivaFilterExec(condition, planLater(child)))
        case _ => Nil
      }
    }
  }
}
