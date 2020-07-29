package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, FilterExec, ProjectExec, SparkPlan}


object GandivaExtension extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectColumnar(_ => ProjectionOnGandivaStrategy)
  }

  private object ProjectionOnGandivaStrategy extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = { p: SparkPlan =>
      p.transformDown {
        case ProjectExec(projectList, child) => GandivaProjectExec(projectList, child)
        case FilterExec(condition, child) => GandivaFilterExec(condition, child)
        case p => p
      }
    }
  }
}
