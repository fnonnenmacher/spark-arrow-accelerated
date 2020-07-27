package nl.tudelft.ewi.abs.nonnenmacher.columnar

import nl.tudelft.ewi.abs.nonnenmacher.columnar.selection.{ColumnarWithSelectionToRowExec, ColumnarWithSelectionVectorSupport}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, ColumnarToRowExec, RowToColumnarExec, SparkPlan}


object ArrowColumnarExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectColumnar(x => ArrowColumnarRule)
  }

  object ArrowColumnarRule extends ColumnarRule {
    override def postColumnarTransitions: Rule[SparkPlan] = {
      case plan@ColumnarToRowExec(child) =>
        if (child.isInstanceOf[ColumnarWithSelectionVectorSupport])
          new ColumnarWithSelectionToRowExec(postColumnarTransitions(child))
        else
          plan.withNewChildren(plan.children.map(p => postColumnarTransitions(p)))
      case RowToColumnarExec(child) => new RowToArrowExec(postColumnarTransitions(child))
      case plan => plan.withNewChildren(plan.children.map(p => postColumnarTransitions(p)))
    }
  }

}
