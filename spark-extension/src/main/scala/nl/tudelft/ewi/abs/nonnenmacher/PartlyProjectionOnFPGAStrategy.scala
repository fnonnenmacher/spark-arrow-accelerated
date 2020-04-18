package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.types.IntegerType

class PartlyProjectionOnFPGAStrategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case logical.Project(projectList, child) => findFPGAEnabledSubtree(projectList, child)
      case _ => Nil
    }
  }

  def findFPGAEnabledSubtree(projectList: Seq[NamedExpression], child: LogicalPlan): Seq[SparkPlan] = {

    val myRule = new MyRule();

    val postFPGA = myRule(projectList.head);

    if (myRule.hasMatched) {
      val preProjection = planLater(Project(myRule.preFpga, child))
      val fpgaProjectExec: FPGAProjectExec = FPGAProjectExec(preProjection, Seq(myRule.out))

      ProjectExec(postFPGA.asInstanceOf[NamedExpression] :: Nil, fpgaProjectExec) :: Nil

    } else {
      Nil
    }
  }
}

object PartlyProjectionOnFPGAStrategy extends PartlyProjectionOnFPGAStrategy

class MyRule extends Rule[Expression] {

  var preFpga: Seq[NamedExpression] = Seq()
  var out: AttributeReference = _
  var hasMatched: Boolean = false;

  override def apply(plan: Expression): Expression = plan transform {
    case Add(Add(in1, in2), in3) => {
      hasMatched = true;

      System.out.println(in1.dataType)
      val a1 = Alias(in1, "in1")()
      val a2 = Alias(in2, "in2")()
      val a3 = Alias(in3, "in3")()
      preFpga = Seq(a1, a2, a3)

      out = AttributeReference("out2", IntegerType)();
      out
    }
  }
}


