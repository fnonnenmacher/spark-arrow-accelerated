package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.vector.types.pojo.Field.nullable
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.types.{DataType, LongType}

case class FPGAModule(val name: String, query: ExpressionMatcher, output: Field) {

  val inputs: Map[String, Field] = {
    val fields = query.flatMap {
      case In(field) => Seq(field)
      case _ => Seq()
    }

    fields.distinct.groupBy(_.getName).mapValues { fields =>
      if (fields.size > 1) {
        throw new RuntimeException(s"FPGAModule '$name' is invalid. There exist two different fields for name '${fields.head.getName}'")
      }
      fields.head
    }
  }

  def prettyPrint(): Unit = {
    println(s"$name")
    println("Inputs:")
    inputs.map { case (name, field) => s"- $name(${field.getType.getTypeID})" }.toList.sorted.foreach(println(_))
    println("Query:")
    println("- " + query)
    println("Output:")
    println(s"- ${output.getName}(${output.getType.getTypeID})")
  }

  def findBestReplacementCandidate(exp: Expression): Option[(Expression, IdMapping)] = {

    //replacing this element
    val maxBenefit = withMaxBenefit(query.matches(exp));
    val replaceThisExp: Option[(Expression, IdMapping)] = if (maxBenefit.isDefined) Option((exp, maxBenefit.get)) else Option.empty

    //replacing the child expressions
    val replaceChildExp = exp.children.map(findBestReplacementCandidate)

    withMaxBenefit2((replaceChildExp :+ replaceThisExp))
  }

  private def withMaxBenefit(mappings: Seq[IdMapping]): Option[IdMapping] = {
    val filteredMappings = mappings.filter(_.benefit > 0)
    if (filteredMappings.isEmpty) Option.empty else Option(filteredMappings.maxBy(_.benefit))
  }

  private def withMaxBenefit2(mappings: Seq[Option[(Expression, IdMapping)]]): Option[(Expression, IdMapping)] = {
    val filteredMappings = mappings.filter(_.isDefined).map(_.get)
    if (filteredMappings.isEmpty) Option.empty else Option(filteredMappings.maxBy(_._2.benefit))
  }
}

object ArrowFieldDefinitionHelper {
  private val ArrowTypeLong = new ArrowType.Int(64, true)
  private val ArrowTypeInteger = new ArrowType.Int(32, true)

  def nullableInt(name: String): Field = {
    nullable(name, ArrowTypeInteger)
  }
}