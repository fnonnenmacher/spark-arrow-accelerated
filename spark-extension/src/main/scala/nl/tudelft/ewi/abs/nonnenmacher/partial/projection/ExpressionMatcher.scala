package nl.tudelft.ewi.abs.nonnenmacher.partial.projection

import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Literal, Multiply, Sqrt}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}
import org.apache.spark.sql.util.ArrowUtils

class IdMapping(val benefit: Int, val map: Map[String, Expression]) {

  def increaseBenefit(b: Int = 1): IdMapping = {
    new IdMapping(benefit + b, map)
  }

  final def merge(that: IdMapping): Option[IdMapping] = {

    val distinctPairs = (this.map.toSeq ++ that.map.toSeq).distinct.groupBy(_._1)

    if (distinctPairs.exists(e => e._2.size > 1)) {
      Option.empty
    } else {
      Option(new IdMapping(this.benefit + that.benefit, distinctPairs.map(e => e._2.head)))
    }
  }
}

object IdMapping {
  def apply(benefit: Int, pair: (String, Expression)): IdMapping = {
    new IdMapping(benefit, Map(pair))
  }
}

abstract class ExpressionMatcher extends TreeNode[ExpressionMatcher] {

  lazy val alternativeRepresentations: Seq[ExpressionMatcher] = {
    val emptySeq: ExpressionMatcher => Seq[ExpressionMatcher] = _ => Seq[ExpressionMatcher]()
    alternativesCreators.flatMap(_.applyOrElse(this, emptySeq))
  }

  def alternativesCreators: Seq[PartialFunction[ExpressionMatcher, Seq[ExpressionMatcher]]] = Seq()

  final def matches(exp: Expression): Seq[IdMapping] = {
    (matchesWithoutAlternatives(exp) ++ alternativeRepresentations.flatMap(_.matchesWithoutAlternatives(exp))).distinct
  }

  private def matchesWithoutAlternatives(exp: Expression): Seq[IdMapping] = {
    val function: Expression => Seq[IdMapping] = _ => Seq[IdMapping]()
    val s1: Seq[IdMapping] = specificMatching.applyOrElse(exp, function)
    val s2: Seq[IdMapping] = neutralizing(exp)
    (s1 ++ s2).distinct
  }

  def specificMatching: PartialFunction[Expression, Seq[IdMapping]]

  def neutralizing(expression: Expression): Seq[IdMapping] = Seq()

  def +(that: ExpressionMatcher): ExpressionMatcher = AddMatcher(this, that)

  def *(that: ExpressionMatcher): ExpressionMatcher = MultiplyMatcher(this, that)



  final override def verboseString(maxFields: Int): String = "";

  override def simpleStringWithNodeId(): String = {
    throw new UnsupportedOperationException(s"$nodeName does not implement simpleStringWithNodeId")
  }

  protected def combinations(mappings1: Seq[IdMapping], mappings2: Seq[IdMapping]): Seq[IdMapping] = {
    if (mappings1.isEmpty || mappings2.isEmpty) { // one side does not match at all -> no mapping possible
      return Seq()
    }
    val combinations: Seq[(IdMapping, IdMapping)] = mappings1.flatMap(map1 => mappings2.map(map2 => (map1, map2)))
    combinations.map(p => p._1.merge(p._2)).filter(_.isDefined).map(_.get)
  }
}

abstract class BinaryOperationMatcher extends ExpressionMatcher {

  def left: ExpressionMatcher

  def right: ExpressionMatcher

  def operator: String

  protected val childMatcherEqual: Boolean = left == right

  override final def children: Seq[ExpressionMatcher] = Seq(left, right)

  def childMatch(l: Expression, r: Expression): Seq[IdMapping] = {
    combinations(left.matches(l), right.matches(r))
  }

  def childrenMatchAssociative(l: Expression, r: Expression): Seq[IdMapping] = {
    if (childMatcherEqual)
      childMatch(l, r)
    else
      childMatch(r, l) ++ childMatch(l, r)
  }

  override def toString = s"($left$operator$right)"
}

case class AddMatcher(left: ExpressionMatcher, right: ExpressionMatcher) extends BinaryOperationMatcher {

  override def alternativesCreators: Seq[PartialFunction[ExpressionMatcher, Seq[ExpressionMatcher]]] = Seq({
    case AddMatcher(AddMatcher(s1, s2), AddMatcher(s3, s4)) => Seq((s1 + s4) + (s2 + s3), (s1 + s3) + (s2 + s4))
  }, {
    case AddMatcher(AddMatcher(s1, s2), s3) => Seq(s1 + (s2 + s3), s2 + (s1 + s3))
  }, {
    case AddMatcher(s3, AddMatcher(s1, s2)) => Seq(s1 + (s2 + s3), s2 + (s1 + s3))
  })

  override def operator: String = "+"

  override def specificMatching: PartialFunction[Expression, Seq[IdMapping]] = {
    case Add(l, r) => childrenMatchAssociative(l, r).map(_.increaseBenefit())
  }

  override def neutralizing(expression: Expression): Seq[IdMapping] = {
    //TODO datatype
    childrenMatchAssociative(Literal(0, expression.dataType), expression)
  }
}

case class MultiplyMatcher(left: ExpressionMatcher, right: ExpressionMatcher) extends BinaryOperationMatcher {
  override def operator: String = "*"

  override def specificMatching: PartialFunction[Expression, Seq[IdMapping]] = {
    case Multiply(l, r) => childrenMatchAssociative(l, r).map(_.increaseBenefit())
    case Literal(lit, IntegerType) => {
      val l = lit.asInstanceOf[Int]
      val root = intSqrt(l)
      if (childMatcherEqual && root * root == l) childMatch(Literal(root), Literal(root)) else Seq()
    }
  }

  private def intSqrt(l: Int): Int = {
    Math.sqrt(l).toInt
  }

  override def neutralizing(expression: Expression): Seq[IdMapping] = {
    //TODO datatype
    childrenMatchAssociative(Literal(1), expression)
  }
}

case class SqrtMatcher(child: ExpressionMatcher) extends ExpressionMatcher {
  override def specificMatching: PartialFunction[Expression, Seq[IdMapping]] = {
    case Sqrt(child_exp) => child.matches(child_exp)
  }

  override def children: Seq[ExpressionMatcher] = Seq(child)

  override def toString = s"sqrt(${child})"
}

case class In(field: Field) extends ExpressionMatcher {

  override final def children: Seq[ExpressionMatcher] = Seq()

  def isCompatible(dataType: DataType, arrowType: ArrowType): Boolean = dataType match {
    //TODO
    case LongType => if (arrowType.getTypeID == ArrowTypeID.Int && arrowType.asInstanceOf[ArrowType.Int].getBitWidth > 30) true else false
    case IntegerType => if (arrowType.getTypeID == ArrowTypeID.Int && arrowType.asInstanceOf[ArrowType.Int].getBitWidth > 30) true else false
    case _ => false
  }

  override def toString = s"'${field.getName}'"

  override def specificMatching: PartialFunction[Expression, Seq[IdMapping]] = {
    case expression =>
      if (isCompatible(expression.dataType, field.getType)) Seq(IdMapping(0, field.getName -> expression)) else Seq()
  }
}