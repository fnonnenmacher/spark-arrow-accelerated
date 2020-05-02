package nl.tudelft.ewi.abs.nonnenmacher

import nl.tudelft.ewi.abs.nonnenmacher.ArrowFieldDefinitionHelper.nullableInt
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, Literal, Multiply, Sqrt}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import spire.math.Algebraic.Expr.Mul

@RunWith(classOf[JUnitRunner])
class ExpressionMatcherTest extends FunSuite {

  private def Attr(name: String) = AttributeReference(name, IntegerType)()

  test("Matching the sum of 3") {
    val in1: In = In(nullableInt("in1"))
    val in2: In = In(nullableInt("in2"))
    val in3: In = In(nullableInt("in3"))

    val vectorMagnitude = FPGAModule("sumOfThree", query = in1+in2+in3, output=nullableInt("out"))

    val attrA = Attr("a")
    val attrB = Attr("b")
    val attrC = Attr("c")
    val attrD = Attr("d")


    // verify expressions can be associative different
    assert(vectorMagnitude.findBestReplacementCandidate(Add(Add(attrA, attrB), attrC)).isDefined)
    assert(vectorMagnitude.findBestReplacementCandidate(Add(attrA, Add(attrB, attrC))).isDefined)
    assert(vectorMagnitude.findBestReplacementCandidate(Add(Add(attrA, attrB), Add(attrC, attrD))).isDefined)

    //
    val r = vectorMagnitude.findBestReplacementCandidate(Add(attrA, attrB))
    assert(r.isDefined)
    assert(r.get._2.map.values.toSeq.count(_ == Literal(0)) == 1) //one addition is neutralized neutral
  }

  test("Example of Vectormagnitude") {

    val in1: In = In(nullableInt("in1"))
    val in2: In = In(nullableInt("in2"))
    val in3: In = In(nullableInt("in3"))

    val out: Field = nullableInt("out")

    val vectorMagnitude = FPGAModule("vectormagnitude", query = SqrtMatcher((in1 * in1) + (in2 * in2) + (in3*in3)), out)

    vectorMagnitude.prettyPrint()

    val attrA = Attr("a")
    val attrB = Attr("b")
    val attrC = Attr("c")
    val attrD = Attr("d")

    val expr1 = Add(Sqrt(Add( Multiply(attrA, attrA), Multiply(attrB, attrB))), Literal(1))

    val opt = vectorMagnitude.findBestReplacementCandidate(expr1)

    assert(opt.isDefined)

    val (exp, mapping) = opt.get
    println(s"The expression'$exp' should be replaced.")
    print(s"The benefit is ${mapping.benefit}: ")
    mapping.map.map(e => s"  ${e._1}: ${e._2}").toList.sorted.foreach(print(_))
    println('\n')

//    println(vectorMagnitude.replace(expr1, exp, mapping).toString())

  }
}
