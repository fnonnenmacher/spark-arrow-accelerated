package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import org.apache.arrow.gandiva.expression.TreeBuilder.{makeField, makeFunction, makeLiteral}
import org.apache.arrow.gandiva.expression.{TreeBuilder, TreeNode}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType}
import org.apache.spark.sql.util.ArrowUtils

import scala.collection.JavaConverters._

object GandivaExpressionConverter {

  def transform(expr: Expression): TreeNode = expr match {
    case bin @BinaryArithmetic(_,_)=> makeBinArithmeticFunction(bin)
    case bin @BinaryComparison(_,_)=> makeBinComparisonFunction(bin)
    case Literal(value, dataType) => makeLiteral(value, dataType)
    case AttributeReference(name, dataType, nullable, _) => makeFieldReference(name, dataType, nullable)
    case Alias(child, _) => transform(child) //skip
    case _ => throw new Exception(s"For expression type ${expr.getClass} is no Gandiva conversion defined. \n Expression: $expr")
  }

  private def makeFieldReference(name: String, dataType: DataType, nullable: Boolean) = {
    makeField(new Field(name, new FieldType(nullable, ArrowUtils.toArrowType(dataType, null), null), null))
  }

  private def functionName(binaryArithmetic: BinaryArithmetic) : String = binaryArithmetic match{
    case Add(_, _) => "add"
    case Multiply(_,_) => "multiply"
    case Subtract(_,_) => "subtract"
    case Divide(_,_)  => "divide"
    case _ => throw new Exception(s"For BinaryArithmetic type ${binaryArithmetic.getClass} is no LLVM function name defined. \n Expression: $binaryArithmetic")
  }

  private def functionName(binaryComparison: BinaryComparison) : String = binaryComparison match{
    case Equality(_,_) => "equal"
    case GreaterThan(_,_) => "greater_than"
    case LessThan(_,_) => "less_than"
    case LessThanOrEqual(_,_) => "less_than_or_equal_to"
    case GreaterThanOrEqual(_,_) => "greater_than_or_equal_to"
    case _ => throw new Exception(s"For BinaryComparison type ${binaryComparison.getClass} is no LLVM function name defined. \n Expression: $binaryComparison")
  }

  private def makeBinComparisonFunction(bin: BinaryComparison): TreeNode = {
    makeFunction(functionName(bin), List(transform(bin.left), transform(bin.right)).asJava, ArrowType.Bool.INSTANCE)
  }

  private def makeBinArithmeticFunction(bin: BinaryArithmetic): TreeNode = {
    makeFunction(functionName(bin), List(transform(bin.left), transform(bin.right)).asJava, ArrowUtils.toArrowType(bin.dataType, null))
  }

  private def makeLiteral(value: Any, dataType: DataType): TreeNode = dataType match {
    case IntegerType => TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Integer])
    case LongType => TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Long])
    case ByteType => TreeBuilder.makeBinaryLiteral(Array(value.asInstanceOf[java.lang.Byte]))
    case StringType => TreeBuilder.makeStringLiteral(value.asInstanceOf[java.lang.String])
    case FloatType => TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Float])
    case DoubleType => TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Double])
    case _ => throw new Exception(s"For Literals of type ${dataType} is no LLVM equivalent is defined.")
  }
}
