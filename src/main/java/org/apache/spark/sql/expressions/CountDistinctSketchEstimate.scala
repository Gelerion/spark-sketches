package org.apache.spark.sql.expressions

import com.gelerion.spark.skecthes.SketchType
import com.gelerion.spark.skecthes.SketchType.{THETA, HLL}
import com.gelerion.spark.skecthes.hll.HyperLogLogSketch
import com.gelerion.spark.skecthes.theta.ThetaSketch
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, DataType, HyperLogLogSketchType, LongType, ThetaSketchType}

abstract class CountDistinctSketchEstimate extends UnaryExpression
  with ImplicitCastInputTypes
  with Serializable {

  val child: Expression
  val sketch: SketchType.Value

  override def inputTypes: Seq[AbstractDataType] = sketch match {
    case THETA => Seq(ThetaSketchType)
    case HLL   => Seq(HyperLogLogSketchType)
  }

  override def dataType: DataType = LongType

  override protected def nullSafeEval(bytes: Any): Any = {
    sketch match {
      case THETA => ThetaSketch(bytes.asInstanceOf[Array[Byte]]).getEstimate
      case HLL   => HyperLogLogSketch(bytes.asInstanceOf[Array[Byte]]).getEstimate
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sketchClass = sketch match {
      case THETA => fullName(ThetaSketch.getClass)
      case HLL   => fullName(HyperLogLogSketch.getClass)
    }

    //We don't use `nullSafeCodeGen` to support backward compatibility between Spark versions.
    //There were incompatible changes between Spark versions 2.3 and 2.4 that caused NoSuchMethodFound errors.
    //e.g ExprCode.value method returns String in Spark 2.3 and ExprValue return in 2.4
    defineCodeGen(ctx, ev, child => s"$sketchClass.apply($child).getEstimate();")
  }

  override def toString: String = s"sketch_get_estimate($child)"

  override def prettyName: String = "sketch_get_estimate"

  private def fullName(clazz: Class[_]): String = {
    //class com.abc.Test -> com.abc.Text.MODULE$
    s"${clazz.toString.substring("class ".length)}.MODULE$$"
  }
}

case class CountDistinctHllSketchEstimate(child: Expression) extends CountDistinctSketchEstimate {
  override val sketch: SketchType.Value = SketchType.HLL
}

case class CountDistinctThetaSketchEstimate(child: Expression) extends CountDistinctSketchEstimate {
  override val sketch: SketchType.Value = SketchType.THETA
}
