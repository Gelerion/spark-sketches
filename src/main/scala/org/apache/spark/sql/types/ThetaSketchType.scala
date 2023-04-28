package org.apache.spark.sql.types

import com.gelerion.spark.sketches.theta.ThetaSketch
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JValue

class ThetaSketchType extends UserDefinedType[ThetaSketch] {

  def sqlType: DataType = DataTypes.BinaryType

  def serialize(sketch: ThetaSketch): Array[Byte] = sketch.toByteArray

  def deserialize(datum: Any): ThetaSketch = {
    val bytes = datum.asInstanceOf[Array[Byte]]
    ThetaSketch(bytes)
  }

  def userClass: Class[ThetaSketch] = classOf[ThetaSketch]

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
    ("class" -> this.getClass.getName.stripSuffix("$")) ~
    ("pyClass" -> pyUDT) ~
    ("sqlType" -> sqlType.jsonValue)
  }

  //for more readable exceptions in case of types mismatch
  override def catalogString: String = "ThetaSketch"
}

case object ThetaSketchType extends ThetaSketchType