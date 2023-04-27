package org.apache.spark.sql.types

import com.gelerion.spark.sketches.hll.HyperLogLogSketch
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

case class HyperLogLogSketchType private() extends UserDefinedType[HyperLogLogSketch] {
  def sqlType: DataType = DataTypes.BinaryType

  def serialize(sketch: HyperLogLogSketch): Array[Byte] = sketch.toByteArray

  def deserialize(datum: Any): HyperLogLogSketch = {
    val bytes = datum.asInstanceOf[Array[Byte]]
    HyperLogLogSketch(bytes)
  }

  def userClass: Class[HyperLogLogSketch] = classOf[HyperLogLogSketch]

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
    ("class" -> this.getClass.getName.stripSuffix("$")) ~
    ("pyClass" -> pyUDT) ~
    ("sqlType" -> sqlType.jsonValue)
  }

  //for more readable exceptions in case of types mismatch
  override def catalogString: String = "HyperLogLogSketch"
}

object HyperLogLogSketchType extends HyperLogLogSketchType
