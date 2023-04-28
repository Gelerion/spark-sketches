package org.apache.spark.sql.aggregates.theta

import com.gelerion.spark.sketches.theta.{ThetaSketch, ThetaSketchConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, NumericType, StringType, ThetaSketchType}

/**
 * config("spark.sql.execution.useObjectHashAggregateExec", "false")
 * config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "10000")
 */
case class ThetaSketchBuildAggregate(child: Expression,
                                     config: ThetaSketchConfig = ThetaSketchConfig(),
                                     override val mutableAggBufferOffset: Int = 0,
                                     override val inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ThetaSketch] with ImplicitCastInputTypes with UnaryLike[Expression] {

  //single expression constructor is mandatory for function to be used in SQL queries
  def this(child: Expression) = this(child, ThetaSketchConfig(), 0, 0)

  override def createAggregationBuffer(): ThetaSketch = ThetaSketch(config)

  override def update(buffer: ThetaSketch, input: InternalRow): ThetaSketch = {
    val value = child.eval(input)

    // Ignore empty rows, for example: theta_sketch(null)
    if (value == null)
      return buffer

    //update the buffer
    child.dataType match {
      case string: StringType => buffer.update(value.asInstanceOf[string.InternalType].getBytes)
      case number: NumericType => buffer.update(number.numeric.toDouble(value.asInstanceOf[number.InternalType]))
      case other: DataType =>
        throw new UnsupportedOperationException(s"Unsupported data type ${other.catalogString}")
    }

    buffer
  }

  override def merge(buffer: ThetaSketch, input: ThetaSketch): ThetaSketch = buffer.merge(input)

  override def eval(buffer: ThetaSketch): Any = buffer.toByteArray

  override def serialize(buffer: ThetaSketch): Array[Byte] = ThetaSketchType.serialize(buffer)

  override def deserialize(bytes: Array[Byte]): ThetaSketch = ThetaSketchType.deserialize(bytes)

  def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newOffset)
  }

  def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newOffset)
  }

  def nullable: Boolean = false

  def dataType: DataType = ThetaSketchType

  override def prettyName: String = "theta_sketch_build"

  def inputTypes: Seq[AbstractDataType] = Seq(StringType, NumericType)

  protected def withNewChildInternal(newChild: Expression): ThetaSketchBuildAggregate = {
    copy(child = newChild)
  }
}
