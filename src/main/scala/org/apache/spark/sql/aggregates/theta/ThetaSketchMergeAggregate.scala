package org.apache.spark.sql.aggregates.theta

import com.gelerion.spark.sketches.theta.ThetaSketch
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, ThetaSketchType}

case class ThetaSketchMergeAggregate(child: Expression,
                                     override val mutableAggBufferOffset: Int = 0,
                                     override val inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ThetaSketch] with ImplicitCastInputTypes with UnaryLike[Expression] {

  //single expression constructor is mandatory for function to be used in SQL queries
  def this(child: Expression) = this(child, 0, 0)

  def createAggregationBuffer(): ThetaSketch = ThetaSketch()

  def update(buffer: ThetaSketch, inputRow: InternalRow): ThetaSketch = {
    val value = child.eval(inputRow)

    // Ignore empty rows, for example: theta_sketch(null)
    if (value != null) {
      child.dataType match {
        case thetaSketchBytes: ThetaSketchType => buffer.merge(thetaSketchBytes.deserialize(value))
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unsupported data type ${other.catalogString}")
      }
    }

    buffer
  }

  def merge(buffer: ThetaSketch, input: ThetaSketch): ThetaSketch = {
    buffer.merge(input)
    buffer
  }

  def eval(buffer: ThetaSketch): Any = buffer.toByteArray

  def serialize(buffer: ThetaSketch): Array[Byte] = ThetaSketchType.serialize(buffer)

  def deserialize(bytes: Array[Byte]): ThetaSketch = ThetaSketchType.deserialize(bytes)

  def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newOffset)
  }

  def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newOffset)
  }

  def nullable: Boolean = false

  def dataType: DataType = ThetaSketchType

  override def prettyName: String = "theta_sketch_merge"

  def inputTypes: Seq[AbstractDataType] = Seq(ThetaSketchType)

  protected def withNewChildInternal(newChild: Expression): ThetaSketchMergeAggregate = {
    copy(child = newChild)
  }
}
