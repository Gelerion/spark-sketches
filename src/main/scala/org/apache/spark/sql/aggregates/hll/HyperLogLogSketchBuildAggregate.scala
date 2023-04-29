package org.apache.spark.sql.aggregates.hll

import com.gelerion.spark.sketches.hll.{HllSketchConfig, HyperLogLogSketch}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, HyperLogLogSketchType, NumericType, StringType}

case class HyperLogLogSketchBuildAggregate(child: Expression,
                                           config: HllSketchConfig = HllSketchConfig(),
                                           override val mutableAggBufferOffset: Int = 0,
                                           override val inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[HyperLogLogSketch] with ImplicitCastInputTypes with UnaryLike[Expression] {

  //single expression constructor is mandatory for function to be used in SQL queries
  def this(child: Expression) = this(child, HllSketchConfig(), 0, 0)

  def createAggregationBuffer(): HyperLogLogSketch = HyperLogLogSketch()

  def update(buffer: HyperLogLogSketch, input: InternalRow): HyperLogLogSketch = {
    val value = child.eval(input)

    // Ignore empty rows, for example: theta_sketch(null)
    if (value == null)
      return buffer

    //update the buffer
    child.dataType match {
      case string: StringType => buffer.update(value.asInstanceOf[string.InternalType].getBytes)
      case number: NumericType => buffer.update(number.numeric.toDouble(value.asInstanceOf[number.InternalType]))
      case other: DataType =>
        throw new UnsupportedOperationException(s"Unexpected data type ${other.catalogString}")
    }

    buffer
  }

  def merge(buffer: HyperLogLogSketch, input: HyperLogLogSketch): HyperLogLogSketch = buffer.merge(input)

  def eval(buffer: HyperLogLogSketch): Any = buffer.toByteArray

  def serialize(buffer: HyperLogLogSketch): Array[Byte] = HyperLogLogSketchType.serialize(buffer)

  def deserialize(bytes: Array[Byte]): HyperLogLogSketch = HyperLogLogSketchType.deserialize(bytes)

  def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newOffset)
  }

  def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newOffset)
  }

  def nullable: Boolean = false

  def dataType: DataType = HyperLogLogSketchType

  override def prettyName: String = "hll_sketch_build"

  def inputTypes: Seq[AbstractDataType] = Seq(StringType, NumericType)

  override protected def withNewChildInternal(newChild: Expression): HyperLogLogSketchBuildAggregate = {
    copy(child = newChild)
  }
}
