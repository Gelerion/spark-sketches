package org.apache.spark.sql.aggregates.hll

import com.gelerion.spark.sketches.hll.HyperLogLogSketch
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.types.{DataType, HyperLogLogSketchType}

case class HyperLogLogSketchMergeAggregate(child: Expression,
                                           override val mutableAggBufferOffset: Int = 0,
                                           override val inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[HyperLogLogSketch] {

  //single expression constructor is mandatory for function to be used in SQL queries
  def this(child: Expression) = this(child, 0, 0)

  def createAggregationBuffer(): HyperLogLogSketch = HyperLogLogSketch()

  def update(buffer: HyperLogLogSketch, input: InternalRow): HyperLogLogSketch = {
    val value = child.eval(input)

    if (value != null) {
      child.dataType match {
        case hllSketchBytes: HyperLogLogSketchType => buffer.merge(hllSketchBytes.deserialize(value))
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unsupported data type ${other.catalogString}")
      }
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

  def children: Seq[Expression] = Seq(child)

  override def prettyName: String = "hll_sketch_merge"
}

