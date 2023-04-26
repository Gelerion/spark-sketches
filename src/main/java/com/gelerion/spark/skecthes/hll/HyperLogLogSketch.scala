package com.gelerion.spark.skecthes.hll

import com.gelerion.spark.skecthes.contract.TypedAggregationBuffer
import org.apache.datasketches.memory.WritableMemory
import org.apache.datasketches.hll.{HllSketch, Union}
import org.apache.spark.TaskContext
import org.apache.spark.sql.types.{HyperLogLogSketchType, SQLUserDefinedType}

@SQLUserDefinedType(udt = classOf[HyperLogLogSketchType])
class HyperLogLogSketch(sketch: HllSketch) extends TypedAggregationBuffer[HyperLogLogSketch] {
  override type ValueType = Any
  override type ReturnType = Unit

  var union: Union = _ //postpone initialization till the reduce phase

  override def update(value: Any): Unit = value match {
    case double: Double     => sketch.update(double)
    case long: Long         => sketch.update(long)
    case int: Int           => sketch.update(int)
    case str: String        => sketch.update(str)
    case bytes: Array[Byte] => sketch.update(bytes)
    case _ => throw new IllegalStateException(s"Value [$value] of type [${value.getClass}] is unsupported by HllSketch")
  }

  override def merge(that: HyperLogLogSketch): HyperLogLogSketch = {
    logState(that)

    //see ThetaSketch merge for more detailed explanation
    if (!getSketch.isEmpty) {
      getUnion.update(this.getSketch)
      getUnion.update(that.getSketch)

      val newBuffer = HyperLogLogSketch()
      newBuffer.union = this.getUnion
      logDebug(s"[MERGE] New sketch ${newBuffer.hashCode()} is created. Partition Id ${TaskContext.getPartitionId()} - Thread Name  ${Thread.currentThread().getName}")
      return newBuffer
    }

    getUnion.update(that.getSketch)
    this
  }

  def toByteArray: Array[Byte] = {
    if (getSketch.isEmpty) {
      return union.toCompactByteArray
    }
    getSketch.toCompactByteArray
  }

  def getEstimate: Long = Math.round(sketch.getEstimate)

  override def toString: String = sketch.toString

  private def getSketch: HllSketch = sketch

  private def getUnion: Union = {
    if (union == null) union = new Union()
    union
  }

  private def logState(that: HyperLogLogSketch): Unit = {
    logDebug(
      s"""
         |[MERGE]
         |This buffer: ${this.hashCode()} with that buffer ${that.hashCode()}.
         |This Union state: ${if (union == null) "Empty, a new union will be created, slow path" else "Populated, fast path"}
         |This Sketch state: ${if (this.sketch.isEmpty) "Empty " else "Full "} and ${if (this.sketch.isCompact) "Compacted" else "Not Compacted"}
         |That Sketch state: ${if (that.getSketch.isEmpty) "Empty " else "Full "} and ${if (that.getSketch.isCompact) "Compacted" else "Not Compacted"}
         |Partition Id ${TaskContext.getPartitionId()} - Thread Name  ${Thread.currentThread().getName}
       """.stripMargin)
  }
}

//companion object
object HyperLogLogSketch {

  def apply(): HyperLogLogSketch = new HyperLogLogSketch(new HllSketch())

  def apply(config: HllSketchConfig): HyperLogLogSketch = new HyperLogLogSketch(new HllSketch(config.lgConfigK, config.tgtHllType))

  def apply(bytes: Array[Byte]): HyperLogLogSketch = {
    val mem = WritableMemory.writableWrap(bytes)
    new HyperLogLogSketch(HllSketch.wrap(mem))
  }
}
