package com.gelerion.spark.sketches.contract

import org.apache.spark.internal.Logging

trait TypedAggregationBuffer[T <: TypedAggregationBuffer[T]]
  extends Logging
    with Mergeable[T] {

  type ValueType <: Any
  type ReturnType <: Any

  def update(value: ValueType): ReturnType

  def merge(that: T): T

}
