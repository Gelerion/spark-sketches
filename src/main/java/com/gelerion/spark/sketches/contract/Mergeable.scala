package com.gelerion.spark.sketches.contract

trait Mergeable[T] {
  def merge(that: T): T
}

