package com.gelerion.spark.skecthes.contract

trait Mergeable[T] {
  def merge(that: T): T
}

