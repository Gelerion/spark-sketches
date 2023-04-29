package org.apache.spark.sql.registrar

import org.apache.spark.sql.aggregates.hll.{HyperLogLogSketchBuildAggregate, HyperLogLogSketchMergeAggregate}
import org.apache.spark.sql.aggregates.theta.{ThetaSketchBuildAggregate, ThetaSketchMergeAggregate}
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.expressions.{CountDistinctHllSketchEstimate, CountDistinctThetaSketchEstimate}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder

object SketchFunctionsRegistrar extends FunctionsRegistrar {
  override protected def expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    //Theta
    expression[ThetaSketchBuildAggregate]("theta_sketch_build"),
    expression[ThetaSketchMergeAggregate]("theta_sketch_merge"),
    expression[CountDistinctThetaSketchEstimate]("theta_sketch_get_estimate"),

    //HLL
    expression[HyperLogLogSketchBuildAggregate]("hll_sketch_build"),
    expression[HyperLogLogSketchMergeAggregate]("hll_sketch_merge"),
    expression[CountDistinctHllSketchEstimate]("hll_sketch_get_estimate")
  )
}
