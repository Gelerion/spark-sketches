package org.apache.spark.sql

import com.gelerion.spark.sketches.hll.HllSketchConfig
import com.gelerion.spark.sketches.theta.ThetaSketchConfig
import org.apache.spark.sql.aggregates.hll.{HyperLogLogSketchBuildAggregate, HyperLogLogSketchMergeAggregate}
import org.apache.spark.sql.aggregates.theta.{ThetaSketchBuildAggregate, ThetaSketchMergeAggregate}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.expressions.{CountDistinctHllSketchEstimate, CountDistinctThetaSketchEstimate}

object functions_ex {

  /**
   * Aggregate function: returns the theta sketch as a byte array which represents
   * approximate number of distinct items in a group
   *
   * {{{
   *   ds.groupBy($"name").agg(functions_ex.theta_sketch_build($"id"))
   * }}}
   *
   * @group agg_funcs
   */
  def theta_sketch_build(column: Column): Column = theta_sketch_build(column, ThetaSketchConfig())

  def theta_sketch_build(column: Column, config: ThetaSketchConfig): Column = withAggregateFunction {
    ThetaSketchBuildAggregate(column.expr, config)
  }
  /**
   * Aggregate function: merges theta sketches
   *
   * {{{
   *   ds.groupBy($"name").agg(theta_sketch_merge($"distinct_users_sketch"))
   * }}}
   *
   * @group agg_funcs
   */
  def theta_sketch_merge(column: Column): Column = withAggregateFunction {
    ThetaSketchMergeAggregate(column.expr)
  }

  /**
   * Gets the unique count estimate.
   *
   * {{{
   *   ds.select(theta_sketch_evaluate("distinct_users_sketch"))
   * }}}
   *
   * @group binary_funcs
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  def theta_sketch_get_estimate(column: Column): Column = withExpr {
    CountDistinctThetaSketchEstimate(column.expr)
  }

  /**
   * Aggregate function: returns the hll sketch as a byte array which represents
   * approximate number of distinct items in a group
   *
   * {{{
   *   ds.groupBy($"name").agg(functions_ex.hll_sketch_build($"id"))
   * }}}
   *
   * @group agg_funcs
   */
  def hll_sketch_build(column: Column): Column = hll_sketch_build(column, HllSketchConfig())

  def hll_sketch_build(column: Column, config: HllSketchConfig): Column = withAggregateFunction {
    HyperLogLogSketchBuildAggregate(column.expr, config)
  }

  /**
   * Aggregate function: returns the hll sketch as a byte array which represents
   * approximate number of distinct items in a group
   *
   * {{{
   *   ds.groupBy($"name").agg(hll_sketch_merge($"distinct_users_sketch"))
   * }}}
   *
   * @group agg_funcs
   */
  def hll_sketch_merge(column: Column): Column = withAggregateFunction {
    HyperLogLogSketchMergeAggregate(column.expr)
  }

  /**
   * Gets the unique count estimate.
   *
   * {{{
   *   ds.select(hll_sketch_evaluate("distinct_users_sketch"))
   * }}}
   *
   * @group binary_funcs
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  def hll_sketch_get_estimate(column: Column): Column = withExpr {
    CountDistinctHllSketchEstimate(column.expr)
  }

  private def withAggregateFunction(func: AggregateFunction): Column = {
    Column(func.toAggregateExpression())
  }

  private def withExpr(expr: Expression): Column = Column(expr)
}
