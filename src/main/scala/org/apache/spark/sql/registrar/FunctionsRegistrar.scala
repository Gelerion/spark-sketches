package org.apache.spark.sql.registrar

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, FunctionRegistryBase}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

//based on org.apache.spark.sql.catalyst.analysis.FunctionRegistry
trait FunctionsRegistrar {

  /*
  Aggregator<IN, BUF, OUT> agg = // custom Aggregator
  Encoder<IN> enc = // input encoder
  // register a UDF based on agg and enc   (JAVA compatible API)
  spark.udf.register("myCustomAgg", functions.udaf(agg, enc))
   */

  protected def expressions: Map[String, (ExpressionInfo, FunctionBuilder)]

  protected def registerFunctions(fr: FunctionRegistry): Unit = {
    expressions.foreach { case (name, (info, builder)) => fr.registerFunction(FunctionIdentifier(name), info, builder) }
  }

  def registerFunctions(spark: SparkSession): Unit = {
    registerFunctions(spark.sessionState.functionRegistry)
  }

  protected def expression[T <: Expression](name: String)
                                           (implicit tag: ClassTag[T]): (String, (ExpressionInfo, FunctionBuilder)) = {
    val (expressionInfo, builder) = FunctionRegistryBase.build[T](name, None)
    (name, (expressionInfo, builder))
  }
}
