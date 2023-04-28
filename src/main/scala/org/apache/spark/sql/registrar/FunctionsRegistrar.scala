package org.apache.spark.sql.registrar

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, RuntimeReplaceable}

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
  type FunctionBuilder = Seq[Expression] => Expression

  protected def expressions: Map[String, (ExpressionInfo, FunctionBuilder)]

  protected def registerFunctions(fr: FunctionRegistry): Unit = {
    expressions.foreach { case (name, (info, builder)) => fr.registerFunction(FunctionIdentifier(name), info, builder) }
  }

  def registerFunctions(spark: SparkSession): Unit = {
    registerFunctions(spark.sessionState.functionRegistry)
  }

  protected def expression[T <: Expression](name: String)
                                           (implicit tag: ClassTag[T]): (String, (ExpressionInfo, FunctionBuilder)) = {

    // For `RuntimeReplaceable`, skip the constructor with most arguments, which is the main
    // constructor and contains non-parameter `child` and should not be used as function builder.
    val constructors = if (classOf[RuntimeReplaceable].isAssignableFrom(tag.runtimeClass)) {
      val all = tag.runtimeClass.getConstructors
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs)
    } else {
      tag.runtimeClass.getConstructors
    }
    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        try {
          val exp = varargCtor.get.newInstance(expressions).asInstanceOf[Expression]
          exp
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw new AnalysisException(e.getCause.getMessage)
        }
      } else {
        // Otherwise, find a constructor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
          val validParametersCount = constructors
            .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
            .map(_.getParameterCount).distinct.sorted
          val invalidArgumentsMsg = if (validParametersCount.length == 0) {
            s"Invalid arguments for function $name"
          } else {
            val expectedNumberOfParameters = if (validParametersCount.length == 1) {
              validParametersCount.head.toString
            } else {
              validParametersCount.init.mkString("one of ", ", ", " and ") +
                validParametersCount.last
            }
            s"Invalid number of arguments for function $name. " +
              s"Expected: $expectedNumberOfParameters; Found: ${params.length}"
          }
          throw new AnalysisException(invalidArgumentsMsg)
        }
        try {
          val exp = f.newInstance(expressions: _*).asInstanceOf[Expression]
          exp
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw new AnalysisException(e.getCause.getMessage)
        }
      }
    }

    (name, (expressionInfo[T](name), builder))
  }

  /**
   * Creates an ExpressionInfo for the function as defined by expression T using the given name.
   */
  private def expressionInfo[T <: Expression : ClassTag](name: String): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      if (df.extended().isEmpty) {
        new ExpressionInfo(
          clazz.getCanonicalName,
          null,
          name,
          df.usage(),
          df.arguments(),
          df.examples(),
          df.note(),
          df.group(),
          df.since(),
          df.deprecated())
      } else {
        // This exists for the backward compatibility with old `ExpressionDescription`s defining
        // the extended description in `extended()`.
        new ExpressionInfo(clazz.getCanonicalName, null, name, df.usage(), df.extended())
      }
    } else {
      new ExpressionInfo(clazz.getCanonicalName, name)
    }
  }
}
