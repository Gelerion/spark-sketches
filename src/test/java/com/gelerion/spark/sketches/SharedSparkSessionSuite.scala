package com.gelerion.spark.sketches

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait SharedSparkSessionSuite extends FunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("data-sketches")
    .master("local[3]")
    .config("spark.network.timeout", "10000001")
    .config("spark.executor.heartbeatInterval", "10000000")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()

  override protected def beforeAll(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }


  override protected def afterAll(): Unit = {
    spark.stop()
  }

}
