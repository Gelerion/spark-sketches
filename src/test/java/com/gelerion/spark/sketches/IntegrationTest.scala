package com.gelerion.spark.sketches

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.registrar.SketchFunctionsRegistrar
import org.apache.spark.sql.types.{HyperLogLogSketchType, StructField, ThetaSketchType}

import java.nio.file.Files
import scala.collection.mutable
import scala.util.Random
import org.apache.spark.sql.functions_ex._

class IntegrationTest extends SharedSparkSessionSuite {

  import spark.implicits._

  test("test theta-sketches") {
    val data: Seq[User] = generateUsers()
    val userStatistics = Statistics()
    data.foreach(user => userStatistics.addUser(user))

    val users = spark.createDataset(data)

    assert(users.count() == userStatistics.totalRows)

    // ---- Build Sketch
    val distinctUserIdsTheta = users
      .groupBy($"name")
      .agg(theta_sketch_build($"id").as("distinct_ids_sketch"))

    val schemaFields = distinctUserIdsTheta.schema.fields
    assert(schemaFields.length == 2)

    val thetaSketchField: StructField = distinctUserIdsTheta.schema.fields(1)
    assert(thetaSketchField.dataType == ThetaSketchType)

    // ---- Evaluate Sketch
    val evaluatedValues = distinctUserIdsTheta
      .select($"name", theta_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"))

    val approxDistinctUserIdsPairs = evaluatedValues.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })
  }

  test("test hll-sketches") {
    val data: Seq[User] = generateUsers()
    val userStatistics = Statistics()
    data.foreach(user => userStatistics.addUser(user))

    val users = spark.createDataset(data)

    assert(users.count() == userStatistics.totalRows)

    // ---- Build Sketch
    val distinctUserIdsTheta = users
      .groupBy($"name")
      .agg(hll_sketch_build($"id").as("distinct_ids_sketch"))

    val schemaFields = distinctUserIdsTheta.schema.fields
    assert(schemaFields.length == 2)

    val thetaSketchField: StructField = distinctUserIdsTheta.schema.fields(1)
    assert(thetaSketchField.dataType == HyperLogLogSketchType)

    // ---- Evaluate Sketch
    val evaluatedValues = distinctUserIdsTheta
      .select($"name", hll_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"))

    val approxDistinctUserIdsPairs = evaluatedValues.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })
  }

  test("merge hll-sketches") {
    val data: Seq[User] = generateUsers()
    val userStatistics = Statistics()
    data.foreach(user => userStatistics.addUser(user))

    val approxDistinctUserIdsPairs = spark.createDataset(data)
      .groupBy($"name", $"age")
      .agg(hll_sketch_build($"id").as("distinct_ids_sketch"))
      // drop dimension, merge sketches
      .groupBy($"name")
      .agg(hll_sketch_merge($"distinct_ids_sketch").as("distinct_ids_sketch"))
      // evaluate results
      .select($"name", hll_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"))
      .collect()
      //to scala Pair
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })
  }

  test("merge theta-sketches") {
    val data: Seq[User] = generateUsers()
    val userStatistics = Statistics()
    data.foreach(user => userStatistics.addUser(user))

    val approxDistinctUserIdsPairs = spark.createDataset(data)
      .groupBy($"name", $"age")
      .agg(theta_sketch_build($"id").as("distinct_ids_sketch"))
      // drop dimension, merge sketches
      .groupBy($"name")
      .agg(theta_sketch_merge($"distinct_ids_sketch").as("distinct_ids_sketch"))
      // evaluate results
      .select($"name", theta_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"))
      .collect()
      //to scala Pair
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })
  }

  test("usable in SQL, theta sketch") {
    //Register functions
    SketchFunctionsRegistrar.registerFunctions(spark)
    val data: Seq[User] = generateUsers()

    val users = spark.createDataset(data)
    users.createTempView("users_theta")

    validate(spark.sql(
      """
        | SELECT tmp.name, theta_sketch_get_estimate(theta_sketch_merge(tmp.distinct_ids_sketch)) as approx_count_distinct
        | FROM (SELECT name, age, theta_sketch_build(id) as distinct_ids_sketch
        |       FROM users_theta
        |       GROUP BY name, age) as tmp
        | GROUP BY tmp.name
        |""".stripMargin),
      initial = data)
  }

  test("usable in SQL, hll sketch") {
    //Register functions
    SketchFunctionsRegistrar.registerFunctions(spark)
    val data: Seq[User] = generateUsers()

    val users = spark.createDataset(data)
    users.createTempView("users_hll")

    validate(spark.sql(
      """
        | SELECT tmp.name, hll_sketch_get_estimate(hll_sketch_merge(tmp.distinct_ids_sketch)) as approx_count_distinct
        | FROM (SELECT name, age, hll_sketch_build(id) as distinct_ids_sketch
        |       FROM users_hll
        |       GROUP BY name, age) as tmp
        | GROUP BY tmp.name
        |""".stripMargin),
      initial = data)
  }

  test("writing and reading hll sketches as parquet files") {
    val data: Seq[User] = generateUsers()
    val output = Files.createTempDirectory("spark-tmp").toFile
    output.deleteOnExit()

    spark.createDataset(data)
      .groupBy($"name")
      .agg(hll_sketch_build($"id").as("distinct_sketch"))
      .write.mode(SaveMode.Overwrite).parquet(output.getAbsolutePath)

    val reread = spark.read.parquet(output.getAbsolutePath)
    validate(
      reread.select($"name", hll_sketch_get_estimate($"distinct_sketch").as("approx_count_distinct")),
      data)
  }

  test("writing and reading theta sketches as parquet files") {
    val data: Seq[User] = generateUsers()
    val output = Files.createTempDirectory("spark-tmp").toFile
    output.deleteOnExit()

    spark.createDataset(data)
      .groupBy($"name")
      .agg(theta_sketch_build($"id").as("distinct_sketch"))
      .write.mode(SaveMode.Overwrite).parquet(output.getAbsolutePath)

    val reread = spark.read.parquet(output.getAbsolutePath)
    validate(
      reread.select($"name", theta_sketch_get_estimate($"distinct_sketch").as("approx_count_distinct")),
      data)
  }

  def isWithinBounds(boundaryPercentage: Int, exactCount: Long)(approximateCount: Long): Boolean = {
    val minBoundary = exactCount - (exactCount * (boundaryPercentage.toDouble / 100))
    val maxBoundary = exactCount + (exactCount * (boundaryPercentage.toDouble / 100))
    approximateCount >= minBoundary && approximateCount <= maxBoundary
  }

  def validate(calculated: DataFrame, initial: Seq[User]): Unit = {
    val userStatistics = Statistics()
    initial.foreach(user => userStatistics.addUser(user))

    calculated.cache()
    assert(calculated.count() > 1)

    val approxDistinctUserIdsPairs = calculated.collect()
      //to scala Pair
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("approx_count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })
  }

  def generateUsers(toGenerate: Int = 50000, idsBound: Int = 30000): Seq[User] = {
    val names = Array("1", "2", "3", "4", "5", "6")
    val size = names.length

    for (i <- 0 to toGenerate) yield {
      User(names(i % size), Random.nextInt(70), Random.nextInt(idsBound))
    }
  }
}

case class User(name: String, age:Int, id: Int)

case class Statistics() {
  private val nameToUniqueIds: mutable.Map[String, mutable.Set[Long]] = mutable.Map.empty
  var totalRows: Int = 0

  def contains(name: String): Boolean = {
    nameToUniqueIds.contains(name)
  }

  def addUser(user: User): this.type  = {
    nameToUniqueIds.getOrElseUpdate(user.name, new mutable.HashSet[Long]()).add(user.id)
    totalRows += 1
    this
  }

  def exactUniqueIds(name: String): Long = {
    nameToUniqueIds.get(name).map(values => values.size.toLong).getOrElse(Long.MinValue)
  }
}
