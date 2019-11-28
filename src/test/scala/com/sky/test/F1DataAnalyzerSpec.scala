package com.sky.test

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class F1DataAnalyzerSpec extends FunSuite with Matchers with BeforeAndAfterAll {
  private[this] var spark: SparkSession = _
  private[this] var f1DataAnalyzer: F1DataAnalyzer = _

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.sql.shuffle.partitions","2")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setSparkHome(System.getenv("SPARK_HOME"))
    spark = SparkSession.builder().config(conf).getOrCreate
    f1DataAnalyzer = new F1DataAnalyzer(spark)
  }

  test("test create data schema") {
    val schemaString = "col1, col2, col3"
    val schema = f1DataAnalyzer.createDataSchema(schemaString)
    val generatedSchemaString = schema.fieldNames.reduce((col1, col2) => s"${col1}, ${col2}")
    generatedSchemaString should be(schemaString)
  }

  test("test transformation") {
    val schema =
      StructType(
        Array(
          StructField("driver_name", StringType, nullable = false),
          StructField("lap_time", StringType, nullable = false)))
    val rows = Seq(
      Row("driver1", "3.0"), Row("driver2", "4.0"), Row("driver3", "5.0"),
      Row("driver1", "3.0"), Row("driver2", "4.0"), Row("driver3", "5.0"))
    val inputDf = spark.sqlContext.createDataFrame(rows.asJava, schema)

    val transformedDf = f1DataAnalyzer.transform(inputDf)

    val transformedDfSchema =
      StructType(
        Array(
          StructField("driver_name", StringType, nullable = false),
          StructField("avg(lap_time)", DoubleType, nullable = true)))
    val expectedRows = Seq( Row("driver1", 3.0), Row("driver2", 4.0), Row("driver3", 5.0))
    val expectedTransformedDf = spark.sqlContext.createDataFrame(expectedRows.asJava, transformedDfSchema)

    transformedDf.count should be(expectedTransformedDf.count)
    transformedDf.schema should be(expectedTransformedDf.schema)
    transformedDf.collect should contain theSameElementsAs expectedTransformedDf.collect()
  }

  test("topThreeRecords method should return top three records dataframe") {
    val schema =
      StructType(
        Array(
          StructField("driver_name", StringType, nullable = false),
          StructField("avg(lap_time)", DoubleType, nullable = true)))
    val rows = Seq( Row("driver1", 3.0), Row("driver2", 4.0), Row("driver3", 5.0),  Row("driver4", 6.0))
    val inputDf = spark.sqlContext.createDataFrame(rows.asJava, schema)

    val topThreeRecordsDf = f1DataAnalyzer.topThreeRecords(inputDf)

    val expectedRows = Seq( Row("driver1", 3.0), Row("driver2", 4.0), Row("driver3", 5.0))
    val expectedTopThreeRecordsDf = spark.sqlContext.createDataFrame(expectedRows.asJava, schema)

    topThreeRecordsDf.count should be(expectedTopThreeRecordsDf.count)
    topThreeRecordsDf.schema should be(expectedTopThreeRecordsDf.schema)
    topThreeRecordsDf.collect should contain theSameElementsAs expectedTopThreeRecordsDf.collect()
  }

  override def afterAll() {
    spark.stop()
    // Clearing the driver port so that we don't try and bind to the same port on restart.
    System.clearProperty("spark.driver.port")
  }
}
