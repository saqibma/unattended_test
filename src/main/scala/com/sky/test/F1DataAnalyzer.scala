package com.sky.test

import java.io.File

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class F1DataAnalyzer(val spark: SparkSession) extends Serializable {

  def extract(filePath: String): DataFrame = {
    val schemaString = "driver_name,lap_time"
    val fieldNames = schemaString.split(",")
    val fields = fieldNames.map(fieldName => StructField(fieldName, StringType, nullable = false))
    val schema = StructType(fields)
    spark.read.schema(schema).csv(getResourcePath(filePath))
  }

  private final def getResourcePath(file: String): String =
    new File(getClass.getClassLoader.getResource(file).getFile).getCanonicalPath

  def transform(f1DataExtracted: DataFrame): DataFrame = {
    val f1Data = f1DataExtracted
      .withColumn("lap_time", f1DataExtracted("lap_time").cast("double"))
    f1Data.persist()
    f1Data
      .groupBy(col("driver_name"))
      .agg(avg(col("lap_time")))
      .select(col("driver_name"), col("avg(lap_time)"))
      .orderBy(col("avg(lap_time)").asc)
  }

  def load(f1DataTransformed: DataFrame, resultsOutputPath: String): Unit = {
    f1DataTransformed
      .limit(3)
      .coalesce(1)
      .write
      .format("csv")
      .option("mode","OVERWRITE")
      .save(resultsOutputPath)
  }

}
