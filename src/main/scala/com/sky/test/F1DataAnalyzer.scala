package com.sky.test

import java.io.File

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class F1DataAnalyzer(val spark: SparkSession) extends Serializable {

  def extract(filePath: String): DataFrame =
    spark
      .read
      .schema(createDataSchema("driver_name, lap_time"))
      .csv(getResourcePath(filePath))

  def createDataSchema(schemaString: String): StructType = {
    val fieldNames = schemaString.split(",")
    val fields = fieldNames.map(fieldName => StructField(fieldName.trim, StringType, nullable = false))
    StructType(fields)
  }

  private final def getResourcePath(file: String): String =
    new File(getClass.getClassLoader.getResource(file).getFile).getCanonicalPath

  def transform(f1DataExtracted: DataFrame): DataFrame =
    f1DataExtracted
      .groupBy(col("driver_name"))
      .agg(avg(col("lap_time")))
      .select(col("driver_name"), col("avg(lap_time)"))
      .orderBy(col("avg(lap_time)").asc)

  def load(f1DataTransformed: DataFrame, resultsOutputPath: String): Unit = {
    topThreeRecords(f1DataTransformed)
      .coalesce(1)
      .write
      .format("csv")
      .option("mode","OVERWRITE")
      .save(resultsOutputPath)
  }

  def topThreeRecords(df: DataFrame) =
    df.limit(3)
}
