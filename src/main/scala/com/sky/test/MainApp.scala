package com.sky.test

import org.apache.spark.sql.SparkSession

object MainApp extends App {

  val appName = "unattended-test"
  val master = "local[*]" //  In order to run application in yarn we need to set master to yarn and set yarn queue
  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.sql.shuffle.partitions","2") //optimised for standalone mode
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName(appName)
    .master(master)
    .getOrCreate()

  val inputDataPath: String = "f1DriverLapTime.csv"
  val resultsOutputPath: String = args(0)

  val f1DataAnalyzer = new F1DataAnalyzer(spark)
  val f1DataExtracted = f1DataAnalyzer.extract(inputDataPath)
  val f1DataTransformed = f1DataAnalyzer.transform(f1DataExtracted)
  f1DataAnalyzer.load(f1DataTransformed, resultsOutputPath)
}
