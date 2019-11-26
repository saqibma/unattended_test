package com.sky.test

class F1DataAnalyzerSuite extends BaseSuite {
  private var f1DataAnalyzer: F1DataAnalyzer = _

  override def beforeAll() {
    super.beforeAll()
    f1DataAnalyzer = new F1DataAnalyzer(spark)
  }

  test("Calculate the average lap time for each driver and sort them in ascending order") {
    Given("F1 Driver Data")
    val input = "f1DriverLapTime.csv"

    When("Call F1DataAnalyzer extract and transform methods")
    val fileDataExtracted = f1DataAnalyzer.extract(input)
    val actualResult = f1DataAnalyzer.transform(fileDataExtracted)

    Then("Compare actual and expected data frames")
    val expectedResult = createDF("f1DriverAverageLapTime.csv", "driver_name,avg(lap_time)")
    val expectedResultTypeCasted = expectedResult
      .withColumn("avg(lap_time)", expectedResult("avg(lap_time)").cast("double"))
    assertDataFrameEquals(actualResult, expectedResultTypeCasted)
  }

  override def afterAll() {
    spark.stop()
    // Clearing the driver port so that we don't try and bind to the same port on restart.
    System.clearProperty("spark.driver.port")
    super.afterAll()
  }
}
