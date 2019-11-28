Spark Based F1 Data Analytics App

Please go through the following points before running the application.
1) Please run the main program MainApp.scala in order to perform F1 Data analytics.
3) Please provide full path to the output folder as an argument to the main program.
   For example C:\dev\result
4) Please make sure to delete output files and folders produced before running the main program second time.
5) F1DataAnalyzer performs extraction, transformation and load activities.
   Please refer F1DataAnalyzer.scala file.
6) F1DataAnalyzer extract method parses and extracts F1 driver name and its lap time.
7) F1DataAnalyzer transform method performs all the transformations asked in the exercise and calculate the average lap time for each driver and sort them in ascending order   
8) FileDataAnalyzer load method saves transformed dataframes as csv files(without header) under the output
   folder specified as an argument to the main program.
9) Unit testing was done in a TDD style. All the user defined methods were tested thoroughly. The idea behind TDD is to test business logic only.
    We are not supposed to test spark read and write API's as they are well tested.
    Please refer F1DataAnalyzerSpec.scala class.  
10) Integration testing was done in a BDD style for each transformations asked in the exercise.
    Please refer F1DataAnalyzerSuite.scala class.
11) Please run F1DataAnalyzerSuite.scala class in order to test all the transformations.
12) Transformed dataframes are being validated against the pre-calculated results present in csv files under
    src\test\resources directory as a part of the integration test
13) A generic spark based test framework was designed and developed to test all kinds of RDDs and data frames.
    Please refer BaseSuite.scala class for the detail implementation.
14) pom.xml file was updated to create a jar with dependencies called uber jar.

