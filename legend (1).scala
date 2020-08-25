// Databricks notebook source
// %sql select * from test_sales_tablessasdfc

// COMMAND ----------

ddasdfsss

// COMMAND ----------

case class SalesEntry(category: String, year: Int, salesAmount: Double)
val salesEntryDataFrame = sc.parallelize(
  SalesEntry("f", 2012, 100.50) :: 
  SalesEntry("b", 2012, 200.50) :: 
  SalesEntry("m", 2012, 400.50) :: 
  SalesEntry("c", 2012, 100.50) :: 
  SalesEntry("v", 2012, 200.50) :: 
  SalesEntry("bb", 2012, 400.50) :: 
  SalesEntry("q", 2012, 100.50) :: 
  SalesEntry("w", 2012, 200.50) :: 
  SalesEntry("e", 2012, 400.50) :: 
  SalesEntry("r", 2012, 100.50) :: 
  SalesEntry("t", 2012, 200.50) :: 
  SalesEntry("m", 2012, 400.50) :: 
  SalesEntry("i", 2012, 100.50) :: 
  SalesEntry("o", 2012, 200.50) :: 
  SalesEntry("k", 2012, 400.50) :: 
  SalesEntry("ll", 2012, 100.50) :: 
  SalesEntry("i", 2012, 200.50) :: 
  SalesEntry("mm", 2012, 400.50) :: 
  Nil).toDF()
salesEntryDataFrame.registerTempTable("test_sales_table")

// COMMAND ----------

display(sqlContext.sql("select * from test_sales_table"))

// COMMAND ----------

// case class SalesEntry(category: String, product: String, year: Int, salesAmount: Double)
// val salesEntryDataFrame = sc.parallelize(
//   SalesEntry("fruits_and_vegetables", "apples", 2000000000, 100.50) :: 
//   SalesEntry("fruits_and_vegetables", "oranges", 2000000000, 100.75) :: 
//   SalesEntry("fruits_and_vegetables", "apples", 2000000000, 200.25) :: 
//   SalesEntry("fruits_and_vegetables", "oranges", 2000000000, 300.65) :: 
//   SalesEntry("fruits_and_vegetables", "apples", 2000000000, 300.65) :: 
//   SalesEntry("fruits_and_vegetables", "oranges", 2000000000, 100.35) ::
//     SalesEntry("fruits_and_vegetables", "apples", 2000000001, 100.50) :: 
//   SalesEntry("fruits_and_vegetables", "oranges", 2000000001, 100.75) :: 
//   SalesEntry("fruits_and_vegetables", "apples", 2000000001, 200.25) :: 
//   SalesEntry("fruits_and_vegetables", "oranges", 2000000001, 300.65) :: 
//   SalesEntry("fruits_and_vegetables", "apples", 2000000001, 300.65) :: 
//   SalesEntry("fruits_and_vegetables", "oranges", 2000000001, 100.35) ::
//     SalesEntry("fruits_and_vegetables", "apples", 2000000002, 100.50) :: 
//   SalesEntry("fruits_and_vegetables", "oranges", 2000000002, 100.75) :: 
//   SalesEntry("fruits_and_vegetables", "apples", 2000000002, 200.25) :: 
//   SalesEntry("fruits_and_vegetables", "oranges", 2000000002, 300.65) :: 
//   SalesEntry("fruits_and_vegetables", "apples", 2000000002, 300.65) :: 
//   SalesEntry("fruits_and_vegetables", "oranges", 2000000002, 100.35) ::
//   SalesEntry("butcher_shop", "beef", 2000000003, 200.50) :: 
//   SalesEntry("butcher_shop", "chicken", 2000000003, 200.75) :: 
//   SalesEntry("butcher_shop", "pork", 2000000003, 400.25) :: 
//   SalesEntry("butcher_shop", "beef", 2000000003, 600.65) :: 
//   SalesEntry("butcher_shop", "beef", 2000000003, 600.65) :: 
//   SalesEntry("butcher_shop", "chicken", 2000000003, 200.35) ::
//   SalesEntry("misc", "gum", 2000000004, 400.50) :: 
//   SalesEntry("misc", "cleaning_supplies", 2000000004, 400.75) :: 
//   SalesEntry("misc", "greeting_cards", 2000000004, 800.25) :: 
//   SalesEntry("misc", "kitchen_utensils", 2000000004, 1200.65) :: 
//   SalesEntry("misc", "cleaning_supplies", 2000000004, 1200.65) :: 
//   SalesEntry("misc", "cleaning_supplies", 2000000004, 400.35) ::
  
//   Nil).toDF()
// salesEntryDataFrame.registerTempTable("test_sales_table")
// display(sqlContext.sql("select * from test_sales_table"))

// COMMAND ----------



// COMMAND ----------

