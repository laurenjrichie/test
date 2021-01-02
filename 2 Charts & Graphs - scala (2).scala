// Databricks notebook source
// Click on the Plot Options Button...to see how this pivot table was configured.
// NOTE how Pivot Tables are highlighted in green to distinguish them from regular charts.
case class PivotEntry(key: String, series_grouping: String, value: Int)
val largePivotSeries = for (x <- 1 to 5000) yield PivotEntry("k_%03d".format(x % 200),"group_%01d".format(x % 3), x)
val largePivotDataFrame = sc.parallelize(largePivotSeries).toDF()
largePivotDataFrame.registerTempTable("table_to_be_pivoted")
display(sqlContext.sql("select * from table_to_be_pivoted"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Another way to think of a pivot table is that it does a group by on your original table by the key & series grouping, but instead of outputting (key, series_grouping, aggregation_function(value)) tuples, it outputs a table where the schema is the key and every unique value for the series grouping.
// MAGIC * See the results of group_by statement below, which contains all the data that is in the pivot table above.

// COMMAND ----------

case class SalesEntry(category: String, product: String, year: Int, salesAmount: Double)
val salesEntryDataFrame = sc.parallelize(
  SalesEntry("fruits_and_vegetables", "apples", 2000000000, 100.50) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2000000000, 100.75) :: 
  SalesEntry("fruits_and_vegetables", "apples", 2000000000, 200.25) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2000000000, 300.65) :: 
  SalesEntry("fruits_and_vegetables", "apples", 2000000000, 300.65) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2000000000, 100.35) ::
    SalesEntry("fruits_and_vegetables", "apples", 2000000001, 100.50) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2000000001, 100.75) :: 
  SalesEntry("fruits_and_vegetables", "apples", 2000000001, 200.25) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2000000001, 300.65) :: 
  SalesEntry("fruits_and_vegetables", "apples", 2000000001, 300.65) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2000000001, 100.35) ::
    SalesEntry("fruits_and_vegetables", "apples", 2000000002, 100.50) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2000000002, 100.75) :: 
  SalesEntry("fruits_and_vegetables", "apples", 2000000002, 200.25) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2000000002, 300.65) :: 
  SalesEntry("fruits_and_vegetables", "apples", 2000000002, 300.65) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2000000002, 100.35) ::
  SalesEntry("butcher_shop", "beef", 2000000003, 200.50) :: 
  SalesEntry("butcher_shop", "chicken", 2000000003, 200.75) :: 
  SalesEntry("butcher_shop", "pork", 2000000003, 400.25) :: 
  SalesEntry("butcher_shop", "beef", 2000000003, 600.65) :: 
  SalesEntry("butcher_shop", "beef", 2000000003, 600.65) :: 
  SalesEntry("butcher_shop", "chicken", 2000000003, 200.35) ::
  SalesEntry("miscd", "gum", 2000000004, 400.50) :: 
  SalesEntry("miscd", "cleaning_supplies", 2000000004, 400.75) :: 
  SalesEntry("miscd", "greeting_cards", 2000000004, 800.25) :: 
  SalesEntry("miscd", "kitchen_utensils", 2000000004, 1200.65) :: 
  SalesEntry("miscd", "cleaning_supplies", 2000000004, 1200.65) :: 
  SalesEntry("miscd", "cleaning_supplies", 2000000004, 400.35) ::
    SalesEntry("butcher_shops", "beef", 2000000003, 600.65) :: 
  SalesEntry("butcher_shops", "beef", 2000000003, 600.65) :: 
  SalesEntry("butcher_shops", "chicken", 2000000003, 200.35) ::
  SalesEntry("misca", "gum", 2000000004, 400.50) :: 
  SalesEntry("misca", "cleaning_supplies", 2000000004, 400.75) :: 
  SalesEntry("misca", "greeting_cards", 2000000004, 800.25) :: 
  SalesEntry("miscb", "kitchen_utensils", 2000000004, 1200.65) :: 
  SalesEntry("miscb", "cleaning_supplies", 2000000004, 1200.65) :: 
  SalesEntry("miscb", "cleaning_supplies", 2000000004, 400.35) ::
  
  Nil).toDF()
salesEntryDataFrame.registerTempTable("test_sales_table")
display(sqlContext.sql("select * from test_sales_table"))

// COMMAND ----------

// MAGIC %sql select key, series_grouping, sum(value) from table_to_be_pivoted group by key, series_grouping order by key, series_grouping

// COMMAND ----------

// MAGIC %md To plot just a graph of the US by state, use US state postal codes as the key.

// COMMAND ----------

case class StateEntry(state: String, value: Int)
val stateRDD = sc.parallelize(
  StateEntry("MO", 1) :: StateEntry("MO", 10) ::
  StateEntry("NH", 4) ::
  StateEntry("MA", 8) ::
  StateEntry("NY", 4) ::
  StateEntry("CA", 7) ::  Nil).toDF()
stateRDD.registerTempTable("test_state_table")
display(sqlContext.sql("Select * from test_state_table"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC To plot a graph of the world, use [country codes in ISO 3166-1 alpha-3 format](http://en.wikipedia.org/wiki/ISO_3166-1_alpha-3) as the key.

// COMMAND ----------

// Reminder: It's not a requirement to register this RDD as a temp table for Spark SQL - display can also be called directly on the RDD. 
case class WorldEntry(country: String, value: Int)
val worldRDD = sc.parallelize(
  WorldEntry("USA", 1000) ::
  WorldEntry("JPN", 23) ::
  WorldEntry("GBR", 23) ::
  WorldEntry("FRA", 21) ::
  WorldEntry("TUR", 3) ::
  Nil).toDF()
display(worldRDD)


// COMMAND ----------

// MAGIC %md ### A **Scatter Plot** allows you to see if there is a correlation between two variables.
// MAGIC * **Plot Options...** was selected to configure the graph below.
// MAGIC * **Keys** will be used to color the points on the graphs - with a legend on the side.
// MAGIC * **Series Grouping** is ignored.
// MAGIC * **Value** must contain at least two numberical fields.  This graph has a, b, and c as the values.
// MAGIC * The diagonal of the resulting plot is the kernal density plot of the variable.
// MAGIC * The row always has the variable in the Y-Axis, and the column has the variable in the X-Axis.

// COMMAND ----------

case class ScatterPlotEntry(key: String, a: Double, b: Double, c: Double)
val scatterPlotRDD = sc.parallelize(
  ScatterPlotEntry("k1", 0.2, 120, 1) :: ScatterPlotEntry("k1", 0.4, 140, 1) :: ScatterPlotEntry("k1", 0.6, 160, 1) :: ScatterPlotEntry("k1", 0.8, 180, 1) ::
  ScatterPlotEntry("k2", 0.2, 220, 1) :: ScatterPlotEntry("k2", 0.4, 240, 1) :: ScatterPlotEntry("k2", 0.6, 260, 1) :: ScatterPlotEntry("k2", 0.8, 280, 1) ::
  ScatterPlotEntry("k1", 1.2, 120, 1) :: ScatterPlotEntry("k1", 1.4, 140, 1) :: ScatterPlotEntry("k1", 1.6, 160, 1) :: ScatterPlotEntry("k1", 1.8, 180, 1) ::
  ScatterPlotEntry("k2", 1.2, 220, 2) :: ScatterPlotEntry("k2", 1.4, 240, 2) :: ScatterPlotEntry("k2", 1.6, 260, 2) :: ScatterPlotEntry("k2", 1.8, 280, 2) ::
  ScatterPlotEntry("k1", 2.2, 120, 1) :: ScatterPlotEntry("k1", 2.4, 140, 1) :: ScatterPlotEntry("k1", 2.6, 160, 1) :: ScatterPlotEntry("k1", 2.8, 180, 1) ::
  ScatterPlotEntry("k2", 2.2, 220, 3) :: ScatterPlotEntry("k2", 2.4, 240, 3) :: ScatterPlotEntry("k2", 2.6, 260, 3) :: ScatterPlotEntry("k2", 2.8, 280, 3) ::
  Nil).toDF()
display(scatterPlotRDD)



// COMMAND ----------

// MAGIC %md #### LOESS Fit Curves for Scatter Plots
// MAGIC 
// MAGIC [LOESS](https://en.wikipedia.org/wiki/Local_regression) is a method of performing local regression on your data to produce a smooth estimation curve that describes the data trend of your scatter plot. It does this by interpolating a curve within its neighborhood of data points. The LOESS fit curve is controlled by a bandwidth parameter that specifies how many neighboring points should be used to smooth the plot. A high bandwidth parameter (close to 1) gives a very smooth curve that may miss the general trend, while a low bandwidth parameter (close to 0) does not smooth the plot much.
// MAGIC 
// MAGIC LOESS fit curves are now available for scatter plots. Here is an example of how you can create a LOESS fit for your scatter plots.
// MAGIC 
// MAGIC **NOTE:** If your dataset has more than 5000 data points, the LOESS fit is computed using the first 5000 points.

// COMMAND ----------

// Create data points for scatter plot
val rng = new scala.util.Random(0)
val points = sc.parallelize((0L until 1000L).map { x => (x/100.0, 4 * math.sin(x/100.0) + rng.nextGaussian()) }).toDF()

// COMMAND ----------

// MAGIC %md You can turn this data into a scatter plot using the controls on the bottom left of the display table.
// MAGIC 
// MAGIC ![screen shot 2015-10-13 at 3 42 52 pm](https://cloud.githubusercontent.com/assets/7594753/10472059/d7e16396-71d0-11e5-866c-20b4d8b746cb.png)
// MAGIC 
// MAGIC You can now access the LOESS fit option when you select *Plot Options*:
// MAGIC 
// MAGIC 
// MAGIC ![screen shot 2015-10-13 at 3 43 16 pm](https://cloud.githubusercontent.com/assets/7594753/10472058/d7ce763c-71d0-11e5-91b2-4d90e9a704c9.png)
// MAGIC 
// MAGIC You can experiment with the bandwith parameter to see how the curve adapts to noisy data.
// MAGIC 
// MAGIC Once you accept the change, you will see the LOESS fit on your scatter plot!

// COMMAND ----------

display(points)

// COMMAND ----------

// MAGIC %md ### A **Histogram** allows you to determine the distribution of values.
// MAGIC * **Plot Options...** was selected to configure the graph below.
// MAGIC * **Value** should contain exactly one field.
// MAGIC * **Series Grouping** is always ignored.
// MAGIC * **Keys** can support up to 2 fields.
// MAGIC   * When no key is specified, exactly one histogram is output.
// MAGIC   * When 2 fields are specified, then there is a trellis of histograms.
// MAGIC * **Aggregation** is not applicable.
// MAGIC * **Number of bins** is a special option that appears only for histogram plots, and controls the number of bins in the histogram.
// MAGIC * Bins are computed on the serverside for histograms, so it can plot all the rows in a table.

// COMMAND ----------

// Hover over the entry in the histogram to read off the exact valued plotted.
case class HistogramEntry(key1: String, key2: String, value: Double)
val HistogramRDD = sc.parallelize(
  HistogramEntry("a", "x", 0.2) :: HistogramEntry("a", "x", 0.4) :: HistogramEntry("a", "x", 0.6) :: HistogramEntry("a", "x", 0.8) :: HistogramEntry("a", "x", 1.0) ::
  HistogramEntry("b", "z", 0.2) :: HistogramEntry("b", "x", 0.4) :: HistogramEntry("b", "x", 0.6) :: HistogramEntry("b", "y", 0.8) :: HistogramEntry("b", "x", 1.0) ::
  HistogramEntry("a", "x", 0.2) :: HistogramEntry("a", "y", 0.4) :: HistogramEntry("a", "x", 0.6) :: HistogramEntry("a", "x", 0.8) :: HistogramEntry("a", "x", 1.0) ::
  HistogramEntry("b", "x", 0.2) :: HistogramEntry("b", "x", 0.4) :: HistogramEntry("b", "x", 0.6) :: HistogramEntry("b", "z", 0.8) :: HistogramEntry("b", "x", 1.0) ::
  Nil).toDF()
display(HistogramRDD)

// COMMAND ----------

// MAGIC %md ### A **Quantile plot** allows you to view what the value is for a given quantile value.
// MAGIC * For more information on Quantile Plots, see http://en.wikipedia.org/wiki/Normal_probability_plot.
// MAGIC * **Plot Options...** was selected to configure the graph below.
// MAGIC * **Value** should contain exactly one field.
// MAGIC * **Series Grouping** is always ignored.
// MAGIC * **Keys** can support up to 2 fields.
// MAGIC   * When no key is specified, exactly one quantile plot is output.
// MAGIC   * When 2 fields are specified, then there is a trellis of quantile plots .
// MAGIC * **Aggregation** is not applicable.
// MAGIC * Quantiles are not being calculated on the serverside for now, so only the 1000 rows can be reflected in the plot.

// COMMAND ----------

case class QuantileEntry(key: String, grouping: String, otherField: Int, value: Int)
val quantileSeries = for (x <- 1 to 5000) yield QuantileEntry("key_%01d".format(x % 4),"group_%01d".format(x % 3), x, x*x)
val quantileSeriesRDD = sc.parallelize(quantileSeries).toDF()
display(quantileSeriesRDD)

// COMMAND ----------

// MAGIC %md ### A **Q-Q plot** shows you how a field of values are distributed.
// MAGIC * For more information on Q-Q plots, see http://en.wikipedia.org/wiki/Q%E2%80%93Q_plot.
// MAGIC * **Value** should contain one or two fields.
// MAGIC * **Series Grouping** is always ignored.
// MAGIC * **Keys** can support up to 2 fields.
// MAGIC   * When no key is specified, exactly one quantile plot is output.
// MAGIC   * When 2 fields are specified, then there is a trellis of quantile plots .
// MAGIC * **Aggregation** is not applicable.
// MAGIC * Q-Q Plots are not being calculated on the serverside for now, so only the 1000 rows can be reflected in the plot.

// COMMAND ----------

case class QQPlotEntry(key: String, grouping: String, value: Int, value_squared: Int)
val qqPlotSeries = for (x <- 1 to 5000) yield QQPlotEntry("k_%03d".format(x % 5),"group_%01d".format(x % 3), x, x*x)
val qqPlotRDD = sc.parallelize(qqPlotSeries).toDF()

// COMMAND ----------

// MAGIC %md When there is only one field specified for Values, a Q-Q plot will just compare the distribution of the field with a normal distribution.

// COMMAND ----------

display(qqPlotRDD)

// COMMAND ----------

// MAGIC %md When there are two fields specified for Values, a Q-Q plot will compare the distribution of the two fields with each other.

// COMMAND ----------

