package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object DFAnalyticFunctionsManip extends App {


  val ss = SparkSession.builder().master("local[*]").getOrCreate()
  import org.apache.spark.sql.functions._

    /***
    * Ranking Functions
    */
  val employersDF = ss.read.csv("src/main/resources/windowmanip.csv").toDF("name", "dep", "salary")

  //employersDF.show

  val window = Window.partitionBy("dep").orderBy(asc("salary"))

  val cumeDistColumn = cume_dist().over(window)
  employersDF.select(col("name"), col("dep"), col("salary"), cumeDistColumn.as("cum_dist")).show

  val firstColumn = first(col("salary")).over(window)
  //val lastColumn = last(col("salary")).over(window)
  employersDF.select(col("name"), col("dep"), col("salary"), firstColumn.as("first")).show


  val lagColumn = lag(col("salary"),1,0).over(window)
  val leadColumn = lead(col("salary"),1,0).over(window)
  employersDF.select(col("name"), col("dep"), col("salary"), lagColumn.as("lag"), leadColumn.as("lead")).show

  employersDF.select(col("name"), col("dep"), col("salary"), col("salary").minus(lagColumn)).show


}
