package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object DFAggregationFunctionsManip extends App {


  val ss = SparkSession.builder().master("local[*]").getOrCreate()
  import org.apache.spark.sql.functions._

    /***
    * Ranking Functions
    */
  val employersDF = ss.read.csv("src/main/resources/windowmanip.csv").toDF("name", "dep", "salary")

  //employersDF.show

  val window = Window.partitionBy("dep").orderBy(asc("salary")).rowsBetween(-1,1)

  val sumColumn = sum(col("salary")).over(window)
  employersDF.select(col("name"), col("dep"), col("salary"), sumColumn.as("sum")).show



}
