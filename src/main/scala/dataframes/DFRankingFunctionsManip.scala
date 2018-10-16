package dataframes

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object DFRankingFunctionsManip extends App {


  val ss = SparkSession.builder().master("local[*]").getOrCreate()
  import org.apache.spark.sql.functions._
  import ss.implicits._
  import utils.StringUtils._

  /***
    * Ranking Functions
    */
  val employersDF = ss.read.csv("src/main/resources/windowmanip.csv").toDF("name", "dep", "salary")

  //employersDF.show

  val window = Window.partitionBy("dep").orderBy(desc("salary"))

  val rankColumn = rank.over(window)

  employersDF.select(col("name"), col("dep"), col("salary"), rankColumn.as("rank")).where(
  col("rank").leq(3)).show

  val denseRankColumn = dense_rank.over(window)
  employersDF.select(col("name"), col("dep"), col("salary"), denseRankColumn.as("dense_rank")).where(
    col("dense_rank").leq(3)).show

  val rowNumberColumn = row_number().over(window)
  employersDF.select(col("name"), col("dep"), col("salary"), rowNumberColumn.as("row_number")).show

  val percentRankColumn = percent_rank().over(window)
  employersDF.select(col("name"), col("dep"), col("salary"), percentRankColumn.as("persent_rank")).show

  val ntileColumn = ntile(2).over(window)
  employersDF.select(col("name"), col("dep"), col("salary"), ntileColumn.as("ntile_rank")).show


}
