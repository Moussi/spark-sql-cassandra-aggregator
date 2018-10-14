package dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}

object DFApisExample extends App {


  val ss = SparkSession.builder().master("local[*]").getOrCreate()
  import ss.implicits._

  import utils.StringUtils._

  val italianPosts = ss.sparkContext.textFile("src/main/resources/italianPosts.csv").map(_.split("~"))
  val italianPostsDF = italianPosts.map(post => Post(post(0).toIntSafe,
    post(1).toTimestampSafe,
    post(2).toLongSafe,
    post(3),
    post(4).toIntSafe,
    post(5).toTimestampSafe,
    post(6).toIntSafe,
    post(7),
    post(8),
    post(9).toIntSafe,
    post(10).toLongSafe,
    post(11).toLongSafe,
    post(12).toLong
  )).toDF

  italianPostsDF.printSchema()

  val selectedDF: DataFrame = italianPostsDF.select("id", "body")
  val selectedDF1: DataFrame = italianPostsDF.select($"id", $"body")
  val selectedDF2: DataFrame = italianPostsDF.select(italianPostsDF.col("id"), italianPostsDF.col("body"))
  val selectedDF3: DataFrame = italianPostsDF.select(Symbol("id"), Symbol("body"))

  println("**************** Selecting ********************")
  selectedDF.show(1)
  println("----------------------------------")
  selectedDF1.show(1)
  println("----------------------------------")
  selectedDF2.show(1)
  println("----------------------------------")
  selectedDF3.show(1)
  println("----------------------------------")

  val droppedColumnsDF = italianPostsDF.drop("body", "title")
  droppedColumnsDF.printSchema()
  println("**************** Filtering ********************")

  val filteredDF = italianPostsDF.filter('body contains "Italiano").toDF
  filteredDF.show(2)
  println("Count = "+filteredDF.count)

}
