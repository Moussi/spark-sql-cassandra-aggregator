package dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}

object DFApisExample extends App {


  val ss = SparkSession.builder().master("local[*]").getOrCreate()
  import ss.implicits._
  import org.apache.spark.sql.functions._

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
  /**
    * Full filtering operators
    * http://spark.apache.org/docs/2.0.0/api/scala/index.html#org.apache.spark.sql.Column
    */

  val filteredDF = italianPostsDF.filter('body contains "Italiano").toDF
  filteredDF.show(2)
  println("Count = "+filteredDF.count)

  val noAnswerDF = italianPostsDF.filter(('postTypeId === 1) and ('acceptedAnswerId isNull))
  noAnswerDF.show(2)
  println("Count = "+noAnswerDF.count)
  val noAnswerLimitDF = italianPostsDF.filter('postTypeId === 1).limit(10)
  println("Count noAnswerLimitDF = "+noAnswerLimitDF.count)

  val renamedColumnDF = noAnswerLimitDF.withColumnRenamed("body", "content")
  renamedColumnDF.printSchema()

  println("****************** Adding Column DF")
  val addColumnDF = noAnswerLimitDF.withColumn("ratio", 'viewCount / 'score).filter('ratio < 36)
  addColumnDF.show
  val sortedDF = addColumnDF.sort(desc("ratio"))
  sortedDF.show


}
