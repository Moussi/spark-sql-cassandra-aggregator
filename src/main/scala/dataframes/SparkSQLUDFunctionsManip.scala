package dataframes

import org.apache.spark.sql.SparkSession
import utils.StringUtils._

object SparkSQLUDFunctionsManip extends App {


  val ss = SparkSession.builder().master("local[*]").getOrCreate()
  import ss.implicits._
    /***
    * Ranking Functions
    */
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

  italianPostsDF.createOrReplaceTempView("post")
  italianPostsDF.printSchema()

  val tagsCountUdf = ss.udf.register("tagsCount", (tags:String) => "&lt;".r.findAllMatchIn(tags).length)

  ss.sql("""select tags, tagsCount(tags) as counts from post where postTypeId = 1""").show
  println("************************************")
  italianPostsDF.filter('postTypeId === 1).select('tags, tagsCountUdf('tags).as("tags count")).show

}
