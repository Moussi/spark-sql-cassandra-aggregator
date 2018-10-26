package dataframes

import org.apache.spark.sql.SparkSession

object DFAggregationFunctionsManip extends App {


  val ss = SparkSession.builder().master("local[*]").getOrCreate()
  import ss.implicits._
  import utils.StringUtils._
  import org.apache.spark.sql.functions._

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

  val groupedDF = italianPostsDF.groupBy('ownerUserId, 'tags, 'postTypeId).count.orderBy('ownerUserId desc)show(10) // .count return dataframe
  val groupedDF2 = italianPostsDF.groupBy('ownerUserId)
      .agg(max('score).as('maxScore), max('lastActivityDate))
          .orderBy('maxScore desc).show(10) // .count return dataframe



}
