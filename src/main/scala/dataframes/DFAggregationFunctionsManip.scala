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

  /**
    * Cube: applies aggregate expressions to all possible combinations of the grouping columns
    */
  val df = Seq(("foo", 1L), ("foo", 2L), ("bar", 2L), ("bar", 2L)).toDF("x", "y")

  val cube = df.cube('x, 'y).count().show()
  // +----+----+-----+
  // |   x|   y|count|
  // +----+----+-----+
  // |null|   1|    1|   <- count of records where y = 1
  // |null|   2|    3|   <- count of records where y = 2
  // | foo|null|    2|   <- count of records where x = foo
  // | bar|   2|    2|   <- count of records where x = bar AND y = 2
  // | foo|   1|    1|   <- count of records where x = foo AND y = 1
  // | foo|   2|    1|   <- count of records where x = foo AND y = 2
  // |null|null|    4|   <- total count of records
  // | bar|null|    2|   <- count of records where x = bar
  // +----+----+-----+

  /**
    * Rollup : val similar: Nothing = null val to: Nothing = null val is: Nothing = null val which:
    * Nothing = null val hierarchical: Nothing = null val from: Nothing = null
    */

  val rollup = df.rollup('x, 'y).count().show()

  //  +----+----+-----+
  //  |   x|   y|count|
  //  +----+----+-----+
  //  | bar|   2|    2|
  //  |null|null|    4|
  //  | foo|   2|    1|
  //  | foo|null|    2|
  //  | foo|   1|    1|
  //  | bar|null|    2|
  //  +----+----+-----+
}
