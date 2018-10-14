package dataframes

import org.apache.spark.sql.{Row, SparkSession}

object StructSchemaDFExample extends App {


  val ss = SparkSession.builder().master("local[*]").getOrCreate()

  import ss.implicits._
  import utils.StringUtils._

  import org.apache.spark.sql.types._

  val postSchema = buildPostStructType

  val italianPosts = ss.sparkContext.textFile("src/main/resources/italianPosts.csv").map(post => stringToRow(post))
  val italianPostsDF = ss.createDataFrame(italianPosts, postSchema)

  println("*************** DF schema ****************")
  italianPostsDF.printSchema()
  println("************** DF columns *********************")
  italianPostsDF.columns.foreach(println)

  println("************** DF types **************")
  italianPostsDF.dtypes.foreach(println)
  println("************** Title index **************")
  italianPostsDF.take(1).foreach(s => println(s.fieldIndex("title")))


  def stringToRow(row: String): Row = {
    val r = row.split("~")
    Row(r(0).toIntSafe.getOrElse(null),
      r(1).toTimestampSafe.getOrElse(null),
      r(2).toLongSafe.getOrElse(null),
      r(3),
      r(4).toIntSafe.getOrElse(null),
      r(5).toTimestampSafe.getOrElse(null),
      r(6).toIntSafe.getOrElse(null),
      r(7),
      r(8),
      r(9).toIntSafe.getOrElse(null),
      r(10).toLongSafe.getOrElse(null),
      r(11).toLongSafe.getOrElse(null),
      r(12).toLong)
  }

  private def buildPostStructType = {
    StructType(Seq(
      StructField("commentCount", IntegerType, true),
      StructField("lastActivityDate", TimestampType, true),
      StructField("ownerUserId", LongType, true),
      StructField("body", StringType, true),
      StructField("score", IntegerType, true),
      StructField("creationDate", TimestampType, true),
      StructField("viewCount", IntegerType, true),
      StructField("title", StringType, true),
      StructField("tags", StringType, true),
      StructField("answerCount", IntegerType, true),
      StructField("acceptedAnswerId", LongType, true),
      StructField("postTypeId", LongType, true),
      StructField("id", LongType, false))
    )
  }

}
