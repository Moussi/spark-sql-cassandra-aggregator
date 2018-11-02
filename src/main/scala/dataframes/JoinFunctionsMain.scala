package dataframes

import dataframes.DFSqlCalculations.ss
import org.apache.spark.sql.SparkSession

/**
  * Created by amoussi on 30/10/18.
  */
object JoinFunctionsMain extends App {

    val ss = SparkSession.builder().master("local[*]").appName("JoinFunctions").getOrCreate()
    import ss.implicits._
    import utils.StringUtils._
    val italianVotes = ss.sparkContext.textFile("src/main/resources/italianVotes.csv").map(_.split("~"))

    val italianVotesDf = italianVotes.map(row => Vote(id=row(0).toLongSafe,
        postId = row(1).toLongSafe,
        voteTypeId = row(2).toIntSafe,
        creationDate = row(3).toTimestampSafe)).toDF
    italianVotesDf.show

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

    val postVotes = italianPostsDF.join(italianVotesDf, italianPostsDF("id") === 'postId, "outer").show
//        .where(italianPostsDF("id").isNotNull)
//        .where(italianVotesDf("postId").isNull).show



}
