package dataframes

import java.sql.Timestamp
case class Post(
                 commentCount:Option[Int],
                 lastActivityDate:Option[java.sql.Timestamp],
                 ownerUserId:Option[Long],
                 body:String,
                 score:Option[Int],
                 creationDate:Option[Timestamp],
                 viewCount:Option[Int],
                 title:String,
                 tags:String,
                 answerCount:Option[Int],
                 acceptedAnswerId:Option[Long],
                 postTypeId:Option[Long],
                 id:Long)
