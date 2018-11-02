package dataframes

/**
  * Created by amoussi on 30/10/18.
  */
case class Vote(id: Option[Long],
                postId: Option[Long],
                voteTypeId: Option[Int],
                creationDate: Option[java.sql.Timestamp]) {
}
